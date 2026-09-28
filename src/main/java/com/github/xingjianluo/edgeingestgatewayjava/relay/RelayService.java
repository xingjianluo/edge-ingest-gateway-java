package com.github.xingjianluo.edgeingestgatewayjava.relay;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.SpooledEvent;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 续传器。
 *
 * <p>规则：
 * <ul>
 *   <li>每轮从本地存储加载待发事件，<b>按业务时间 eventTime 升序</b>补发（同时间按入库序号）；</li>
 *   <li>上游断连时立即停发，已确认的先推进位点，未发/发送失败的保留在本地等待下一轮；</li>
 *   <li>位点推进支持乱序确认：把本轮成功的 seq 并入确认集合，只把从 frontier+1 开始连续的
 *       前缀一次性推进；中间空洞未补齐前绝不越过，保证不丢数据；</li>
 *   <li>位点只能前进：回退由存储层 {@code advanceCheckpoint} 明确拒绝。</li>
 * </ul>
 */
public class RelayService {

    private final EventStore store;
    private final UpstreamClient upstream;
    private final TreeSet<Long> acked = new TreeSet<>();

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public RelayService(EventStore store, UpstreamClient upstream) {
        this.store = store;
        this.upstream = upstream;
    }

    /**
     * 执行一轮补发。
     *
     * @return 本轮结果（尝试数、成功数、是否遇到断连、推进后的 frontier）
     */
    public synchronized PumpResult pumpOnce() {
        if (!upstream.available()) {
            return new PumpResult(0, 0, true, store.frontier());
        }
        List<SpooledEvent> pending = store.loadPending();
        int attempted = 0;
        int delivered = 0;
        boolean interrupted = false;
        long failedSeq = -1L;
        for (SpooledEvent event : pending) {
            // 跳过此前轮次已成功、但因乱序空洞尚未推进位点的事件
            if (event.seq() <= store.frontier() || acked.contains(event.seq())) {
                continue;
            }
            attempted++;
            try {
                upstream.publish(event);
                acked.add(event.seq());
                delivered++;
            } catch (UpstreamUnavailableException e) {
                // 断连：停止本轮。当前事件未被确认，仍留在本地缓冲
                interrupted = true;
                failedSeq = event.seq();
                break;
            }
        }
        long frontier = advanceContiguous();
        return new PumpResult(attempted, delivered, interrupted, frontier)
                .withFailedSeq(failedSeq);
    }

    /**
     * 将已确认且与 frontier 连续的前缀一次性推进。乱序成功的 seq 先暂存于 acked 集合，
     * 等空洞补齐后位点才越过它。
     */
    private long advanceContiguous() {
        long frontier = store.frontier();
        long target = frontier;
        while (acked.remove(target + 1)) {
            target++;
        }
        if (target > frontier) {
            store.advanceCheckpoint(target);
            frontier = target;
        }
        return frontier;
    }

    /** 后台定时补发（守护线程，不阻止 JVM 退出）。 */
    public void start(long intervalMillis) {
        if (running) {
            return;
        }
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "edge-relay");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (running) {
                    pumpOnce();
                }
            } catch (RuntimeException ignored) {
                // 单轮异常不影响后续轮询；失败数据仍在本地
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    public boolean isRunning() {
        return running;
    }

    /** 一轮补发的可解释结果。 */
    public record PumpResult(int attempted, int delivered, boolean interrupted,
                             long frontierAfter, long failedSeq) {

        PumpResult(int attempted, int delivered, boolean interrupted, long frontierAfter) {
            this(attempted, delivered, interrupted, frontierAfter, -1L);
        }

        PumpResult withFailedSeq(long seq) {
            return new PumpResult(attempted, delivered, interrupted, frontierAfter, seq);
        }
    }
}
