package com.github.xingjianluo.edgeingestgatewayjava.relay;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.SpooledEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内存假上游：
 * <ul>
 *   <li>可随时切换断连/恢复，断连期间 publish 抛 {@link UpstreamUnavailableException}；</li>
 *   <li>按幂等键去重（模拟云端幂等接收），因此网关 at-least-once 重试是安全的；</li>
 *   <li>记录投递次数，测试可断言"补发没有造成云端重复"。</li>
 * </ul>
 */
public class FakeUpstreamClient implements UpstreamClient {

    private volatile boolean available = true;
    private final Map<String, SpooledEvent> received = new LinkedHashMap<>();
    private final Map<String, AtomicInteger> deliverCounts = new LinkedHashMap<>();
    private final List<String> publishLog = new ArrayList<>();

    @Override
    public synchronized void publish(SpooledEvent event) {
        publishLog.add(event.idempotencyKey() + "#seq=" + event.seq());
        if (!available) {
            throw new UpstreamUnavailableException(
                    "upstream link is DOWN, publish rejected for seq=" + event.seq()
                            + " (event stays in local buffer for retry)");
        }
        received.putIfAbsent(event.idempotencyKey(), event);
        deliverCounts.computeIfAbsent(event.idempotencyKey(), k -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public boolean available() {
        return available;
    }

    @Override
    public void setAvailable(boolean available) {
        this.available = available;
    }

    /** 云端去重后最终保留的事件（按首次成功投递顺序）。 */
    public synchronized Map<String, SpooledEvent> received() {
        return new LinkedHashMap<>(received);
    }

    /** 每个幂等键实际到达云端的次数（>1 说明发生过重试，但云端只保留一份）。 */
    public synchronized int deliverCount(String idempotencyKey) {
        AtomicInteger c = deliverCounts.get(idempotencyKey);
        return c == null ? 0 : c.get();
    }

    /** 所有 publish 尝试（含断连失败的尝试），用于打印判定依据。 */
    public synchronized List<String> publishLog() {
        return new ArrayList<>(publishLog);
    }
}
