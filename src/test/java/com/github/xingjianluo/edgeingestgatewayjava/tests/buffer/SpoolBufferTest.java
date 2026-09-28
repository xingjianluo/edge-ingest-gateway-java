package com.github.xingjianluo.edgeingestgatewayjava.tests.buffer;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.BufferRejectException;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.clock.ManualGatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;
import com.github.xingjianluo.edgeingestgatewayjava.resume.OffsetStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地缓冲测试：重复上报、乱序、写满背压、并发写入、时钟回拨、重启恢复、位点回退。
 * 每个用例打印关键输入与判定依据。
 */
class SpoolBufferTest {

    @TempDir
    Path spoolDir;

    private UnifiedReport report(String device, String metric, long businessMillis, double value) {
        String key = device + "#" + metric + "#" + businessMillis;
        return new UnifiedReport(device, metric, value,
                Instant.ofEpochMilli(businessMillis), Instant.ofEpochMilli(businessMillis),
                "json-v1", key);
    }

    @Test
    void duplicateReportIsRejectedByDedupKey() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        UnifiedReport r = report("d1", "temp", 1_700_000_000_000L, 21.5);

        Optional<Long> first = buffer.append(r);
        Optional<Long> second = buffer.append(r);

        System.out.printf("[SpoolBufferTest] 重复上报 | 输入=同一 dedupKey 写入两次 | "
                        + "第一次=%s 第二次=%s | 判定依据=第二次应被去重%n",
                first, second);
        assertTrue(first.isPresent());
        assertTrue(second.isEmpty());
        assertEquals(1, buffer.pendingCount());
        assertEquals(1, buffer.totalAccepted());
        assertEquals(1, buffer.totalDuplicates());
    }

    @Test
    void nextBatchReturnsBusinessTimeOrder() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        // 乱序写入：t3, t1, t2
        buffer.append(report("d1", "temp", 3_000L, 3.0));
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.append(report("d1", "temp", 2_000L, 2.0));

        List<LocalSpoolBuffer.BufferedEntry> batch = buffer.nextBatch(10);
        List<Long> times = batch.stream()
                .map(e -> e.report().getBusinessTime().toEpochMilli()).toList();
        System.out.printf("[SpoolBufferTest] 乱序补发 | 写入顺序=[3000,1000,2000] | 读取业务时间=%s | 判定依据=必须按业务时间升序%n",
                times);
        assertEquals(List.of(1_000L, 2_000L, 3_000L), times);
    }

    @Test
    void bufferFullRejectsWithBackpressure() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 3, clock);
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.append(report("d1", "temp", 2_000L, 2.0));
        buffer.append(report("d1", "temp", 3_000L, 3.0));

        BufferRejectException ex = assertThrows(BufferRejectException.class,
                () -> buffer.append(report("d1", "temp", 4_000L, 4.0)));
        System.out.printf("[SpoolBufferTest] 缓冲写满 | 容量=3 已写=3 再写1条 | 判定=%s | 依据=%s%n",
                ex.getCode(), ex.getMessage());
        assertEquals("BUFFER_FULL", ex.getCode());
        assertEquals(3, buffer.pendingCount());
    }

    @Test
    void concurrentAppendsLoseNothingAndDoNotInterleave() throws Exception {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        int threads = 8;
        int perThread = 50;
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, threads * perThread, clock);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 每条唯一 dedupKey：设备#指标#业务时间
                        long ts = 1_700_000_000_000L + (long) tid * perThread + i;
                        buffer.append(report("d" + tid, "temp", ts, i)).ifPresent(s -> accepted.incrementAndGet());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        System.out.printf("[SpoolBufferTest] 并发写入 | 线程=%d 每线程=%d | 成功=%d pending=%d | 判定依据=成功数必须等于总写入数，无丢失%n",
                threads, perThread, accepted.get(), buffer.pendingCount());
        assertEquals(threads * perThread, accepted.get());
        assertEquals(threads * perThread, buffer.pendingCount());
    }

    @Test
    void clockRollbackDoesNotCorruptSequence() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        Optional<Long> s1 = buffer.append(report("d1", "temp", 1_000L, 1.0));
        // 人为回拨时钟 1 小时
        clock.setTimeMillis(1_700_000_000_000L - 3_600_000L);
        Optional<Long> s2 = buffer.append(report("d1", "temp", 2_000L, 2.0));

        System.out.printf("[SpoolBufferTest] 时钟回拨 | seq1=%s 回拨后 seq2=%s | 判定依据=序号必须继续单调递增，不得覆盖%n",
                s1, s2);
        assertTrue(s1.isPresent() && s2.isPresent());
        assertTrue(s2.get() > s1.get());
        assertEquals(2, buffer.pendingCount());
    }

    @Test
    void restartRecoversPendingAndDedupState() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.append(report("d1", "temp", 2_000L, 2.0));
        buffer.append(report("d1", "temp", 3_000L, 3.0));
        // 提交前两条
        buffer.commitBatch(List.of(1L, 2L), List.of("d1#temp#1000", "d1#temp#2000"));
        long watermarkBefore = buffer.committedWatermarkSeq();
        buffer.close();

        // 模拟进程重启：重新打开同一目录
        LocalSpoolBuffer reopened = new LocalSpoolBuffer(spoolDir, 100, clock);
        System.out.printf("[SpoolBufferTest] 重启恢复 | 重启前位点=%d | 重启后 pending=%d 位点=%d | 判定依据=已送达不重现，未送达不丢失%n",
                watermarkBefore, reopened.pendingCount(), reopened.committedWatermarkSeq());
        assertEquals(1, reopened.pendingCount());
        assertEquals(watermarkBefore, reopened.committedWatermarkSeq());

        // 已送达的 dedupKey 重启后仍应被去重
        Optional<Long> dup = reopened.append(report("d1", "temp", 1_000L, 9.9));
        System.out.printf("[SpoolBufferTest] 重启后重复上报 | 输入=已送达的 dedupKey | 判定=%s | 依据=重启后仍应去重%n",
                dup.isEmpty() ? "DUPLICATE(去重)" : "ACCEPT(错误)");
        assertTrue(dup.isEmpty());
    }

    @Test
    void offsetRollbackIsRejected() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.append(report("d1", "temp", 2_000L, 2.0));
        buffer.commitBatch(List.of(1L, 2L), List.of("d1#temp#1000", "d1#temp#2000"));

        OffsetStoreException ex = assertThrows(OffsetStoreException.class,
                () -> buffer.checkpointWatermark(1L));
        System.out.printf("[SpoolBufferTest] 位点回退 | 当前位点=%d 请求回退到=1 | 判定=拒绝 | 依据=%s%n",
                buffer.committedWatermarkSeq(), ex.getMessage());
        assertEquals(2L, buffer.committedWatermarkSeq());
    }

    @Test
    void tamperedWatermarkFileIsRejectedOnStartup() throws Exception {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.append(report("d1", "temp", 2_000L, 2.0));
        buffer.commitBatch(List.of(1L, 2L), List.of("d1#temp#1000", "d1#temp#2000"));
        buffer.close();

        // 人为把位点文件改旧（模拟回退/篡改）
        java.nio.file.Files.writeString(spoolDir.resolve("watermark.dat"), "0");
        OffsetStoreException ex = assertThrows(OffsetStoreException.class,
                () -> new LocalSpoolBuffer(spoolDir, 100, clock));
        System.out.printf("[SpoolBufferTest] 位点文件被改旧 | delivered最大序号=2 位点文件=0 | 判定=拒绝启动 | 依据=%s%n",
                ex.getMessage());
    }

    @Test
    void commitIsIdempotent() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        buffer.append(report("d1", "temp", 1_000L, 1.0));
        buffer.commitBatch(List.of(1L), List.of("d1#temp#1000"));
        // 重复提交同一批次（模拟“投递成功但提交前崩溃，重启后重投”）
        buffer.commitBatch(List.of(1L), List.of("d1#temp#1000"));
        System.out.printf("[SpoolBufferTest] 幂等提交 | 同一批次提交两次 | pending=%d 位点=%d | 判定依据=不产生重复效果%n",
                buffer.pendingCount(), buffer.committedWatermarkSeq());
        assertEquals(0, buffer.pendingCount());
        assertEquals(1L, buffer.committedWatermarkSeq());
    }

    @Test
    void batchSizeLimitsNextBatch() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 100, clock);
        for (int i = 1; i <= 5; i++) {
            buffer.append(report("d1", "temp", i * 1_000L, i));
        }
        List<LocalSpoolBuffer.BufferedEntry> batch = buffer.nextBatch(2);
        List<Long> times = new ArrayList<>(batch.stream()
                .map(e -> e.report().getBusinessTime().toEpochMilli()).toList());
        System.out.printf("[SpoolBufferTest] 批次大小 | 写入5条 读取max=2 | 读到=%s | 判定依据=按业务时间取最早2条%n", times);
        assertEquals(List.of(1_000L, 2_000L), times);
    }
}
