package com.github.xingjianluo.edgeingestgatewayjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.CheckpointRollbackException;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalFileEventStore;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.FlatJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ProbeV2Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.StandardJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：有界缓冲背压 + 并发写入不丢不串 + 位点回退拒绝。
 */
class BufferBackpressureTest {

    private static final int CAPACITY = 20;

    private ObjectMapper mapper;
    private EventStore store;
    private IngestService ingest;

    @BeforeEach
    void setUp(@TempDir Path tmp) {
        mapper = new ObjectMapper();
        List<Normalizer> normalizers =
                List.of(new StandardJsonNormalizer(), new FlatJsonNormalizer(), new ProbeV2Normalizer());
        store = new LocalFileEventStore(tmp.resolve("store"), CAPACITY, 200, mapper);
        ingest = new IngestService(normalizers, store, mapper, 300_000);
    }

    @Test
    void bufferFullIsRejectedWithReason() {
        int accepted = 0;
        for (int i = 0; i < CAPACITY; i++) {
            IngestResult r = ingest.ingest(report("full", i, 1727500000000L + i));
            assertTrue(r.accepted(), "容量内应接受: " + i);
            accepted++;
        }
        System.out.printf("[背压] 已写满缓冲: accepted=%d, pending=%d, capacity=%d%n",
                accepted, store.pendingCount(), CAPACITY);

        IngestResult overflow = ingest.ingest(report("full", 999, 1727500100000L));
        System.out.printf("[背压] 第 %d 条 => accepted=%s reason=%s detail=%s%n",
                CAPACITY + 1, overflow.accepted(), overflow.rejectReason(), overflow.detail());
        assertFalse(overflow.accepted());
        assertEquals(RejectReason.BUFFER_FULL, overflow.rejectReason());
        assertEquals(CAPACITY, store.pendingCount(), "拒绝的写绝不能进入缓冲");

        // 模拟补发释放一个槽位后可继续写
        store.advanceCheckpoint(1);
        IngestResult afterDrain = ingest.ingest(report("full", 999, 1727500100000L));
        System.out.printf("[背压] 位点推进释放槽位后 => accepted=%s seq=%d pending=%d%n",
                afterDrain.accepted(), afterDrain.ingestSeq(), store.pendingCount());
        assertTrue(afterDrain.accepted());
        assertEquals(CAPACITY, store.pendingCount());
        System.out.println("[背压] 判定依据：满则明确拒绝 BUFFER_FULL，补发腾出槽位后恢复可写");
    }

    @Test
    void concurrentWritersLoseNothingAndStayConsistent() throws Exception {
        int threads = 8;
        int perThread = 50; // 总量 400 > 容量 20，必然发生背压拒绝
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger acceptedCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();
        ConcurrentLinkedQueue<Long> acceptedSeqs = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int worker = t;
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    String body = "{\"deviceId\":\"dev-" + worker + "\",\"ts\":"
                            + (1727500000000L + worker * 1000L + i)
                            + ",\"reportId\":\"w" + worker + "-" + i + "\","
                            + "\"metrics\":{\"v\":" + i + "}}";
                    IngestResult r = ingest.ingest(body);
                    if (r.accepted() && !r.duplicate()) {
                        acceptedCount.incrementAndGet();
                        acceptedSeqs.add(r.ingestSeq());
                    } else if (!r.accepted()) {
                        rejectedCount.incrementAndGet();
                        assertEquals(RejectReason.BUFFER_FULL, r.rejectReason());
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long distinctSeqs = acceptedSeqs.stream().distinct().count();
        System.out.printf("[并发] 线程=%d 每线程=%d 接受=%d 拒绝=%d 不同seq=%d 缓冲待发=%d%n",
                threads, perThread, acceptedCount.get(), rejectedCount.get(),
                distinctSeqs, store.pendingCount());
        assertEquals(acceptedCount.get(), distinctSeqs, "每个接受的写必须拿到唯一序号，不得串号");
        assertEquals(acceptedCount.get(), store.pendingCount(), "接受数必须等于缓冲内记录数，不丢数据");
        assertEquals(threads * perThread, acceptedCount.get() + rejectedCount.get(),
                "每次写都必须有明确结论：接受或背压拒绝");
        System.out.println("[并发] 判定依据：接受序号唯一、与缓冲记录一一对应、无静默丢弃（拒绝均为 BUFFER_FULL）");
    }

    @Test
    void checkpointRollbackMustBeRejected() {
        for (int i = 1; i <= 5; i++) {
            assertTrue(ingest.ingest(report("cb", i, 1727500000000L + i)).accepted());
        }
        store.advanceCheckpoint(3);
        System.out.println("[位点] 正常推进后 frontier=" + store.frontier());

        CheckpointRollbackException ex = assertThrows(CheckpointRollbackException.class,
                () -> store.advanceCheckpoint(2));
        System.out.printf("[位点] 回退请求 3->2 被拒绝: %s%n", ex.getMessage());
        assertEquals(3, store.frontier(), "拒绝回退，frontier 保持不变");

        CheckpointRollbackException same = assertThrows(CheckpointRollbackException.class,
                () -> store.advanceCheckpoint(3));
        System.out.printf("[位点] 重复确认 3（不前进）同样被拒绝: %s%n", same.getMessage());
        assertEquals(3, store.frontier());

        store.advanceCheckpoint(4);
        assertEquals(4, store.frontier());
        System.out.println("[位点] 判定依据：位点只能单调前进，回退/重复均明确拒绝，只能继续前进到 4");
    }

    private String report(String tag, int i, long ts) {
        return "{\"deviceId\":\"dev-" + tag + "\",\"ts\":" + ts
                + ",\"reportId\":\"" + tag + "-" + i + "\",\"metrics\":{\"v\":" + i + "}}";
    }

    @Test
    void lockTimeoutProducesCongestionRejection() throws Exception {
        // 容量 20、锁等待 200ms（见 setUp）。人为长时间持锁，模拟处理能力被打满，
        // 采集线程必须在超时后得到 WRITE_CONGESTION 结论，而不是无限阻塞主流程。
        java.lang.reflect.Field lockField = LocalFileEventStore.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock internal =
                (java.util.concurrent.locks.ReentrantLock) lockField.get(store);

        CountDownLatch lockHeld = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            internal.lock();
            lockHeld.countDown();
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                internal.unlock();
            }
        });
        holder.start();
        lockHeld.await();

        long t0 = System.currentTimeMillis();
        IngestResult r = ingest.ingest(report("cong", 1, 1727500000000L));
        long waited = System.currentTimeMillis() - t0;
        holder.join();

        System.out.printf("[拥塞] 持锁期间写入 => accepted=%s reason=%s detail=%s 等待=%dms%n",
                r.accepted(), r.rejectReason(), r.detail(), waited);
        assertFalse(r.accepted());
        assertEquals(RejectReason.WRITE_CONGESTION, r.rejectReason());
        assertTrue(waited < 500, "必须在锁超时附近快速失败，不得长时间阻塞采集线程");
        System.out.println("[拥塞] 判定依据：锁竞争超时被明确降级拒绝，采集主流程只等待约一个超时周期");
    }
}
