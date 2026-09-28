package com.github.xingjianluo.edgeingestgatewayjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.CheckpointRollbackException;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalFileEventStore;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.SpooledEvent;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.FlatJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ProbeV2Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.StandardJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.relay.FakeUpstreamClient;
import com.github.xingjianluo.edgeingestgatewayjava.relay.RelayService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：进程重启后的中断恢复、崩溃一致性、位点回退拒绝、时钟回拨安全。
 */
class RecoveryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private IngestService newIngest(EventStore store) {
        List<Normalizer> normalizers =
                List.of(new StandardJsonNormalizer(), new FlatJsonNormalizer(), new ProbeV2Normalizer());
        return new IngestService(normalizers, store, mapper, 300_000);
    }

    @Test
    void restartRelaysUnackedDataWithoutLossOrDuplication(@TempDir Path baseDir) {
        Path dir = baseDir.resolve("store");
        // ---- 第一个"进程"：写入 5 条，上游中断，只补发前 2 条后宕机 ----
        EventStore store1 = new LocalFileEventStore(dir, 100, 50, mapper);
        IngestService ingest1 = newIngest(store1);
        FakeUpstreamClient upstream1 = new FakeUpstreamClient();
        RelayService relay1 = new RelayService(store1, upstream1);
        for (int i = 1; i <= 5; i++) {
            assertTrue(ingest1.ingest(report("rst", i, 1727500000000L + i * 1000)).accepted());
        }
        upstream1.setAvailable(true);
        // 手动模拟"只确认了前 2 条"（等价于在第 3 条发送前进程被 kill）：
        List<SpooledEvent> pending = store1.loadPending();
        upstream1.publish(pending.get(0));
        upstream1.publish(pending.get(1));
        store1.advanceCheckpoint(2);
        System.out.printf("[恢复] 进程1：写入5条，仅确认 seq<=2 后崩溃；frontier=%d pending=%d%n",
                store1.frontier(), store1.pendingCount());
        assertEquals(3, store1.pendingCount());

        // ---- 第二个"进程"：同一目录启动 ----
        EventStore store2 = new LocalFileEventStore(dir, 100, 50, mapper);
        IngestService ingest2 = newIngest(store2);
        FakeUpstreamClient upstream2 = new FakeUpstreamClient();
        RelayService relay2 = new RelayService(store2, upstream2);

        System.out.printf("[恢复] 进程2启动：frontier=%d 待补发=%d%n",
                store2.frontier(), store2.pendingCount());
        assertEquals(2, store2.frontier(), "位点必须从磁盘恢复");
        assertEquals(3, store2.pendingCount(), "未确认数据必须全部还在");

        // 重启后重放历史报文：必须识别为重复，而不是再写一条
        IngestResult repeat = ingest2.ingest(report("rst", 1, 1727500001000L));
        System.out.printf("[恢复] 重启后重放 seq1 报文 => accepted=%s dup=%s seq=%d%n",
                repeat.accepted(), repeat.duplicate(), repeat.ingestSeq());
        assertTrue(repeat.duplicate());
        assertEquals(3, store2.pendingCount(), "重放不得产生新数据");

        RelayService.PumpResult pump = relay2.pumpOnce();
        System.out.println("[恢复] 进程2补发: " + pump);
        assertEquals(3, pump.delivered());
        assertEquals(5, store2.frontier());
        assertEquals(0, store2.pendingCount());

        CheckpointRollbackException rollback =
                assertThrows(CheckpointRollbackException.class, () -> store2.advanceCheckpoint(2));
        System.out.printf("[恢复] 尝试把位点从 5 回退到 2 => 拒绝: %s%n", rollback.getMessage());
        System.out.println("[恢复] 判定依据：重启后位点=2、待发3条，按业务时间补齐，回退被拒绝");
    }

    @Test
    void clockRollbackDoesNotReuseOrOverwriteSequence(@TempDir Path baseDir) {
        Path dir = baseDir.resolve("store");
        EventStore store = new LocalFileEventStore(dir, 100, 50, mapper);
        IngestService ingest = newIngest(store);
        assertTrue(ingest.ingest(report("clk", 1, 1727500000000L)).accepted());
        assertTrue(ingest.ingest(report("clk", 2, 1727500001000L)).accepted());

        // 时钟大幅回拨本身无法在 Java 内直接操纵，但可以证明判定不依赖墙上时钟：
        // 序号来自持久化计数器而非时间戳；重放旧时间点的不同事件仍会拿到全新递增序号。
        SpooledEvent before = store.loadPending().get(1);
        System.out.printf("[时钟] 已存在 seq=%d eventTime=%d wallClock=%d%n",
                before.seq(), before.eventTimeMillis(), before.ingestWallClock());
        String oldTimeNewData =
                "{\"deviceId\":\"dev-clk\",\"ts\":1,\"reportId\":\"ancient-1\",\"metrics\":{\"v\":9}}";
        IngestResult r = ingest.ingest(oldTimeNewData);
        System.out.printf("[时钟] 写入业务时间=1ms 的远古事件 => accepted=%s seq=%d（继续递增，不覆盖旧序号）%n",
                r.accepted(), r.ingestSeq());
        assertTrue(r.accepted() && r.ingestSeq() == 3);
        assertEquals(3, store.pendingCount());
        System.out.println("[时钟] 判定依据：序号分配与墙上时钟无关，时钟回拨/极早时间都不会覆盖任何数据");
    }

    @Test
    void crashBetweenSpoolAndDedupWritesIsReconciled(@TempDir Path baseDir) throws Exception {
        Path dir = baseDir.resolve("store");
        Files.createDirectories(dir);
        // 手工构造"spool 已落盘但 dedup 索引缺失"的崩溃现场
        String spoolLine1 = "{\"seq\":1,\"key\":\"dev-x#c1\",\"deviceId\":\"dev-x\","
                + "\"eventTime\":1727500000000,\"wallClock\":1,\"payload\":\"{}\"}";
        String spoolLine2 = "{\"seq\":2,\"key\":\"dev-x#c2\",\"deviceId\":\"dev-x\","
                + "\"eventTime\":1727500001000,\"wallClock\":2,\"payload\":\"{}\"}";
        Files.writeString(dir.resolve("spool.log"), spoolLine1 + System.lineSeparator()
                + spoolLine2 + System.lineSeparator(), StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        Files.writeString(dir.resolve("checkpoint"), "0", StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        // 注意：不创建 dedup.idx —— 模拟崩溃发生在 spool fsync 之后、dedup 写入之前

        EventStore store = new LocalFileEventStore(dir, 100, 50, mapper);
        IngestService ingest = newIngest(store);
        assertEquals(2, store.pendingCount(), "spool 中的数据必须全部恢复，不能因索引缺失而丢");

        // 构造一条与崩溃现场同幂等键（dev-x#c1）的报文，验证索引重建后仍能去重
        String sameKey = "{\"deviceId\":\"dev-x\",\"ts\":1727500000000,"
                + "\"reportId\":\"c1\",\"metrics\":{\"v\":0}}";
        IngestResult dup = ingest.ingest(sameKey);
        System.out.printf("[崩溃恢复] dedup 索引缺失时重放 c1 => accepted=%s dup=%s seq=%d%n",
                dup.accepted(), dup.duplicate(), dup.ingestSeq());
        assertTrue(dup.duplicate() && dup.ingestSeq() == 1,
                "以 spool 重建索引后，旧数据仍可去重");
        assertEquals(2, store.pendingCount());
        System.out.println("[崩溃恢复] 判定依据：spool 为准重建去重索引，2 条数据全恢复，重放不重复");
    }

    private String report(String tag, int i, long ts) {
        return "{\"deviceId\":\"dev-" + tag + "\",\"ts\":" + ts
                + ",\"reportId\":\"" + tag + "-" + i + "\",\"metrics\":{\"v\":" + i + "}}";
    }
}
