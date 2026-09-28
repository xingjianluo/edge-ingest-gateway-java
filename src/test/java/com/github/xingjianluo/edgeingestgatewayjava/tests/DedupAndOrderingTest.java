package com.github.xingjianluo.edgeingestgatewayjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：重复上报（显式 reportId / 业务指纹）与乱序到达。
 */
class DedupAndOrderingTest {

    private ObjectMapper mapper;
    private EventStore store;
    private IngestService ingest;
    private FakeUpstreamClient upstream;
    private RelayService relay;

    @BeforeEach
    void setUp(@TempDir Path tmp) {
        mapper = new ObjectMapper();
        List<Normalizer> normalizers =
                List.of(new StandardJsonNormalizer(), new FlatJsonNormalizer(), new ProbeV2Normalizer());
        store = new LocalFileEventStore(tmp.resolve("store"), 100, 50, mapper);
        ingest = new IngestService(normalizers, store, mapper, 300_000);
        upstream = new FakeUpstreamClient();
        relay = new RelayService(store, upstream);
    }

    @Test
    void duplicateReportByIdAndByFingerprint() {
        String first = "{\"deviceId\":\"dev-1\",\"ts\":1727500000000,\"reportId\":\"R-9\",\"metrics\":{\"temp\":1}}";
        String repeat = "{\"deviceId\":\"dev-1\",\"timestamp\":1727500000000,\"reportId\":\"R-9\","
                + "\"metrics\":{\"temp\":1}}"; // 同一 reportId，时间戳字段名不同
        // 两条均不带 reportId：靠 设备|业务时间|指标指纹 识别重复
        // 1727526405000 == 2024-09-28T12:26:45Z
        String fp1 = "{\"deviceId\":\"dev-1\",\"ts\":1727526405000,\"metrics\":{\"temp\":1}}";
        String fpRepeat = "{\"device\":\"dev-1\",\"time\":\"2024-09-28T12:26:45.000Z\","
                + "\"temp\":1}"; // 扁平格式表达同一数据点
        String differentValue =
                "{\"deviceId\":\"dev-1\",\"ts\":1727500000000,\"metrics\":{\"temp\":2}}"; // 同时间但值不同 -> 新事件

        IngestResult r1 = ingest.ingest(first);
        IngestResult r2 = ingest.ingest(repeat);
        IngestResult r3 = ingest.ingest(fp1);
        IngestResult r4 = ingest.ingest(fpRepeat);
        IngestResult r5 = ingest.ingest(differentValue);

        System.out.printf("[去重] 首次       accepted=%s dup=%s seq=%d%n", r1.accepted(), r1.duplicate(), r1.ingestSeq());
        System.out.printf("[去重] reportId   accepted=%s dup=%s seq=%d （期望 dup=true 且 seq 不变）%n",
                r2.accepted(), r2.duplicate(), r2.ingestSeq());
        System.out.printf("[去重] 指纹首次   accepted=%s dup=%s seq=%d%n", r3.accepted(), r3.duplicate(), r3.ingestSeq());
        System.out.printf("[去重] 指纹重复   accepted=%s dup=%s seq=%d （跨格式同设备/时间/指标值）%n",
                r4.accepted(), r4.duplicate(), r4.ingestSeq());
        System.out.printf("[去重] 值不同     accepted=%s dup=%s seq=%d （期望作为新事件）%n",
                r5.accepted(), r5.duplicate(), r5.ingestSeq());

        assertTrue(r1.accepted() && !r1.duplicate());
        assertTrue(r2.accepted() && r2.duplicate() && r2.ingestSeq() == r1.ingestSeq());
        assertTrue(r3.accepted() && !r3.duplicate());
        assertTrue(r4.accepted() && r4.duplicate() && r4.ingestSeq() == r3.ingestSeq());
        assertTrue(r5.accepted() && !r5.duplicate());
        assertEquals(3, store.pendingCount(), "5 次上报只应产生 3 条数据");
        System.out.println("[去重] 判定依据：重复返回原 seq，缓冲待发=3，无重复数据");
    }

    @Test
    void outOfOrderArrivalIsRelayedByBusinessTime() {
        // 入库顺序故意与业务时间相反
        String late = "{\"deviceId\":\"d\",\"ts\":1727500003000,\"reportId\":\"t3\",\"metrics\":{\"v\":3}}";
        String mid = "{\"deviceId\":\"d\",\"ts\":1727500002000,\"reportId\":\"t2\",\"metrics\":{\"v\":2}}";
        String early = "{\"deviceId\":\"d\",\"ts\":1727500001000,\"reportId\":\"t1\",\"metrics\":{\"v\":1}}";

        ingest.ingest(late);
        ingest.ingest(mid);
        ingest.ingest(early);
        System.out.println("[乱序] 入库业务时间顺序: 3000 -> 2000 -> 1000（乱序到达）");

        List<SpooledEvent> pending = store.loadPending();
        System.out.printf("[乱序] 补发加载顺序: %s%n",
                pending.stream().map(e -> e.eventTimeMillis() + "(seq=" + e.seq() + ")").toList());
        assertEquals(List.of(1727500001000L, 1727500002000L, 1727500003000L),
                pending.stream().map(SpooledEvent::eventTimeMillis).toList());

        RelayService.PumpResult result = relay.pumpOnce();
        System.out.println("[乱序] 补发结果: " + result);
        assertEquals(3, result.delivered());
        assertEquals(3, upstream.received().size());
        assertEquals(0, store.pendingCount());
        assertFalse(result.interrupted());
        System.out.println("[乱序] 判定依据：补发严格按业务时间 1000->2000->3000，位点推进到 3，缓冲清空");
    }

    @Test
    void interruptionDoesNotDuplicateOnRestartOfPump() throws Exception {
        for (int i = 1; i <= 3; i++) {
            ingest.ingest("{\"deviceId\":\"d\",\"ts\":%d,\"reportId\":\"r%d\",\"metrics\":{\"v\":%d}}"
                    .formatted(1727500000000L + i * 1000, i, i));
        }
        upstream.setAvailable(false);
        RelayService.PumpResult down = relay.pumpOnce();
        System.out.println("[中断] 断连时补发: " + down + "（期望 0 投递，缓冲保持 3 条）");
        assertTrue(down.interrupted());
        assertEquals(0, down.delivered());
        assertEquals(3, store.pendingCount());
        assertEquals(0, store.frontier());

        upstream.setAvailable(true);
        RelayService.PumpResult up = relay.pumpOnce();
        // 再补一次：模拟重试轮次，确认已确认事件不会重复投递
        RelayService.PumpResult secondRound = relay.pumpOnce();
        System.out.println("[中断] 恢复后第一轮: " + up);
        System.out.println("[中断] 恢复后第二轮: " + secondRound + "（期望 0 次重复投递）");

        assertEquals(3, up.delivered());
        assertEquals(0, secondRound.delivered());
        assertEquals(3, upstream.received().size(), "云端按幂等键去重后只有 3 条");
        upstream.received().keySet().forEach(k ->
                assertEquals(1, upstream.deliverCount(k), "每条只成功投递一次: " + k));
        System.out.println("[中断] 判定依据：断连零丢失，恢复后顺序补发，第二轮零重复，frontier=3");
    }
}
