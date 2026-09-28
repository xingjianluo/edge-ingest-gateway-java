package com.github.xingjianluo.edgeingestgatewayjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：多格式归一化 + 非法上报必须给出可区分的拒绝原因（不得静默忽略）。
 */
class NormalizationTest {

    private IngestService ingestService;
    private EventStore store;

    @BeforeEach
    void setUp(@TempDir Path tmp) {
        ObjectMapper mapper = new ObjectMapper();
        List<Normalizer> normalizers =
                List.of(new StandardJsonNormalizer(), new FlatJsonNormalizer(), new ProbeV2Normalizer());
        store = new LocalFileEventStore(tmp.resolve("store"), 100, 50, mapper);
        ingestService = new IngestService(normalizers, store, mapper, 300_000);
    }

    @Test
    void threeFormatsNormalizeToSameModel() {
        long ts = 1727500000000L;
        String standard = """
                {"deviceId":"dev-A","ts":%d,"reportId":"r1","metrics":{"temp":23.5}}
                """.formatted(ts);
        String flat = """
                {"device":"dev-B","time":%d,"temp":24.1,"rpm":1200,"msgId":"f1"}
                """.formatted(ts + 1000);
        String probe = """
                {"ver":"2","dev":{"id":"dev-C","loc":"rack-7"},"reportId":"p1",
                 "data":[{"k":"temp","v":30.2,"ts":%d},{"k":"humi","v":60,"ts":%d}]}
                """.formatted(ts + 2000, ts + 2000);

        for (String input : List.of(standard, flat, probe)) {
            IngestResult result = ingestService.ingest(input);
            System.out.printf("[归一化] 输入=%s => 接受=%s 重复=%s key=%s seq=%d%n",
                    input.replaceAll("\\s+", ""), result.accepted(), result.duplicate(),
                    result.idempotencyKey(), result.ingestSeq());
            assertTrue(result.accepted(), "应被接受: " + input);
            assertFalse(result.duplicate());
            assertNotNull(result.idempotencyKey());
        }
        assertEquals(3, store.pendingCount(), "三种格式应落到同一缓冲/统一模型");
        System.out.println("[归一化] 判定依据：三种格式各产生 1 条统一事件，缓冲待发=3");
    }

    @Test
    void rejectedReportsHaveDistinctReasons() {
        long futureTs = System.currentTimeMillis() + 3_600_000; // 1 小时后，超出 5 分钟容忍
        List<RejectCase> cases = List.of(
                new RejectCase("{not-json", RejectReason.MALFORMED_JSON, "非法 JSON"),
                new RejectCase("{}", RejectReason.UNSUPPORTED_FORMAT, "空对象/无法识别格式"),
                new RejectCase("[]", RejectReason.UNSUPPORTED_FORMAT, "顶层不是对象"),
                new RejectCase("""
                        {"ts":1727500000000,"metrics":{"temp":1}}""",
                        RejectReason.MISSING_DEVICE_ID, "缺设备号"),
                new RejectCase("""
                        {"deviceId":"d1","metrics":{"temp":1}}""",
                        RejectReason.MISSING_TIMESTAMP, "缺时间戳"),
                new RejectCase("""
                        {"deviceId":"d1","ts":"2026/09/28 10:00","metrics":{"temp":1}}""",
                        RejectReason.INVALID_TIMESTAMP_FORMAT, "时间戳格式不完整/无法解析"),
                new RejectCase("""
                        {"deviceId":"d1","ts":%d,"metrics":{"temp":1}}""".formatted(futureTs),
                        RejectReason.TIMESTAMP_OUT_OF_RANGE, "时间戳过远未来"),
                new RejectCase("""
                        {"deviceId":"d1","ts":0,"metrics":{"temp":1}}""",
                        RejectReason.TIMESTAMP_OUT_OF_RANGE, "时间戳非正数"),
                new RejectCase("""
                        {"deviceId":"d1","ts":1727500000000}""",
                        RejectReason.MISSING_METRICS, "缺 metrics"),
                new RejectCase("""
                        {"deviceId":"d1","ts":1727500000000,"metrics":{"temp":"hot"}}""",
                        RejectReason.INVALID_METRIC_VALUE, "指标值非数值"),
                new RejectCase("""
                        {"ver":"2","dev":{"id":"d1"},"data":[]}""",
                        RejectReason.MISSING_METRICS, "探针批量为空"));

        for (RejectCase c : cases) {
            IngestResult result = ingestService.ingest(c.input);
            System.out.printf("[拒绝] 场景=%-18s 输入=%s => 接受=%s 原因=%s 说明=%s%n",
                    c.desc, c.input.replaceAll("\\s+", ""), result.accepted(),
                    result.rejectReason(), result.detail());
            assertFalse(result.accepted(), "应被拒绝: " + c.desc);
            assertEquals(c.expected, result.rejectReason(),
                    "拒绝原因不匹配: " + c.desc + " 实际: " + result.detail());
            assertNotNull(result.detail(), "拒绝必须附带可解释说明");
        }
        assertEquals(0, store.pendingCount(), "任何非法上报都不得进入缓冲");
        System.out.println("[拒绝] 判定依据：11 类非法输入全部带不同原因拒绝，缓冲仍为空，无静默丢弃");
    }

    private record RejectCase(String input, RejectReason expected, String desc) {
    }
}
