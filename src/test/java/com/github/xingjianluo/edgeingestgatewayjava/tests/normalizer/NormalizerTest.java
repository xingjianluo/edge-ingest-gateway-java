package com.github.xingjianluo.edgeingestgatewayjava.tests.normalizer;

import com.github.xingjianluo.edgeingestgatewayjava.clock.ManualGatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.NormalizeResult;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ReportNormalizer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 归一化测试：覆盖三种格式与全部拒绝原因。每个用例打印输入与判定依据，保证结论可复现。
 */
class NormalizerTest {

    private final ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
    private final ReportNormalizer normalizer = new ReportNormalizer(clock);

    private void print(String label, String input, NormalizeResult result) {
        System.out.printf("[NormalizerTest] %s | 输入=%s | 判定=%s | 依据=%s%n",
                label, input,
                result.isAccepted() ? "ACCEPT" : "REJECT",
                result.isAccepted()
                        ? "dedupKey=" + result.getReport().getDedupKey()
                        : result.getRejectReason().code() + " - " + result.getRejectReason().message());
    }

    @Test
    void jsonV1AcceptsValidReport() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":21.5,\"ts\":1700000000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("json-v1 正常", body, r);
        assertTrue(r.isAccepted());
        assertEquals("d1", r.getReport().getDeviceId());
        assertEquals("temp", r.getReport().getMetric());
        assertEquals(21.5, r.getReport().getValue());
        assertEquals("d1#temp#1700000000000", r.getReport().getDedupKey());
    }

    @Test
    void jsonV2AcceptsIsoTimestamp() {
        String body = "{\"dev_id\":\"d2\",\"ts_iso\":\"2023-11-14T22:13:20Z\",\"readings\":{\"temp\":21.5}}";
        NormalizeResult r = normalizer.normalize("json-v2", body);
        print("json-v2 ISO时间", body, r);
        assertTrue(r.isAccepted());
        assertEquals("d2", r.getReport().getDeviceId());
        assertEquals("temp", r.getReport().getMetric());
        // 2023-11-14T22:13:20Z == 1700000000000ms
        assertEquals(1_700_000_000_000L, r.getReport().getBusinessTime().toEpochMilli());
    }

    @Test
    void lineFormatAcceptsValidReport() {
        String body = "d3,temp value=21.5 1700000000000";
        NormalizeResult r = normalizer.normalize("line", body);
        print("line 正常", body, r);
        assertTrue(r.isAccepted());
        assertEquals("d3", r.getReport().getDeviceId());
        assertEquals("temp", r.getReport().getMetric());
    }

    @Test
    void rejectsEmptyBody() {
        NormalizeResult r = normalizer.normalize("json-v1", "");
        print("空请求体", "\"\"", r);
        assertFalse(r.isAccepted());
        assertEquals(RejectReason.EMPTY_BODY, r.getRejectReason());
    }

    @Test
    void rejectsMissingDeviceId() {
        String body = "{\"metric\":\"temp\",\"value\":1.0,\"ts\":1700000000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("缺 deviceId", body, r);
        assertEquals(RejectReason.MISSING_DEVICE_ID, r.getRejectReason());
    }

    @Test
    void rejectsMissingMetric() {
        String body = "{\"deviceId\":\"d1\",\"value\":1.0,\"ts\":1700000000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("缺 metric", body, r);
        assertEquals(RejectReason.MISSING_METRIC, r.getRejectReason());
    }

    @Test
    void rejectsMissingValue() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"ts\":1700000000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("缺 value", body, r);
        assertEquals(RejectReason.MISSING_VALUE, r.getRejectReason());
    }

    @Test
    void rejectsMissingTimestamp() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("缺时间戳", body, r);
        assertEquals(RejectReason.MISSING_TIMESTAMP, r.getRejectReason());
    }

    @Test
    void rejectsIllegalTimestampText() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0,\"ts\":\"not-a-time\"}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("非法时间戳文本", body, r);
        assertEquals(RejectReason.ILLEGAL_TIMESTAMP, r.getRejectReason());
    }

    @Test
    void rejectsOutOfRangeTimestamp() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0,\"ts\":100}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("时间戳超范围", body, r);
        assertEquals(RejectReason.ILLEGAL_TIMESTAMP, r.getRejectReason());
    }

    @Test
    void rejectsIllegalValue() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":\"abc\",\"ts\":1700000000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("非法 value", body, r);
        assertEquals(RejectReason.ILLEGAL_VALUE, r.getRejectReason());
    }

    @Test
    void rejectsMalformedJson() {
        String body = "{\"deviceId\":\"d1\",\"metric\":";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("JSON 截断", body, r);
        assertEquals(RejectReason.MALFORMED_FORMAT, r.getRejectReason());
    }

    @Test
    void rejectsMalformedLine() {
        String body = "d1,temp value=21.5";
        NormalizeResult r = normalizer.normalize("line", body);
        print("line 缺时间戳段", body, r);
        assertEquals(RejectReason.MALFORMED_FORMAT, r.getRejectReason());
    }

    @Test
    void rejectsUnknownFormat() {
        String body = "{\"deviceId\":\"d1\"}";
        NormalizeResult r = normalizer.normalize("xml", body);
        print("未知格式", body, r);
        assertEquals(RejectReason.MALFORMED_FORMAT, r.getRejectReason());
    }

    @Test
    void epochSecondsAreAutoScaledToMillis() {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0,\"ts\":1700000000}";
        NormalizeResult r = normalizer.normalize("json-v1", body);
        print("秒级时间戳", body, r);
        assertTrue(r.isAccepted());
        assertEquals(1_700_000_000_000L, r.getReport().getBusinessTime().toEpochMilli());
    }
}
