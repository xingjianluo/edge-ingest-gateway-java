package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.util.Map;

/**
 * 标准 JSON 格式：
 * <pre>
 * {
 *   "deviceId": "dev-001",            // 或 "device"
 *   "ts": 1727500000000,              // 或 "timestamp"，数字毫秒或 ISO-8601 字符串
 *   "reportId": "r-123",              // 可选，设备侧报文号，用于精确去重
 *   "metrics": {"temp": 23.5},        // 至少一个数值指标
 *   "attributes": {"site": "A"}       // 可选
 * }
 * </pre>
 */
public class StandardJsonNormalizer implements Normalizer {

    @Override
    public boolean supports(JsonNode root) {
        if (root == null || !root.isObject()) {
            return false;
        }
        // 标准格式的标志：存在 metrics 对象，或同时具备 deviceId/ts/timestamp
        return root.has("metrics")
                || (root.has("deviceId") && (root.has("ts") || root.has("timestamp")));
    }

    @Override
    public UnifiedEvent normalize(JsonNode root) {
        String deviceId = Fields.requireText(root, RejectReason.MISSING_DEVICE_ID, "deviceId", "device");
        long ts = Timestamps.requireMillis(root, "ts", "timestamp");
        JsonNode metricsNode = root.get("metrics");
        if (metricsNode == null || metricsNode.isNull()) {
            throw new IngestException(RejectReason.MISSING_METRICS, "missing field 'metrics'");
        }
        Map<String, Double> metrics = Fields.metricObject(metricsNode, "metrics");
        Fields.requireNonEmptyMetrics(metrics);
        String reportId = Fields.optionalText(root, "reportId", "id");
        Map<String, String> attributes = Fields.attributeObject(root.get("attributes"));
        String key = IdempotencyKeys.build(deviceId, reportId, ts, metrics);
        return new UnifiedEvent(key, deviceId, ts, reportId, metrics, attributes);
    }

    @Override
    public String formatName() {
        return "standard";
    }
}
