package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 探针 v2 批量格式：
 * <pre>
 * {
 *   "ver": "2",
 *   "dev": {"id": "dev-003", "loc": "rack-7"},
 *   "reportId": "batch-9",                      // 可选
 *   "data": [
 *     {"k": "temp", "v": 30.2, "ts": 1727500002000},
 *     {"k": "humidity", "v": 60, "ts": 1727500002000}
 *   ]
 * }
 * </pre>
 * 一个报文归一为一条事件：指标取 data 数组聚合，业务时间取最早读数时间（补发排序更安全）。
 * 读数自带时间戳，也必须可解析；非法读数不允许静默跳过。
 */
public class ProbeV2Normalizer implements Normalizer {

    @Override
    public boolean supports(JsonNode root) {
        if (root == null || !root.isObject()) {
            return false;
        }
        JsonNode ver = root.get("ver");
        return ver != null && "2".equals(ver.asText())
                && root.has("dev") && root.has("data");
    }

    @Override
    public UnifiedEvent normalize(JsonNode root) {
        JsonNode dev = root.get("dev");
        if (dev == null || !dev.isObject()) {
            throw new IngestException(RejectReason.MISSING_DEVICE_ID, "missing object field 'dev'");
        }
        String deviceId = Fields.requireText(dev, RejectReason.MISSING_DEVICE_ID, "id", "deviceId");

        JsonNode data = root.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) {
            throw new IngestException(RejectReason.MISSING_METRICS,
                    "field 'data' must be a non-empty array");
        }
        Map<String, Double> metrics = new LinkedHashMap<>();
        long earliestTs = Long.MAX_VALUE;
        for (JsonNode reading : data) {
            if (!reading.isObject()) {
                throw new IngestException(RejectReason.UNSUPPORTED_FORMAT,
                        "each element of 'data' must be an object");
            }
            JsonNode kNode = reading.get("k");
            if (kNode == null || kNode.asText().isBlank()) {
                throw new IngestException(RejectReason.MISSING_METRICS,
                        "each reading requires non-empty field 'k'");
            }
            JsonNode vNode = reading.get("v");
            if (vNode == null || !vNode.isNumber()) {
                throw new IngestException(RejectReason.INVALID_METRIC_VALUE,
                        "reading '" + kNode.asText() + "' requires numeric field 'v'");
            }
            long readingTs = Timestamps.requireMillis(reading, "ts");
            metrics.merge(kNode.asText(), vNode.asDouble(), (a, b) -> b);
            earliestTs = Math.min(earliestTs, readingTs);
        }
        Fields.requireNonEmptyMetrics(metrics);

        String reportId = Fields.optionalText(root, "reportId", "msgId");
        Map<String, String> attributes = Fields.attributeObject(dev.get("attr"));
        JsonNode loc = dev.get("loc");
        if (loc != null && loc.isTextual()) {
            attributes.put("loc", loc.asText());
        }
        String key = IdempotencyKeys.build(deviceId, reportId, earliestTs, metrics);
        return new UnifiedEvent(key, deviceId, earliestTs, reportId, metrics, attributes);
    }

    @Override
    public String formatName() {
        return "probev2";
    }
}
