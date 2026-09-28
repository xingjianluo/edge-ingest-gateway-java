package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 扁平格式（常见于简易 MQTT/HTTP 设备）：
 * <pre>
 * {
 *   "device": "dev-002",          // 或 "deviceId"
 *   "time": 1727500001000,        // 或 "t"，数字毫秒或 ISO-8601
 *   "temp": 24.1, "rpm": 1200,    // 除保留字段外的所有数值字段都视为指标
 *   "msgId": "abc"                // 可选，等同 reportId
 * }
 * </pre>
 */
public class FlatJsonNormalizer implements Normalizer {

    /** 保留字段：不作为指标。 */
    private static final Set<String> RESERVED =
            Set.of("device", "deviceid", "time", "t", "msgid", "reportid", "id");

    @Override
    public boolean supports(JsonNode root) {
        if (root == null || !root.isObject()) {
            return false;
        }
        return (root.has("device") || root.has("deviceId"))
                && (root.has("time") || root.has("t"));
    }

    @Override
    public UnifiedEvent normalize(JsonNode root) {
        String deviceId = Fields.requireText(root, RejectReason.MISSING_DEVICE_ID, "device", "deviceId");
        long ts = Timestamps.requireMillis(root, "time", "t");

        Map<String, Double> metrics = new LinkedHashMap<>();
        root.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            JsonNode v = entry.getValue();
            if (RESERVED.contains(name.toLowerCase()) || !v.isNumber()) {
                return;
            }
            metrics.put(name, v.asDouble());
        });
        Fields.requireNonEmptyMetrics(metrics);

        String reportId = Fields.optionalText(root, "msgId", "reportId", "id");
        // 扁平格式没有独立的属性对象；非数值、非保留字段作为属性保留，避免信息丢失
        Map<String, String> attributes = new LinkedHashMap<>();
        root.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            JsonNode v = entry.getValue();
            if (!RESERVED.contains(name.toLowerCase()) && !v.isNumber() && v.isValueNode()) {
                attributes.put(name, v.asText());
            }
        });
        String key = IdempotencyKeys.build(deviceId, reportId, ts, metrics);
        return new UnifiedEvent(key, deviceId, ts, reportId, metrics, attributes);
    }

    @Override
    public String formatName() {
        return "flat";
    }
}
