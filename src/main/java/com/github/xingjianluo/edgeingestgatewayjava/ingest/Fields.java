package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 归一化共用的字段提取工具。
 */
final class Fields {

    private Fields() {
    }

    /** 读取必填文本字段；缺失/空白抛 MISSING_*。 */
    static String requireText(JsonNode parent, RejectReason missingReason, String... fieldNames) {
        for (String name : fieldNames) {
            JsonNode node = parent.get(name);
            if (node != null && !node.isNull()) {
                String value = node.asText().trim();
                if (value.isEmpty()) {
                    throw new IngestException(missingReason,
                            "field '" + name + "' is present but empty");
                }
                return value;
            }
        }
        throw new IngestException(missingReason,
                "missing field, expected one of: " + String.join("/", fieldNames));
    }

    /** 读取可选文本字段。 */
    static String optionalText(JsonNode parent, String... fieldNames) {
        for (String name : fieldNames) {
            JsonNode node = parent.get(name);
            if (node != null && node.isTextual() && !node.asText().isBlank()) {
                return node.asText().trim();
            }
        }
        return null;
    }

    /**
     * 从 {"metric":number,...} 对象提取指标；null 或空对象由调用方按 MISSING_METRICS 处理。
     */
    static Map<String, Double> metricObject(JsonNode node, String fieldName) {
        Map<String, Double> metrics = new LinkedHashMap<>();
        if (node == null || !node.isObject()) {
            return metrics;
        }
        node.fields().forEachRemaining(entry -> {
            JsonNode v = entry.getValue();
            if (!v.isNumber()) {
                throw new IngestException(RejectReason.INVALID_METRIC_VALUE,
                        "metric '" + entry.getKey() + "' in '" + fieldName
                                + "' must be a number, got: " + v.getNodeType());
            }
            metrics.put(entry.getKey(), v.asDouble());
        });
        return metrics;
    }

    /** 提取可选字符串属性对象。 */
    static Map<String, String> attributeObject(JsonNode node) {
        Map<String, String> attributes = new LinkedHashMap<>();
        if (node == null || !node.isObject()) {
            return attributes;
        }
        node.fields().forEachRemaining(entry -> {
            if (entry.getValue() != null && !entry.getValue().isNull()) {
                attributes.put(entry.getKey(), entry.getValue().asText());
            }
        });
        return attributes;
    }

    static void requireNonEmptyMetrics(Map<String, Double> metrics) {
        if (metrics.isEmpty()) {
            throw new IngestException(RejectReason.MISSING_METRICS,
                    "metrics must contain at least one numeric value");
        }
    }
}
