package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import java.util.Map;
import java.util.TreeMap;

/**
 * 幂等键规则：
 * 1) 报文显式携带 reportId 时：deviceId + "#" + reportId（设备侧唯一报文号，优先级最高）；
 * 2) 否则用业务指纹：deviceId|eventTime|排序后的指标键值，防止同一时刻同值重复上报。
 * 指标值按规范化 double 比较，属性不参与指纹（属性不构成业务数据点差异）。
 */
final class IdempotencyKeys {

    private IdempotencyKeys() {
    }

    static String build(String deviceId, String reportId, long eventTimeMillis,
                        Map<String, Double> metrics) {
        if (reportId != null && !reportId.isBlank()) {
            return deviceId + "#" + reportId.trim();
        }
        TreeMap<String, Double> sorted = new TreeMap<>(metrics);
        StringBuilder sb = new StringBuilder(deviceId).append('|').append(eventTimeMillis).append('|');
        sorted.forEach((k, v) -> sb.append(k).append('=').append(format(v)).append(';'));
        return sb.toString();
    }

    private static String format(double v) {
        if (v == Math.rint(v) && !Double.isInfinite(v)) {
            return Long.toString((long) v);
        }
        return Double.toString(v);
    }
}
