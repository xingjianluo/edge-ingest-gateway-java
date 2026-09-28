package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.clock.GatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * 将多种设备上报格式归一为 {@link UnifiedReport}。
 *
 * <p>支持格式：
 * <ul>
 *   <li><b>json-v1</b>：{@code {"deviceId":"d1","metric":"temp","value":21.5,"ts":1700000000000}}</li>
 *   <li><b>json-v2</b>：{@code {"dev_id":"d1","ts_iso":"2023-11-14T22:13:20Z",
 *       "readings":{"temp":21.5}}}（readings 取第一个指标）</li>
 *   <li><b>line</b>：{@code d1,temp value=21.5 1700000000000}
 *       （设备,指标 空格 value=数字 空格 时间戳）</li>
 * </ul>
 *
 * <p>任何不合法输入都返回带明确 {@link RejectReason} 的结果，绝不抛异常或静默丢弃。
 * 时间戳接受 epoch 毫秒（≥1e12）、epoch 秒（&lt;1e12）与 ISO-8601。
 */
public class ReportNormalizer {

    /** 秒/毫秒判定阈值：小于该值的纯数字时间戳按秒解释。 */
    static final long SECOND_MILLIS_THRESHOLD = 1_000_000_000_000L;

    /** 允许的业务时间范围：2000-01-01 ~ 2100-01-01，超出视为非法，挡掉脏数据。 */
    static final long MIN_BUSINESS_MILLIS = 946_684_800_000L;
    static final long MAX_BUSINESS_MILLIS = 4_102_444_800_000L;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final GatewayClock clock;

    public ReportNormalizer(GatewayClock clock) {
        this.clock = clock;
    }

    public NormalizeResult normalize(String format, String body) {
        if (body == null || body.isBlank()) {
            return NormalizeResult.reject(RejectReason.EMPTY_BODY);
        }
        String fmt = format == null ? "" : format.trim().toLowerCase();
        try {
            return switch (fmt) {
                case "json-v1" -> normalizeJsonV1(body);
                case "json-v2" -> normalizeJsonV2(body);
                case "line" -> normalizeLine(body);
                default -> NormalizeResult.reject(RejectReason.MALFORMED_FORMAT);
            };
        } catch (NormalizeSignal signal) {
            return NormalizeResult.reject(signal.reason);
        } catch (RuntimeException ex) {
            // 解析器未预期的异常一律转为“格式错误”，保证入口不吞异常也不泄漏栈。
            return NormalizeResult.reject(RejectReason.MALFORMED_FORMAT);
        }
    }

    private NormalizeResult normalizeJsonV1(String body) {
        JsonNode root = readJsonObject(body);
        String deviceId = requireText(root, "deviceId", RejectReason.MISSING_DEVICE_ID);
        String metric = requireText(root, "metric", RejectReason.MISSING_METRIC);
        JsonNode valueNode = root.get("value");
        if (valueNode == null || valueNode.isNull()) {
            throw new NormalizeSignal(RejectReason.MISSING_VALUE);
        }
        double value = asDouble(valueNode, RejectReason.ILLEGAL_VALUE);
        Instant ts = requireTimestamp(root, "ts");
        return build(deviceId, metric, value, ts, "json-v1");
    }

    private NormalizeResult normalizeJsonV2(String body) {
        JsonNode root = readJsonObject(body);
        String deviceId = requireText(root, "dev_id", RejectReason.MISSING_DEVICE_ID);
        JsonNode readings = root.get("readings");
        if (readings == null || !readings.isObject() || readings.isEmpty()) {
            throw new NormalizeSignal(RejectReason.MISSING_METRIC);
        }
        java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = readings.fields();
        java.util.Map.Entry<String, JsonNode> first = fields.next();
        String metric = first.getKey();
        if (metric == null || metric.isBlank()) {
            throw new NormalizeSignal(RejectReason.MISSING_METRIC);
        }
        double value = asDouble(first.getValue(), RejectReason.ILLEGAL_VALUE);
        Instant ts = requireTimestamp(root, "ts_iso");
        return build(deviceId, metric, value, ts, "json-v2");
    }

    private NormalizeResult normalizeLine(String body) {
        // 形如：deviceId,metric value=21.5 1700000000000
        String trimmed = body.trim();
        String[] parts = trimmed.split(" ");
        if (parts.length != 3) {
            throw new NormalizeSignal(RejectReason.MALFORMED_FORMAT);
        }
        String[] head = parts[0].split(",", 2);
        if (head.length != 2 || head[0].isBlank() || head[1].isBlank()) {
            throw new NormalizeSignal(RejectReason.MALFORMED_FORMAT);
        }
        String deviceId = head[0].trim();
        String metric = head[1].trim();
        if (!parts[1].startsWith("value=")) {
            throw new NormalizeSignal(RejectReason.MISSING_VALUE);
        }
        double value;
        try {
            value = Double.parseDouble(parts[1].substring("value=".length()).trim());
        } catch (NumberFormatException ex) {
            throw new NormalizeSignal(RejectReason.ILLEGAL_VALUE);
        }
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new NormalizeSignal(RejectReason.ILLEGAL_VALUE);
        }
        Instant ts = parseTimestamp(parts[2].trim());
        return build(deviceId, metric, value, ts, "line");
    }

    private JsonNode readJsonObject(String body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            if (node == null || !node.isObject()) {
                throw new NormalizeSignal(RejectReason.MALFORMED_FORMAT);
            }
            return node;
        } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            throw new NormalizeSignal(RejectReason.MALFORMED_FORMAT);
        }
    }

    private static String requireText(JsonNode node, String field, RejectReason missing) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) {
            throw new NormalizeSignal(missing);
        }
        return v.asText().trim();
    }

    private static double asDouble(JsonNode node, RejectReason illegal) {
        if (node.isNumber()) {
            double d = node.asDouble();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new NormalizeSignal(illegal);
            }
            return d;
        }
        if (node.isTextual()) {
            try {
                double d = Double.parseDouble(node.asText().trim());
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    throw new NormalizeSignal(illegal);
                }
                return d;
            } catch (NumberFormatException ex) {
                throw new NormalizeSignal(illegal);
            }
        }
        throw new NormalizeSignal(illegal);
    }

    private static Instant requireTimestamp(JsonNode root, String field) {
        JsonNode tsNode = root.get(field);
        if (tsNode == null || tsNode.isNull() || tsNode.asText().isBlank()) {
            throw new NormalizeSignal(RejectReason.MISSING_TIMESTAMP);
        }
        return parseTimestamp(tsNode.asText().trim());
    }

    /**
     * 解析时间戳：纯数字按 epoch（秒/毫秒自适应），否则按 ISO-8601；再做范围校验。
     */
    static Instant parseTimestamp(String raw) {
        long millis;
        if (raw.chars().allMatch(Character::isDigit)) {
            long n;
            try {
                n = Long.parseLong(raw);
            } catch (NumberFormatException ex) {
                throw new NormalizeSignal(RejectReason.ILLEGAL_TIMESTAMP);
            }
            millis = n < SECOND_MILLIS_THRESHOLD ? n * 1000L : n;
        } else {
            try {
                millis = Instant.parse(raw).toEpochMilli();
            } catch (DateTimeParseException ex) {
                throw new NormalizeSignal(RejectReason.ILLEGAL_TIMESTAMP);
            }
        }
        if (millis < MIN_BUSINESS_MILLIS || millis > MAX_BUSINESS_MILLIS) {
            throw new NormalizeSignal(RejectReason.ILLEGAL_TIMESTAMP);
        }
        return Instant.ofEpochMilli(millis);
    }

    private NormalizeResult build(String deviceId, String metric, double value,
                                  Instant businessTime, String format) {
        // 去重键：设备 + 指标 + 毫秒级业务时间。同一设备同一指标同一毫秒的上报视为同一点位。
        String dedupKey = deviceId + "#" + metric + "#" + businessTime.toEpochMilli();
        UnifiedReport report = new UnifiedReport(
                deviceId, metric, value, businessTime, clock.now(), format, dedupKey);
        return NormalizeResult.ok(report);
    }

    /** 内部控制流信号：携带拒绝原因跳出解析。 */
    private static final class NormalizeSignal extends RuntimeException {
        private final RejectReason reason;

        NormalizeSignal(RejectReason reason) {
            super(reason.code(), null, false, false);
            this.reason = reason;
        }
    }
}
