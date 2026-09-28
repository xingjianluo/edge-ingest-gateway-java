package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * 时间戳解析与合法性校验。
 * 支持：epoch 毫秒（数字或纯数字字符串）、ISO-8601（如 2026-09-28T10:00:00Z）。
 * 校验：可解析、必须为正、不得超过当前时间 + 允许偏移（防止时钟异常/脏数据）。
 */
final class Timestamps {

    private Timestamps() {
    }

    /**
     * 从 JSON 字段读取时间戳；字段缺失抛 MISSING_TIMESTAMP。
     */
    static long requireMillis(JsonNode parent, String... fieldNames) {
        for (String name : fieldNames) {
            JsonNode node = parent.get(name);
            if (node != null && !node.isNull()) {
                return parseMillis(node, name);
            }
        }
        throw new IngestException(RejectReason.MISSING_TIMESTAMP,
                "missing timestamp field, expected one of: " + String.join("/", fieldNames));
    }

    static long parseMillis(JsonNode node, String fieldName) {
        long millis;
        if (node.isNumber()) {
            millis = node.asLong();
        } else if (node.isTextual()) {
            String text = node.asText().trim();
            if (text.isEmpty()) {
                throw new IngestException(RejectReason.INVALID_TIMESTAMP_FORMAT,
                        "timestamp field '" + fieldName + "' is empty");
            }
            if (isDigits(text)) {
                try {
                    millis = Long.parseLong(text);
                } catch (NumberFormatException e) {
                    throw new IngestException(RejectReason.INVALID_TIMESTAMP_FORMAT,
                            "timestamp field '" + fieldName + "' is not a valid epoch millis: " + text);
                }
            } else {
                try {
                    millis = Instant.parse(text).toEpochMilli();
                } catch (DateTimeParseException e) {
                    throw new IngestException(RejectReason.INVALID_TIMESTAMP_FORMAT,
                            "timestamp field '" + fieldName + "' is neither epoch millis nor ISO-8601: " + text);
                }
            }
        } else {
            throw new IngestException(RejectReason.INVALID_TIMESTAMP_FORMAT,
                    "timestamp field '" + fieldName + "' must be number or string");
        }
        if (millis <= 0) {
            throw new IngestException(RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "timestamp field '" + fieldName + "' must be positive epoch millis: " + millis);
        }
        return millis;
    }

    /**
     * 业务时间戳的范围校验：允许一定的未来偏移；不限制历史时间（设备可能延迟上报）。
     */
    static void validateRange(long eventTimeMillis, long nowMillis, long maxFutureSkewMillis) {
        if (eventTimeMillis > nowMillis + maxFutureSkewMillis) {
            throw new IngestException(RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "event time " + eventTimeMillis + " is too far in the future (now="
                            + nowMillis + ", maxSkew=" + maxFutureSkewMillis + ")");
        }
    }

    private static boolean isDigits(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (i == 0 && c == '-') {
                continue;
            }
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return !text.isEmpty();
    }
}
