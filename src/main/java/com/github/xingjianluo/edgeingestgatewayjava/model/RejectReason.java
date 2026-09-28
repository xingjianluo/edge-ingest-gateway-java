package com.github.xingjianluo.edgeingestgatewayjava.model;

/**
 * 可区分的拒绝原因。
 */
public enum RejectReason {
    /** 报文不是合法 JSON。 */
    MALFORMED_JSON,
    /** 无法识别为任何受支持的上报格式。 */
    UNSUPPORTED_FORMAT,
    /** 缺少设备标识。 */
    MISSING_DEVICE_ID,
    /** 缺少业务时间戳。 */
    MISSING_TIMESTAMP,
    /** 缺少指标数据或指标为空。 */
    MISSING_METRICS,
    /** 时间戳格式无法解析。 */
    INVALID_TIMESTAMP_FORMAT,
    /** 时间戳超出允许范围（非正数/过远未来）。 */
    TIMESTAMP_OUT_OF_RANGE,
    /** 指标值不是数值。 */
    INVALID_METRIC_VALUE,
    /** 本地缓冲已满（背压拒绝）。 */
    BUFFER_FULL,
    /** 并发写锁竞争超时，处理能力超限（背压拒绝）。 */
    WRITE_CONGESTION
}
