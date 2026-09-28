package com.github.xingjianluo.edgeingestgatewayjava.ingest;

/**
 * 上报被拒绝的可区分原因。每种原因对应稳定的错误码，便于调用方排查与测试断言。
 */
public enum RejectReason {
    EMPTY_BODY("EMPTY_BODY", "请求体为空"),
    MALFORMED_FORMAT("MALFORMED_FORMAT", "报文无法解析为所声明的格式"),
    MISSING_DEVICE_ID("MISSING_DEVICE_ID", "缺少设备标识 deviceId"),
    MISSING_METRIC("MISSING_METRIC", "缺少指标名 metric"),
    MISSING_VALUE("MISSING_VALUE", "缺少指标值 value"),
    MISSING_TIMESTAMP("MISSING_TIMESTAMP", "缺少业务时间戳"),
    ILLEGAL_TIMESTAMP("ILLEGAL_TIMESTAMP", "时间戳非法（无法解析或超出允许范围）"),
    ILLEGAL_VALUE("ILLEGAL_VALUE", "指标值不是合法数字");

    private final String code;
    private final String message;

    RejectReason(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String code() { return code; }
    public String message() { return message; }
}
