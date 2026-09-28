package com.github.xingjianluo.edgeingestgatewayjava.model;

/**
 * 归一化/校验阶段的业务异常，携带可区分的拒绝原因。
 */
public class IngestException extends RuntimeException {

    private final RejectReason reason;

    public IngestException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
