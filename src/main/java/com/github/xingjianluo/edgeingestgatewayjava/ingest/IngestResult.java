package com.github.xingjianluo.edgeingestgatewayjava.ingest;

/**
 * 接入结果。status 为稳定状态码：ACCEPTED / DUPLICATE / REJECTED / BUFFER_FULL。
 */
public final class IngestResult {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String DUPLICATE = "DUPLICATE";
    public static final String REJECTED = "REJECTED";
    public static final String BUFFER_FULL = "BUFFER_FULL";

    private final String status;
    private final Long seq;
    private final String reasonCode;
    private final String reasonMessage;

    private IngestResult(String status, Long seq, String reasonCode, String reasonMessage) {
        this.status = status;
        this.seq = seq;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
    }

    public static IngestResult accepted(long seq) {
        return new IngestResult(ACCEPTED, seq, null, null);
    }

    public static IngestResult duplicate(String dedupKey) {
        return new IngestResult(DUPLICATE, null, "DUPLICATE",
                "重复上报已被去重，dedupKey=" + dedupKey);
    }

    public static IngestResult rejected(RejectReason reason) {
        return new IngestResult(REJECTED, null, reason.code(), reason.message());
    }

    public static IngestResult bufferFull(String message) {
        return new IngestResult(BUFFER_FULL, null, "BUFFER_FULL", message);
    }

    public String getStatus() { return status; }
    public Long getSeq() { return seq; }
    public String getReasonCode() { return reasonCode; }
    public String getReasonMessage() { return reasonMessage; }
}
