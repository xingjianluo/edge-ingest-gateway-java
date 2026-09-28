package com.github.xingjianluo.edgeingestgatewayjava.model;

/**
 * 接收结论：接受（可能是重复上报）或拒绝（带原因）。
 */
public record IngestResult(boolean accepted, boolean duplicate, String idempotencyKey,
                           long ingestSeq, RejectReason rejectReason, String detail) {

    public static IngestResult accepted(String key, long seq) {
        return new IngestResult(true, false, key, seq, null, null);
    }

    public static IngestResult duplicate(String key, long seq) {
        return new IngestResult(true, true, key, seq, null, null);
    }

    public static IngestResult rejected(RejectReason reason, String detail) {
        return new IngestResult(false, false, null, -1L, reason, detail);
    }
}
