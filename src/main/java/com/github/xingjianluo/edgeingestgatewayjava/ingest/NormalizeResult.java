package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

/**
 * 归一化结果：成功时携带统一模型，失败时携带可区分的拒绝原因。
 */
public final class NormalizeResult {

    private final boolean accepted;
    private final UnifiedReport report;
    private final RejectReason rejectReason;

    private NormalizeResult(boolean accepted, UnifiedReport report, RejectReason rejectReason) {
        this.accepted = accepted;
        this.report = report;
        this.rejectReason = rejectReason;
    }

    public static NormalizeResult ok(UnifiedReport report) {
        return new NormalizeResult(true, report, null);
    }

    public static NormalizeResult reject(RejectReason reason) {
        return new NormalizeResult(false, null, reason);
    }

    public boolean isAccepted() { return accepted; }
    public UnifiedReport getReport() { return report; }
    public RejectReason getRejectReason() { return rejectReason; }
}
