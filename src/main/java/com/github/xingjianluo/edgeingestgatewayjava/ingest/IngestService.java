package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.BufferRejectException;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

import java.util.Optional;

/**
 * 接入应用服务：归一化 → 入缓冲，产出统一的 {@link IngestResult}。
 * 归一化拒绝、重复上报、缓冲背压都会转成可解释结果，不吞异常、不静默丢弃。
 */
public class IngestService {

    private final ReportNormalizer normalizer;
    private final LocalSpoolBuffer buffer;

    public IngestService(ReportNormalizer normalizer, LocalSpoolBuffer buffer) {
        this.normalizer = normalizer;
        this.buffer = buffer;
    }

    public IngestResult accept(String format, String body) {
        NormalizeResult normalized = normalizer.normalize(format, body);
        if (!normalized.isAccepted()) {
            return IngestResult.rejected(normalized.getRejectReason());
        }
        UnifiedReport report = normalized.getReport();
        try {
            Optional<Long> seq = buffer.append(report);
            return seq.map(IngestResult::accepted)
                    .orElseGet(() -> IngestResult.duplicate(report.getDedupKey()));
        } catch (BufferRejectException ex) {
            return IngestResult.bufferFull(ex.getMessage());
        }
    }
}
