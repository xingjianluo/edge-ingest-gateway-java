package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.util.List;

/**
 * 接收入口编排：解析 JSON -> 选择格式归一化 -> 时间范围校验 -> 落缓冲（去重/背压）。
 * 任何拒绝都返回带 {@link RejectReason} 的结论，不静默丢弃。
 */
public class IngestService {

    private final List<Normalizer> normalizers;
    private final EventStore store;
    private final ObjectMapper objectMapper;
    private final long maxFutureSkewMillis;

    public IngestService(List<Normalizer> normalizers, EventStore store, ObjectMapper objectMapper,
                         long maxFutureSkewMillis) {
        this.normalizers = normalizers;
        this.store = store;
        this.objectMapper = objectMapper;
        this.maxFutureSkewMillis = maxFutureSkewMillis;
    }

    public IngestResult ingest(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return IngestResult.rejected(RejectReason.MALFORMED_JSON, "empty request body");
        }
        JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (JsonProcessingException e) {
            return IngestResult.rejected(RejectReason.MALFORMED_JSON,
                    "body is not valid JSON: " + e.getOriginalMessage());
        }
        if (root == null || !root.isObject()) {
            return IngestResult.rejected(RejectReason.UNSUPPORTED_FORMAT,
                    "top-level payload must be a JSON object");
        }

        Normalizer normalizer = null;
        for (Normalizer n : normalizers) {
            if (n.supports(root)) {
                normalizer = n;
                break;
            }
        }
        if (normalizer == null) {
            return IngestResult.rejected(RejectReason.UNSUPPORTED_FORMAT,
                    "no matching report format for payload: " + rawJson);
        }

        UnifiedEvent event;
        try {
            event = normalizer.normalize(root);
            Timestamps.validateRange(event.eventTimeMillis(), System.currentTimeMillis(),
                    maxFutureSkewMillis);
        } catch (IngestException e) {
            return IngestResult.rejected(e.reason(),
                    "[" + normalizer.formatName() + "] " + e.getMessage());
        }

        try {
            EventStore.AppendOutcome outcome = store.append(event);
            return outcome.duplicate()
                    ? IngestResult.duplicate(event.idempotencyKey(), outcome.seq())
                    : IngestResult.accepted(event.idempotencyKey(), outcome.seq());
        } catch (IngestException e) {
            // 背压拒绝（BUFFER_FULL / WRITE_CONGESTION）由存储层以业务异常给出
            return IngestResult.rejected(e.reason(), e.getMessage());
        }
    }
}
