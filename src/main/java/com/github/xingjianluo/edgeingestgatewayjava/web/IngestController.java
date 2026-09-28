package com.github.xingjianluo.edgeingestgatewayjava.web;

import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备上报入口：POST /ingest/{format}，format ∈ {json-v1, json-v2, line}。
 *
 * <p>状态码语义：
 * <ul>
 *   <li>202 ACCEPTED：已接收并落盘缓冲；</li>
 *   <li>409 CONFLICT：重复上报（已按 dedupKey 去重）；</li>
 *   <li>422 UNPROCESSABLE_ENTITY：格式/字段/时间戳非法，body 携带可区分原因码；</li>
 *   <li>503 SERVICE_UNAVAILABLE：缓冲已满（背压），请稍后重试。</li>
 * </ul>
 */
@RestController
@RequestMapping("/ingest")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping("/{format}")
    public ResponseEntity<Map<String, Object>> ingest(@PathVariable String format,
                                                      @RequestBody(required = false) String body) {
        IngestResult result = ingestService.accept(format, body);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", result.getStatus());
        if (result.getSeq() != null) {
            payload.put("seq", result.getSeq());
        }
        if (result.getReasonCode() != null) {
            payload.put("reasonCode", result.getReasonCode());
            payload.put("reasonMessage", result.getReasonMessage());
        }
        return ResponseEntity.status(mapStatus(result.getStatus())).body(payload);
    }

    private static HttpStatus mapStatus(String status) {
        return switch (status) {
            case IngestResult.ACCEPTED -> HttpStatus.ACCEPTED;
            case IngestResult.DUPLICATE -> HttpStatus.CONFLICT;
            case IngestResult.BUFFER_FULL -> HttpStatus.SERVICE_UNAVAILABLE;
            default -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
    }
}
