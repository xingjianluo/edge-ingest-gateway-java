package com.github.xingjianluo.edgeingestgatewayjava.web;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.relay.RelayService;
import com.github.xingjianluo.edgeingestgatewayjava.relay.UpstreamClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备上报与运维接口。
 */
@RestController
@RequestMapping("/api")
public class IngestController {

    private final IngestService ingestService;
    private final UpstreamClient upstreamClient;
    private final EventStore eventStore;
    private final RelayService relayService;

    public IngestController(IngestService ingestService, UpstreamClient upstreamClient,
                            EventStore eventStore, RelayService relayService) {
        this.ingestService = ingestService;
        this.upstreamClient = upstreamClient;
        this.eventStore = eventStore;
        this.relayService = relayService;
    }

    /** 设备上报：任意受支持格式的原始 JSON 报文。 */
    @PostMapping("/reports")
    public ResponseEntity<IngestResult> report(@RequestBody String rawJson) {
        return ResponseEntity.ok(ingestService.ingest(rawJson));
    }

    /** 模拟断连/恢复（演示与测试用）：{"available":false|true}。 */
    @PostMapping("/upstream/available")
    public Map<String, Object> setUpstreamAvailable(@RequestBody Map<String, Boolean> body) {
        upstreamClient.setAvailable(Boolean.TRUE.equals(body.get("available")));
        return Map.of("available", upstreamClient.available());
    }

    /** 立即触发一轮补发，返回本轮判定依据（不等后台周期）。 */
    @PostMapping("/relay/pump")
    public RelayService.PumpResult pump() {
        return relayService.pumpOnce();
    }

    /** 可观测状态：缓冲占用、位点、上游连通性。 */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("status", "ok");
        status.put("upstreamAvailable", upstreamClient.available());
        status.put("pending", eventStore.pendingCount());
        status.put("capacity", eventStore.capacity());
        status.put("frontier", eventStore.frontier());
        status.put("relayRunning", relayService.isRunning());
        return status;
    }
}
