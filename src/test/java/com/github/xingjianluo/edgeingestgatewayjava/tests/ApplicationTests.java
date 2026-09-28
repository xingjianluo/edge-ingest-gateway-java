package com.github.xingjianluo.edgeingestgatewayjava.tests;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.relay.FakeUpstreamClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * HTTP 端到端：通过 REST 接口覆盖 接收 -> 断连落本地 -> 恢复后后台续传 的完整链路。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "edge.gateway.buffer-capacity=50",
                "edge.gateway.relay-interval-millis=80"
        })
public class ApplicationTests {

    @TempDir
    static Path sharedTempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        // 必须在上下文启动前指定，缓冲文件全部落在临时目录
        registry.add("edge.gateway.data-dir", () -> sharedTempDir.resolve("e2e").toString());
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EventStore store;

    @Autowired
    private FakeUpstreamClient upstream;

    @Test
    public void reportThenDisconnectThenRecoverOverHttp() throws Exception {
        // 1) 先把上游置为断连（清空历史，保证断言干净）
        upstream.received().clear();
        postJson("/api/upstream/available", "{\"available\":false}");
        System.out.println("[E2E] 上游已置为断连");

        // 2) 乱序发 3 条 + 1 条完全重复
        String[] reports = {
                "{\"deviceId\":\"dev-e2e\",\"ts\":1727500003000,\"reportId\":\"e3\",\"metrics\":{\"v\":3}}",
                "{\"deviceId\":\"dev-e2e\",\"ts\":1727500001000,\"reportId\":\"e1\",\"metrics\":{\"v\":1}}",
                "{\"deviceId\":\"dev-e2e\",\"ts\":1727500002000,\"reportId\":\"e2\",\"metrics\":{\"v\":2}}",
                "{\"deviceId\":\"dev-e2e\",\"ts\":1727500002000,\"reportId\":\"e2\",\"metrics\":{\"v\":2}}"
        };
        int accepted = 0;
        for (String body : reports) {
            Map<?, ?> resp = postJson("/api/reports", body);
            System.out.printf("[E2E] 上报 %s => accepted=%s duplicate=%s seq=%s%n",
                    body.substring(body.indexOf("\"reportId\"")),
                    resp.get("accepted"), resp.get("duplicate"), resp.get("ingestSeq"));
            assertEquals(Boolean.TRUE, resp.get("accepted"));
            if (Boolean.TRUE.equals(resp.get("duplicate"))) {
                accepted--;
            }
            accepted++;
        }
        assertEquals(3, accepted, "乱序 3 条 + 1 重复，只应产生 3 条");
        assertEquals(3, store.pendingCount(), "断连期间数据全部落在本地缓冲");
        System.out.println("[E2E] 判定依据：断连时 4 次 HTTP 上报，3 条落本地、1 条判重，云端收到 0 条");

        // 3) 非法报文必须带可区分原因
        Map<?, ?> bad = postJson("/api/reports", "{not-json");
        System.out.println("[E2E] 非法报文 => accepted=" + bad.get("accepted")
                + " reason=" + bad.get("rejectReason"));
        assertEquals(Boolean.FALSE, bad.get("accepted"));
        assertEquals("MALFORMED_JSON", bad.get("rejectReason"));

        // 4) 恢复上游，后台 relay 自动补发
        postJson("/api/upstream/available", "{\"available\":true}");
        System.out.println("[E2E] 上游恢复，等待后台续传...");
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline
                && (store.pendingCount() > 0 || upstream.received().size() < 3)) {
            Thread.sleep(50);
        }
        System.out.printf("[E2E] 补发后：云端收到 %d 条，本地待发 %d 条，frontier=%d%n",
                upstream.received().size(), store.pendingCount(), store.frontier());
        if (upstream.received().size() != 3 || store.pendingCount() != 0) {
            fail("恢复后应补发全部 3 条且缓冲清空");
        }
        // 云端必须按业务时间顺序收到 1000->2000->3000
        var keys = upstream.received().values().stream()
                .map(e -> e.eventTimeMillis()).toList();
        assertEquals(
                java.util.List.of(1727500001000L, 1727500002000L, 1727500003000L), keys);
        System.out.println("[E2E] 判定依据：恢复后云端按业务时间收到 1000->2000->3000，无重复无丢失");
    }

    private Map<?, ?> postJson(String path, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForObject(path, new HttpEntity<>(body, headers), Map.class);
    }
}
