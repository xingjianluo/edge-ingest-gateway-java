package com.github.xingjianluo.edgeingestgatewayjava.tests.web;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.clock.ManualGatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ReportNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.web.IngestController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 接入层 Web 测试：验证 202/409/422/503 状态码与可解释响应体。
 */
class IngestControllerWebTest {

    @TempDir
    Path spoolDir;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        // 容量设为 2，便于稳定触发 503 背压
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 2, clock);
        IngestService service = new IngestService(new ReportNormalizer(clock), buffer);
        mockMvc = MockMvcBuilders.standaloneSetup(new IngestController(service)).build();
    }

    @Test
    void validReportReturns202WithSeq() throws Exception {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":21.5,\"ts\":1700000000000}";
        System.out.println("[WebTest] 正常上报 → 期望 202，body=" + body);
        mockMvc.perform(post("/ingest/json-v1").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.seq").value(1));
    }

    @Test
    void duplicateReturns409WithReason() throws Exception {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":21.5,\"ts\":1700000000000}";
        mockMvc.perform(post("/ingest/json-v1").content(body));
        System.out.println("[WebTest] 重复上报 → 期望 409 CONFLICT");
        mockMvc.perform(post("/ingest/json-v1").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value("DUPLICATE"))
                .andExpect(jsonPath("$.reasonCode").value("DUPLICATE"));
    }

    @Test
    void invalidTimestampReturns422WithReasonCode() throws Exception {
        String body = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0,\"ts\":\"bad-ts\"}";
        System.out.println("[WebTest] 非法时间戳 → 期望 422，reasonCode=ILLEGAL_TIMESTAMP，body=" + body);
        mockMvc.perform(post("/ingest/json-v1").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reasonCode").value("ILLEGAL_TIMESTAMP"));
    }

    @Test
    void emptyBodyReturns422() throws Exception {
        System.out.println("[WebTest] 空请求体 → 期望 422，reasonCode=EMPTY_BODY");
        mockMvc.perform(post("/ingest/json-v1").contentType(MediaType.APPLICATION_JSON).content(""))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reasonCode").value("EMPTY_BODY"));
    }

    @Test
    void bufferFullReturns503() throws Exception {
        String b1 = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":1.0,\"ts\":1700000000000}";
        String b2 = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":2.0,\"ts\":1700000001000}";
        String b3 = "{\"deviceId\":\"d1\",\"metric\":\"temp\",\"value\":3.0,\"ts\":1700000002000}";
        mockMvc.perform(post("/ingest/json-v1").content(b1)).andExpect(status().isAccepted());
        mockMvc.perform(post("/ingest/json-v1").content(b2)).andExpect(status().isAccepted());
        System.out.println("[WebTest] 缓冲容量=2 已满，第3条 → 期望 503 BUFFER_FULL");
        mockMvc.perform(post("/ingest/json-v1").contentType(MediaType.APPLICATION_JSON).content(b3))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("BUFFER_FULL"))
                .andExpect(jsonPath("$.reasonCode").value("BUFFER_FULL"));
    }
}
