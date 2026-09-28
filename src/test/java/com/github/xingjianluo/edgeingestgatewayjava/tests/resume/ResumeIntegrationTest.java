package com.github.xingjianluo.edgeingestgatewayjava.tests.resume;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.clock.ManualGatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestResult;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ReportNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;
import com.github.xingjianluo.edgeingestgatewayjava.resume.SimulatedRemoteSink;
import com.github.xingjianluo.edgeingestgatewayjava.resume.UploadCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线续传端到端：在线直发 → 断线落本地 → 恢复按业务时间补发 → 不重不丢。
 * 另覆盖“投递成功但提交位点前崩溃”的重启幂等场景。
 */
class ResumeIntegrationTest {

    @TempDir
    Path spoolDir;

    private static final long BASE = 1_700_000_000_000L;

    private String jsonV1(String device, String metric, long ts, double value) {
        return "{\"deviceId\":\"" + device + "\",\"metric\":\"" + metric
                + "\",\"value\":" + value + ",\"ts\":" + ts + "}";
    }

    @Test
    void outageBuffersLocallyThenBackfillsInBusinessTimeOrder() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        ReportNormalizer normalizer = new ReportNormalizer(clock);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 1000, clock);
        SimulatedRemoteSink sink = new SimulatedRemoteSink();
        IngestService ingest = new IngestService(normalizer, buffer);
        UploadCoordinator coordinator = new UploadCoordinator(buffer, sink, 2);

        // 1) 在线：3 条直接补发成功
        sink.setAvailable(true);
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 1_000L, 1.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 2_000L, 2.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 3_000L, 3.0));
        int online = coordinator.drainUntilBlocked();
        System.out.printf("[ResumeTest] 在线直发 | 写入3条 补发=%d 云端=%d | 判定依据=在线数据应全部送达%n",
                online, sink.deliveredUniqueCount());
        assertEquals(3, online);
        assertEquals(0, buffer.pendingCount());

        // 2) 断线：再写 4 条（含乱序），只能落本地
        sink.setAvailable(false);
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 6_000L, 6.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 4_000L, 4.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 7_000L, 7.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 5_000L, 5.0));
        int duringOutage = coordinator.drainUntilBlocked();
        System.out.printf("[ResumeTest] 断线期间 | 写入4条(乱序) 补发成功=%d 本地待补=%d | 判定依据=断线时不得送达，数据留在本地%n",
                duringOutage, buffer.pendingCount());
        assertEquals(0, duringOutage);
        assertEquals(4, buffer.pendingCount());
        assertEquals(3, sink.deliveredUniqueCount());

        // 3) 恢复：补发，必须按业务时间顺序，且不重复
        sink.setAvailable(true);
        int backfill = coordinator.drainUntilBlocked();
        List<Long> deliveredTimes = sink.deliveredSnapshot().stream()
                .map(r -> r.getBusinessTime().toEpochMilli()).toList();
        System.out.printf("[ResumeTest] 恢复补发 | 补发=%d 云端总数=%d 送达业务时间=%s | 判定依据=按业务时间升序且无重复%n",
                backfill, sink.deliveredUniqueCount(), deliveredTimes);
        assertEquals(4, backfill);
        assertEquals(7, sink.deliveredUniqueCount());
        assertEquals(List.of(BASE + 1_000L, BASE + 2_000L, BASE + 3_000L, BASE + 4_000L, BASE + 5_000L, BASE + 6_000L, BASE + 7_000L), deliveredTimes);
        assertEquals(0, buffer.pendingCount());
    }

    @Test
    void duplicateReportDuringOutageIsNotDoubleDelivered() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 1000, clock);
        SimulatedRemoteSink sink = new SimulatedRemoteSink();
        IngestService ingest = new IngestService(new ReportNormalizer(clock), buffer);
        UploadCoordinator coordinator = new UploadCoordinator(buffer, sink, 10);

        sink.setAvailable(false);
        IngestResult first = ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 1_000L, 1.0));
        IngestResult dup = ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 1_000L, 1.0));
        System.out.printf("[ResumeTest] 断线期重复上报 | 第一次=%s 第二次=%s | 判定依据=第二次应判重%n",
                first.getStatus(), dup.getStatus());
        assertEquals(IngestResult.ACCEPTED, first.getStatus());
        assertEquals(IngestResult.DUPLICATE, dup.getStatus());

        sink.setAvailable(true);
        coordinator.drainUntilBlocked();
        System.out.printf("[ResumeTest] 恢复后 | 云端唯一数=%d | 判定依据=重复上报只送达一次%n",
                sink.deliveredUniqueCount());
        assertEquals(1, sink.deliveredUniqueCount());
    }

    @Test
    void crashBetweenDeliverAndCommitIsIdempotentAfterRestart() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        ReportNormalizer normalizer = new ReportNormalizer(clock);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 1000, clock);
        SimulatedRemoteSink sink = new SimulatedRemoteSink();
        IngestService ingest = new IngestService(normalizer, buffer);

        sink.setAvailable(true);
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 1_000L, 1.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 2_000L, 2.0));

        // 手动模拟“投递成功但还没提交位点就崩溃”：只投递，不 commitBatch
        List<LocalSpoolBuffer.BufferedEntry> batch = buffer.nextBatch(10);
        boolean delivered = sink.deliver(batch.stream().map(LocalSpoolBuffer.BufferedEntry::report).toList());
        System.out.printf("[ResumeTest] 投递后崩溃 | 投递=%s 位点=%d | 判定依据=位点未推进，数据仍在本地%n",
                delivered, buffer.committedWatermarkSeq());
        assertTrue(delivered);
        assertEquals(0L, buffer.committedWatermarkSeq());
        buffer.close();

        // 重启：位点仍是 0，重投整批；云端幂等，不重复
        LocalSpoolBuffer reopened = new LocalSpoolBuffer(spoolDir, 1000, clock);
        UploadCoordinator coordinator = new UploadCoordinator(reopened, sink, 10);
        coordinator.drainUntilBlocked();
        System.out.printf("[ResumeTest] 重启重投 | 云端唯一数=%d 待补=%d | 判定依据=重投被云端幂等去重，最终不重不丢%n",
                sink.deliveredUniqueCount(), reopened.pendingCount());
        assertEquals(2, sink.deliveredUniqueCount());
        assertEquals(0, reopened.pendingCount());
    }

    @Test
    void pumpStopsWhenSinkUnavailableAndResumesLater() {
        ManualGatewayClock clock = new ManualGatewayClock(1_700_000_000_000L);
        LocalSpoolBuffer buffer = new LocalSpoolBuffer(spoolDir, 1000, clock);
        SimulatedRemoteSink sink = new SimulatedRemoteSink();
        IngestService ingest = new IngestService(new ReportNormalizer(clock), buffer);
        UploadCoordinator coordinator = new UploadCoordinator(buffer, sink, 2);

        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 1_000L, 1.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 2_000L, 2.0));
        ingest.accept("json-v1", jsonV1("d1", "temp", BASE + 3_000L, 3.0));

        sink.setAvailable(false);
        UploadCoordinator.PumpResult blocked = coordinator.pumpOnce();
        System.out.printf("[ResumeTest] 断线 pump | attempted=%d delivered=%d unavailable=%s | 判定依据=断线时本轮不送达%n",
                blocked.attempted(), blocked.delivered(), blocked.sinkUnavailable());
        assertTrue(blocked.sinkUnavailable());
        assertEquals(0, blocked.delivered());
        assertEquals(3, buffer.pendingCount());

        sink.setAvailable(true);
        int total = coordinator.drainUntilBlocked();
        System.out.printf("[ResumeTest] 恢复 drain | 补发=%d 云端=%d | 判定依据=恢复后全部补发%n",
                total, sink.deliveredUniqueCount());
        assertEquals(3, total);
        assertEquals(3, sink.deliveredUniqueCount());
    }
}
