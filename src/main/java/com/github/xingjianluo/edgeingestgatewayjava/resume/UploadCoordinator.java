package com.github.xingjianluo.edgeingestgatewayjava.resume;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

import java.util.List;

/**
 * 补发协调器。
 *
 * <p>每轮：
 * <ol>
 *   <li>从缓冲按业务时间顺序读一批（读取不推进位点）；</li>
 *   <li>整批投递到远端；远端幂等，故失败后整批重试不会重复；</li>
 *   <li>成功才提交位点（delivered + watermark）；失败则停下等待下一轮，数据仍在本地。</li>
 * </ol>
 * 因此：中断期间数据只落本地；恢复后按业务时间顺序补发；崩溃在“投递后、提交前”也安全——
 * 重启后重投整批，远端幂等去重，最终不重不丢。
 */
public class UploadCoordinator {

    private final LocalSpoolBuffer buffer;
    private final RemoteSink sink;
    private final int batchSize;

    public UploadCoordinator(LocalSpoolBuffer buffer, RemoteSink sink, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize 必须为正数");
        }
        this.buffer = buffer;
        this.sink = sink;
        this.batchSize = batchSize;
    }

    /**
     * 补发一轮。
     *
     * @return 本轮结果：attempted=读取条数，delivered=实际成功条数，sinkUnavailable=是否被断线挡住
     */
    public PumpResult pumpOnce() {
        List<LocalSpoolBuffer.BufferedEntry> batch = buffer.nextBatch(batchSize);
        if (batch.isEmpty()) {
            return new PumpResult(0, 0, false);
        }
        List<UnifiedReport> reports = batch.stream().map(LocalSpoolBuffer.BufferedEntry::report).toList();
        boolean ok = sink.deliver(reports);
        if (!ok) {
            // 断线：不提交位点，数据保留在本地，恢复后整批重发（远端幂等）。
            return new PumpResult(batch.size(), 0, true);
        }
        List<Long> seqs = batch.stream().map(LocalSpoolBuffer.BufferedEntry::seq).toList();
        List<String> keys = reports.stream().map(UnifiedReport::getDedupKey).toList();
        buffer.commitBatch(seqs, keys);
        return new PumpResult(batch.size(), batch.size(), false);
    }

    /** 连续补发直到缓冲清空，或远端不可用时停止。返回成功投递的总条数。 */
    public int drainUntilBlocked() {
        int total = 0;
        while (true) {
            PumpResult r = pumpOnce();
            if (r.sinkUnavailable() || r.attempted() == 0) {
                return total;
            }
            total += r.delivered();
        }
    }

    public long pendingCount() {
        return buffer.pendingCount();
    }

    /** 单轮补发结果。 */
    public record PumpResult(int attempted, int delivered, boolean sinkUnavailable) { }
}
