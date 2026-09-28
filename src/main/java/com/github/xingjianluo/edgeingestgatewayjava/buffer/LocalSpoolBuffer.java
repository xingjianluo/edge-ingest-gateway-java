package com.github.xingjianluo.edgeingestgatewayjava.buffer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.xingjianluo.edgeingestgatewayjava.clock.GatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;
import com.github.xingjianluo.edgeingestgatewayjava.resume.OffsetStoreException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 有界的本地落盘缓冲（用本地文件模拟边缘网关的采集缓冲与上报位点）。
 *
 * <h3>磁盘布局（spoolDir）</h3>
 * <ul>
 *   <li>{@code pending.log}：append-only 行式 JSON，每行一条已接收但未确认送达的记录；</li>
 *   <li>{@code delivered.log}：append-only，每行 {@code seq|dedupKey}，记录已确认送达的点位；</li>
 *   <li>{@code watermark.dat}：位点文件，内容为已确认送达的最大序号，tmp+原子替换写入。</li>
 * </ul>
 *
 * <h3>并发与背压</h3>
 * 所有追加/读取/提交在实例锁内串行完成，本地追加只有一次顺序写 + fsync，耗时有界；
 * 容量满时立即抛 {@link BufferRejectException}（BUFFER_FULL）表达背压，而不是无限阻塞或静默丢弃。
 *
 * <h3>不重不丢</h3>
 * 去重键 dedupKey = 设备#指标#毫秒业务时间。序号 seq 来自独立持久计数器（从日志恢复），
 * 与墙钟无关，因此时钟回拨不会造成序号倒退或文件覆盖。
 *
 * <h3>重启恢复与位点</h3>
 * 启动时重放两个日志重建：待发集合 = pending 中未出现在 delivered 里的记录；
 * 位点必须满足 deliveredMax ≤ watermark ≤ 已分配最大 seq，否则判定为回退/损坏并拒绝启动。
 */
public class LocalSpoolBuffer {

    static final String PENDING_FILE = "pending.log";
    static final String DELIVERED_FILE = "delivered.log";
    static final String WATERMARK_FILE = "watermark.dat";

    private static final Comparator<BufferedEntry> BUSINESS_ORDER =
            Comparator.comparing((BufferedEntry e) -> e.report().getBusinessTime())
                    .thenComparingLong(BufferedEntry::seq);

    private final Path spoolDir;
    private final Path pendingFile;
    private final Path deliveredFile;
    private final Path watermarkFile;
    private final long capacity;
    private final GatewayClock clock;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ---- 内存状态（全部在 this 锁内访问）----
    /** seq -> 未送达记录。 */
    private final Map<Long, UnifiedReport> pendingBySeq = new HashMap<>();
    /** dedupKey -> seq，覆盖 pending 与 delivered，用于 O(1) 去重。 */
    private final Map<String, Long> keyIndex = new HashMap<>();
    /** 已送达 seq 集合（恢复用：判定 pending 中哪些已投递）。 */
    private final Set<Long> deliveredSeqs = new HashSet<>();
    /** 已分配的最大序号（与墙钟无关，单调递增）。 */
    private long maxSeq;
    /** 已确认送达的最大序号，即续传位点。 */
    private long watermark;

    private final AtomicLong acceptedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();

    public LocalSpoolBuffer(Path spoolDir, long capacity, GatewayClock clock) {
        this.spoolDir = spoolDir;
        this.capacity = capacity;
        this.clock = clock;
        this.pendingFile = spoolDir.resolve(PENDING_FILE);
        this.deliveredFile = spoolDir.resolve(DELIVERED_FILE);
        this.watermarkFile = spoolDir.resolve(WATERMARK_FILE);
        recover();
    }

    // ============================ 写入 ============================

    /**
     * 追加一条上报。
     *
     * @return 分配到的序号；重复上报返回 empty（调用方据此返回 DUPLICATE，不重复落盘）
     * @throws BufferRejectException 缓冲已满（BUFFER_FULL）
     */
    public Optional<Long> append(UnifiedReport report) {
        String dedupKey = report.getDedupKey();
        String line;
        long seq;
        synchronized (this) {
            if (keyIndex.containsKey(dedupKey)) {
                duplicateCount.incrementAndGet();
                return Optional.empty();
            }
            if (pendingBySeq.size() >= capacity) {
                throw new BufferRejectException("BUFFER_FULL",
                        "本地缓冲已满（容量=" + capacity + "），触发背压拒绝；请在链路恢复并补发后重试");
            }
            seq = ++maxSeq;
            line = encodePending(seq, report) + "\n";
            try {
                appendDurable(pendingFile, line);
            } catch (IOException ex) {
                throw new UncheckedIOException("落盘 pending 失败: " + ex.getMessage(), ex);
            }
            pendingBySeq.put(seq, report);
            keyIndex.put(dedupKey, seq);
            acceptedCount.incrementAndGet();
        }
        return Optional.of(seq);
    }

    // ============================ 读取（补发） ============================

    /**
     * 按业务时间（businessTime, seq）升序读取最多 max 条未送达数据。
     * 仅读取，不删除、不推进位点，因此读取后进程崩溃也不会丢数据。
     */
    public synchronized List<BufferedEntry> nextBatch(int max) {
        List<BufferedEntry> all = new ArrayList<>(pendingBySeq.size());
        for (Map.Entry<Long, UnifiedReport> e : pendingBySeq.entrySet()) {
            all.add(new BufferedEntry(e.getKey(), e.getValue()));
        }
        all.sort(BUSINESS_ORDER);
        if (all.size() <= max) {
            return all;
        }
        return new ArrayList<>(all.subList(0, max));
    }

    // ============================ 位点提交 ============================

    /**
     * 提交一批已成功送达的点位，幂等：重复提交同一批次不产生重复效果。
     * delivered.log 与 watermark 依次 fsync，watermark 以 tmp+原子替换落盘。
     */
    public synchronized void commitBatch(List<Long> seqs, List<String> dedupKeys) {
        if (seqs.size() != dedupKeys.size()) {
            throw new IllegalArgumentException("seqs 与 dedupKeys 数量不一致");
        }
        long batchMax = watermark;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < seqs.size(); i++) {
            long seq = seqs.get(i);
            String key = dedupKeys.get(i);
            if (deliveredSeqs.contains(seq)) {
                continue; // 幂等：重放提交直接跳过
            }
            if (seq > maxSeq || seq <= 0) {
                throw new OffsetStoreException("提交了从未分配的序号: " + seq);
            }
            sb.append(seq).append('|').append(key.replace('|', '/')).append('\n');
            deliveredSeqs.add(seq);
            keyIndex.put(key, seq);
            pendingBySeq.remove(seq);
            if (seq > batchMax) {
                batchMax = seq;
            }
        }
        try {
            if (!sb.isEmpty()) {
                appendDurable(deliveredFile, sb.toString());
            }
            if (batchMax > watermark) {
                writeWatermark(batchMax);
                watermark = batchMax;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("提交位点失败: " + ex.getMessage(), ex);
        }
    }

    /**
     * 显式把位点检查点写到指定值。新位点小于当前位点时明确拒绝（位点回退保护）。
     */
    public synchronized void checkpointWatermark(long newWatermark) {
        if (newWatermark < watermark) {
            throw new OffsetStoreException("拒绝位点回退：当前位点=" + watermark
                    + "，请求回退到=" + newWatermark + "；续传只能向前，不能从中间位点继续");
        }
        if (newWatermark > maxSeq) {
            throw new OffsetStoreException("拒绝非法位点：" + newWatermark
                    + " 超过已分配最大序号 " + maxSeq);
        }
        try {
            writeWatermark(newWatermark);
            watermark = newWatermark;
        } catch (IOException ex) {
            throw new UncheckedIOException("位点落盘失败", ex);
        }
    }

    public synchronized long committedWatermarkSeq() {
        return watermark;
    }

    public synchronized long pendingCount() {
        return pendingBySeq.size();
    }

    public long totalAccepted() {
        return acceptedCount.get();
    }

    public long totalDuplicates() {
        return duplicateCount.get();
    }

    public synchronized void close() {
        // 所有写操作都已 fsync，这里无需额外动作；方法保留用于生命周期对称。
    }

    // ============================ 恢复 ============================

    private void recover() {
        try {
            Files.createDirectories(spoolDir);
            if (!Files.exists(pendingFile)) {
                Files.createFile(pendingFile);
            }
            if (!Files.exists(deliveredFile)) {
                Files.createFile(deliveredFile);
            }

            // 1. 重放 delivered.log
            long deliveredMax = 0L;
            try (BufferedReader r = Files.newBufferedReader(pendingFile.getParent().resolve(DELIVERED_FILE))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    int sep = line.indexOf('|');
                    long seq = Long.parseLong(line.substring(0, sep));
                    String key = line.substring(sep + 1);
                    deliveredSeqs.add(seq);
                    keyIndex.put(key, seq);
                    if (seq > deliveredMax) {
                        deliveredMax = seq;
                    }
                }
            }

            // 2. 重放 pending.log，剔除已送达的
            long pendingMax = 0L;
            try (BufferedReader r = Files.newBufferedReader(pendingFile)) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    JsonNode node = objectMapper.readTree(line);
                    long seq = node.get("seq").asLong();
                    UnifiedReport report = decodeReport(node.get("r"));
                    if (!deliveredSeqs.contains(seq)) {
                        pendingBySeq.put(seq, report);
                        keyIndex.put(report.getDedupKey(), seq);
                    }
                    if (seq > pendingMax) {
                        pendingMax = seq;
                    }
                }
            }
            maxSeq = Math.max(pendingMax, deliveredMax);

            // 3. 校验位点：只允许单调向前
            long stored = readWatermark();
            if (stored < deliveredMax) {
                throw new OffsetStoreException("检测到位点回退（位点文件被改旧）：位点文件=" + stored
                        + "，但 delivered 日志中已确认最大序号=" + deliveredMax
                        + "；拒绝从旧位点继续，避免重复上报");
            }
            if (stored > maxSeq) {
                throw new OffsetStoreException("位点文件损坏/超前：位点=" + stored
                        + "，已分配最大序号=" + maxSeq);
            }
            watermark = stored;
        } catch (OffsetStoreException ex) {
            throw ex;
        } catch (IOException ex) {
            throw new UncheckedIOException("缓冲恢复失败: " + ex.getMessage(), ex);
        }
    }

    // ============================ 编码 / IO ============================

    private String encodePending(long seq, UnifiedReport r) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("seq", seq);
        ObjectNode rn = objectMapper.createObjectNode();
        rn.put("deviceId", r.getDeviceId());
        rn.put("metric", r.getMetric());
        rn.put("value", r.getValue());
        rn.put("businessTime", r.getBusinessTime().toEpochMilli());
        rn.put("receivedAt", r.getReceivedAt().toEpochMilli());
        rn.put("sourceFormat", r.getSourceFormat());
        rn.put("dedupKey", r.getDedupKey());
        root.set("r", rn);
        return root.toString();
    }

    private UnifiedReport decodeReport(JsonNode n) {
        return new UnifiedReport(
                n.get("deviceId").asText(),
                n.get("metric").asText(),
                n.get("value").asDouble(),
                Instant.ofEpochMilli(n.get("businessTime").asLong()),
                Instant.ofEpochMilli(n.get("receivedAt").asLong()),
                n.get("sourceFormat").asText(),
                n.get("dedupKey").asText());
    }

    /** 顺序追加并 fsync，保证进程崩溃/断电时这一行要么完整存在要么不存在。 */
    private void appendDurable(Path file, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(
                file.toFile(), true)) {
            fos.write(bytes);
            fos.getFD().sync();
        }
    }

    private void writeWatermark(long value) throws IOException {
        Path tmp = spoolDir.resolve(WATERMARK_FILE + ".tmp");
        Files.writeString(tmp, Long.toString(value), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        try {
            Files.move(tmp, watermarkFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException ex) {
            Files.move(tmp, watermarkFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private long readWatermark() throws IOException {
        if (!Files.exists(watermarkFile) || Files.size(watermarkFile) == 0) {
            return 0L;
        }
        String s = Files.readString(watermarkFile, StandardCharsets.UTF_8).trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            throw new OffsetStoreException("位点文件内容损坏: " + s);
        }
    }

    /** 缓冲中的一条未决记录。 */
    public record BufferedEntry(long seq, UnifiedReport report) { }
}
