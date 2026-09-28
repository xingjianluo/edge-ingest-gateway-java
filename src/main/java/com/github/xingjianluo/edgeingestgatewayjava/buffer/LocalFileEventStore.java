package com.github.xingjianluo.edgeingestgatewayjava.buffer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.RejectReason;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于本地文件的 {@link EventStore}。
 *
 * <p>存储布局（dataDir 下）：
 * <ul>
 *   <li>{@code spool.log}：JSONL，待补发事件全量（压缩后仅含 seq &gt; frontier）</li>
 *   <li>{@code dedup.idx}：JSONL，幂等键 -&gt; 入库序号（去重索引，不随补发删除）</li>
 *   <li>{@code checkpoint}：单行数字，已连续确认的最高序号 frontier</li>
 * </ul>
 *
 * <p>并发：所有结构性变更由一把可超时的 {@link ReentrantLock} 保护。锁等待超时按
 * {@link RejectReason#WRITE_CONGESTION} 拒绝（背压），避免无限阻塞采集主流程；
 * 缓冲满按 {@link RejectReason#BUFFER_FULL} 拒绝。
 *
 * <p>序号只来自内存计数器（启动时由落盘数据重建），与墙上时钟无关，时钟回拨不会错误覆盖。
 */
public class LocalFileEventStore implements EventStore {

    private final Path dataDir;
    private final Path spoolFile;
    private final Path dedupFile;
    private final Path checkpointFile;
    private final int capacity;
    private final long lockTimeoutMillis;
    private final ObjectMapper mapper;

    private final ReentrantLock lock = new ReentrantLock(true);

    /** 已分配的最大序号。 */
    private long lastSeq;
    /** 已连续确认的最高序号；位点只能前进。 */
    private long frontier;
    /** 内存中的待发记录（seq > frontier），容量上界 = capacity。 */
    private final TreeMap<Long, StoredRecord> pending = new TreeMap<>();
    /** 幂等键 -> 首次入库序号。 */
    private final TreeMap<String, Long> keyToSeq = new TreeMap<>();
    /** 自上次压缩以来被确认剔除的条数。 */
    private long evictedSinceCompaction;
    private static final long COMPACT_THRESHOLD = 256;

    private BufferedWriter spoolWriter;
    private BufferedWriter dedupWriter;

    public LocalFileEventStore(Path dataDir, int capacity, long lockTimeoutMillis,
                               ObjectMapper mapper) {
        this.dataDir = dataDir;
        this.spoolFile = dataDir.resolve("spool.log");
        this.dedupFile = dataDir.resolve("dedup.idx");
        this.checkpointFile = dataDir.resolve("checkpoint");
        this.capacity = capacity;
        this.lockTimeoutMillis = lockTimeoutMillis;
        this.mapper = mapper;
        recover();
        openWriters();
    }

    // ------------------------------------------------------------------ append

    @Override
    public AppendOutcome append(UnifiedEvent event) {
        boolean locked;
        try {
            locked = lock.tryLock(lockTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IngestException(RejectReason.WRITE_CONGESTION,
                    "interrupted while waiting for buffer lock");
        }
        if (!locked) {
            throw new IngestException(RejectReason.WRITE_CONGESTION,
                    "buffer lock busy after " + lockTimeoutMillis + "ms, backpressure reject");
        }
        try {
            Long existing = keyToSeq.get(event.idempotencyKey());
            if (existing != null) {
                // 重复上报：幂等返回原序号，不重复落盘、不会产生第二条数据
                return new AppendOutcome(true, existing);
            }
            int pendingCount = pending.size();
            if (pendingCount >= capacity) {
                throw new IngestException(RejectReason.BUFFER_FULL,
                        "buffer full: pending=" + pendingCount + ", capacity=" + capacity
                                + "; report rejected, device may retry after relay drains");
            }
            long seq = ++lastSeq;
            long wallClock = System.currentTimeMillis();
            StoredRecord record = new StoredRecord(seq, event.idempotencyKey(), event.deviceId(),
                    event.eventTimeMillis(), wallClock, writeValue(event));
            // 崩溃安全顺序：先 spool 落盘，再 dedup 落盘，最后才向调用方返回成功
            appendDurably(spoolWriter, spoolFile, writeValue(record));
            keyToSeq.put(event.idempotencyKey(), seq);
            appendDurably(dedupWriter, dedupFile,
                    writeValue(new DedupEntry(event.idempotencyKey(), seq)));
            pending.put(seq, record);
            return new AppendOutcome(false, seq);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------- read/relay

    @Override
    public List<SpooledEvent> loadPending() {
        lock.lock();
        try {
            List<SpooledEvent> result = new ArrayList<>(pending.size());
            for (StoredRecord r : pending.values()) {
                if (r.seq > frontier) {
                    result.add(r.toSpooled());
                }
            }
            // 恢复后按业务时间顺序补发；业务时间相同则按入库序号保证全序
            result.sort(Comparator.comparingLong(SpooledEvent::eventTimeMillis)
                    .thenComparingLong(SpooledEvent::seq));
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void advanceCheckpoint(long ackedSeq) {
        lock.lock();
        try {
            if (ackedSeq <= frontier) {
                // 位点回退必须被明确拒绝，绝不允许从历史位置重新补发造成重复
                throw new CheckpointRollbackException(frontier, ackedSeq);
            }
            if (ackedSeq > lastSeq) {
                throw new IllegalArgumentException(
                        "checkpoint ackedSeq " + ackedSeq + " is ahead of lastSeq " + lastSeq);
            }
            frontier = ackedSeq;
            persistCheckpoint();
            long evicted = 0;
            while (!pending.isEmpty() && pending.firstKey() <= frontier) {
                pending.pollFirstEntry();
                evicted++;
            }
            evictedSinceCompaction += evicted;
            if (evictedSinceCompaction >= COMPACT_THRESHOLD) {
                compact();
                evictedSinceCompaction = 0;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long frontier() {
        lock.lock();
        try {
            return frontier;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int pendingCount() {
        lock.lock();
        try {
            return pending.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    // -------------------------------------------------------------- recovery

    private void recover() {
        try {
            Files.createDirectories(dataDir);
            frontier = readLongFile(checkpointFile, 0L);

            TreeMap<Long, StoredRecord> recovered = new TreeMap<>();
            long maxSeq = 0;
            if (Files.exists(spoolFile)) {
                for (String line : Files.readAllLines(spoolFile, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    StoredRecord r = mapper.readValue(line, StoredRecord.class);
                    recovered.put(r.seq, r);
                    maxSeq = Math.max(maxSeq, r.seq);
                }
            }

            if (Files.exists(dedupFile)) {
                for (String line : Files.readAllLines(dedupFile, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    DedupEntry e = mapper.readValue(line, DedupEntry.class);
                    // 悬空条目（spool 中已不存在）说明此前崩溃在两次落盘之间，丢弃以免误判重复
                    if (recovered.containsKey(e.seq)) {
                        keyToSeq.put(e.key, e.seq);
                    }
                }
            }
            // 崩溃在 spool 之后、dedup 之前：以 spool 为准重建去重索引
            for (StoredRecord r : recovered.values()) {
                keyToSeq.putIfAbsent(r.key, r.seq);
            }
            for (Map.Entry<Long, StoredRecord> e : recovered.entrySet()) {
                if (e.getKey() > frontier) {
                    pending.put(e.getKey(), e.getValue());
                }
            }
            lastSeq = maxSeq;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to recover local event store at " + dataDir, e);
        }
    }

    private void openWriters() {
        try {
            spoolWriter = Files.newBufferedWriter(spoolFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            dedupWriter = Files.newBufferedWriter(dedupFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open spool writers at " + dataDir, e);
        }
    }

    private void appendDurably(BufferedWriter writer, Path file, String jsonLine) {
        try {
            writer.write(jsonLine);
            writer.newLine();
            writer.flush();
            // force(true)：连元数据一起 fsync，进程崩溃/断电后仍可恢复
            force(file);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to " + file, e);
        }
    }

    private void force(Path file) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("fsync failed: " + file, e);
        }
    }

    private void compact() {
        // 调用方已持锁。重写 spool 仅保留待发；dedup 不去键（去重窗口覆盖整个本地生命周期）
        try {
            closeWriters();
            rewriteJsonl(spoolFile, pending.values(), StoredRecord.class);
            List<DedupEntry> dedupEntries = new ArrayList<>(keyToSeq.size());
            keyToSeq.forEach((k, seq) -> dedupEntries.add(new DedupEntry(k, seq)));
            rewriteJsonl(dedupFile, dedupEntries, DedupEntry.class);
            openWriters();
        } catch (IOException e) {
            throw new UncheckedIOException("compaction failed at " + dataDir, e);
        }
    }

    private <T> void rewriteJsonl(Path target, Iterable<T> rows, Class<T> type) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (T row : rows) {
                w.write(mapper.writeValueAsString(row));
                w.newLine();
            }
            w.flush();
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void persistCheckpoint() {
        try {
            Path tmp = checkpointFile.resolveSibling("checkpoint.tmp");
            Files.writeString(tmp, Long.toString(frontier), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, checkpointFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist checkpoint", e);
        }
    }

    private long readLongFile(Path file, long defaultValue) throws IOException {
        if (!Files.exists(file)) {
            return defaultValue;
        }
        String text = Files.readString(file, StandardCharsets.UTF_8).trim();
        return text.isEmpty() ? defaultValue : Long.parseLong(text);
    }

    private String writeValue(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new UncheckedIOException("serialization failed", e);
        }
    }

    private void closeWriters() throws IOException {
        IOException error = null;
        try {
            if (spoolWriter != null) {
                spoolWriter.close();
            }
        } catch (IOException e) {
            error = e;
        }
        try {
            if (dedupWriter != null) {
                dedupWriter.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            }
        }
        if (error != null) {
            throw error;
        }
    }

    // ------------------------------------------------------------- persisted DTO

    record StoredRecord(long seq, String key, String deviceId, long eventTime,
                        @JsonProperty("wallClock") long ingestWallClock, String payload) {

        SpooledEvent toSpooled() {
            return new SpooledEvent(seq, key, deviceId, eventTime, ingestWallClock, payload);
        }
    }

    record DedupEntry(String key, long seq) {
    }
}
