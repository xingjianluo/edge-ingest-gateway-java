package com.github.xingjianluo.edgeingestgatewayjava.buffer;

import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

import java.util.List;

/**
 * 本地持久化缓冲 + 去重索引 + 上报位点。
 */
public interface EventStore {

    /**
     * 追加一条上报；已存在相同幂等键时返回 duplicate=true。
     * 缓冲满返回 BUFFER_FULL，锁竞争超时返回 WRITE_CONGESTION。
     */
    AppendOutcome append(UnifiedEvent event);

    /** 按业务时间顺序返回所有尚未确认（seq > frontier）的事件。 */
    List<SpooledEvent> loadPending();

    /**
     * 上报位点前进：frontier 只能单调递增。
     * 回退（ackedSeq <= 当前 frontier）必须抛出异常明确拒绝。
     */
    void advanceCheckpoint(long ackedSeq);

    long frontier();

    int pendingCount();

    /** 缓冲容量（条数）。 */
    int capacity();

    record AppendOutcome(boolean duplicate, long seq) {
    }
}
