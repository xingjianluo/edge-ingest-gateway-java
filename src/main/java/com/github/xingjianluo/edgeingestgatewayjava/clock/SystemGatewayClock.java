package com.github.xingjianluo.edgeingestgatewayjava.clock;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于系统墙钟的实现，带单调保护：
 * 每次读数都与历史最大值比较，物理时钟发生回拨时沿用上一次的最大值，时间绝不倒退。
 */
public class SystemGatewayClock implements GatewayClock {

    private final AtomicLong lastMillis = new AtomicLong(Long.MIN_VALUE);

    @Override
    public long nowMillis() {
        long wall = System.currentTimeMillis();
        return lastMillis.updateAndGet(prev -> Math.max(prev, wall));
    }

    @Override
    public Instant now() {
        return Instant.ofEpochMilli(nowMillis());
    }
}
