package com.github.xingjianluo.edgeingestgatewayjava.clock;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用手动时钟。{@link #setTimeMillis(long)} 可直接设置（包括回拨），
 * 用于验证时钟回拨场景下缓冲序号与覆盖行为是否正确。
 */
public class ManualGatewayClock implements GatewayClock {

    private final AtomicLong current = new AtomicLong(0L);

    public ManualGatewayClock() {
    }

    public ManualGatewayClock(long initialMillis) {
        this.current.set(initialMillis);
    }

    @Override
    public Instant now() {
        return Instant.ofEpochMilli(current.get());
    }

    @Override
    public long nowMillis() {
        return current.get();
    }

    public void setTimeMillis(long millis) {
        current.set(millis);
    }

    public void advanceMillis(long deltaMillis) {
        current.addAndGet(deltaMillis);
    }
}
