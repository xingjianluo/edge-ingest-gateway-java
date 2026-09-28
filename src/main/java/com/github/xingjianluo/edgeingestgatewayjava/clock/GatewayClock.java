package com.github.xingjianluo.edgeingestgatewayjava.clock;

import java.time.Instant;

/**
 * 可注入的时钟，便于测试控制时间。
 */
public interface GatewayClock {
    Instant now();
    long nowMillis();
}
