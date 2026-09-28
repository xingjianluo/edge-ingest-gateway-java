package com.github.xingjianluo.edgeingestgatewayjava.relay;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.SpooledEvent;

/**
 * 上游上报客户端。断连时抛 UpstreamUnavailableException，由续传器稍后重试。
 * 上游按 idempotencyKey 去重，重复投递是安全的（at-least-once + 幂等）。
 */
public interface UpstreamClient {

    void publish(SpooledEvent event) throws UpstreamUnavailableException;

    boolean available();

    /** 测试/运维用：模拟断连与恢复。 */
    void setAvailable(boolean available);
}
