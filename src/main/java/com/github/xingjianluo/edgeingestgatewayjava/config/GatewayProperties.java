package com.github.xingjianluo.edgeingestgatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关配置：edge.gateway.*
 */
@ConfigurationProperties(prefix = "edge.gateway")
public class GatewayProperties {

    /** 本地缓冲目录。 */
    private String dataDir = "data/edge-gateway";
    /** 缓冲容量（条数）。 */
    private int bufferCapacity = 10_000;
    /** 追加写获取锁的最长等待毫秒，超时按背压拒绝。 */
    private long writeLockTimeoutMillis = 50;
    /** 允许的最大未来时间偏移（毫秒），超出视为非法时间戳。 */
    private long maxFutureSkewMillis = 300_000;
    /** 续传轮询间隔毫秒。 */
    private long relayIntervalMillis = 100;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getBufferCapacity() { return bufferCapacity; }
    public void setBufferCapacity(int bufferCapacity) { this.bufferCapacity = bufferCapacity; }
    public long getWriteLockTimeoutMillis() { return writeLockTimeoutMillis; }
    public void setWriteLockTimeoutMillis(long writeLockTimeoutMillis) { this.writeLockTimeoutMillis = writeLockTimeoutMillis; }
    public long getMaxFutureSkewMillis() { return maxFutureSkewMillis; }
    public void setMaxFutureSkewMillis(long maxFutureSkewMillis) { this.maxFutureSkewMillis = maxFutureSkewMillis; }
    public long getRelayIntervalMillis() { return relayIntervalMillis; }
    public void setRelayIntervalMillis(long relayIntervalMillis) { this.relayIntervalMillis = relayIntervalMillis; }
}
