package com.github.xingjianluo.edgeingestgatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关可调参数，前缀 edge.gateway。
 */
@ConfigurationProperties(prefix = "edge.gateway")
public class GatewayProperties {

    /** 本地缓冲目录。 */
    private String spoolDir = "build/spool";

    /** 缓冲容量（未送达条数上限）。 */
    private long capacity = 1000L;

    /** 单次补发批次大小。 */
    private int batchSize = 100;

    /** 补发轮询间隔（毫秒）。 */
    private long pumpIntervalMillis = 500L;

    public String getSpoolDir() { return spoolDir; }
    public void setSpoolDir(String spoolDir) { this.spoolDir = spoolDir; }

    public long getCapacity() { return capacity; }
    public void setCapacity(long capacity) { this.capacity = capacity; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getPumpIntervalMillis() { return pumpIntervalMillis; }
    public void setPumpIntervalMillis(long pumpIntervalMillis) { this.pumpIntervalMillis = pumpIntervalMillis; }
}
