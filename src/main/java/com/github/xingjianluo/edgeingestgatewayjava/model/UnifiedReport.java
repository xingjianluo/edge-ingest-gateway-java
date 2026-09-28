package com.github.xingjianluo.edgeingestgatewayjava.model;

import java.time.Instant;

/**
 * 归一化后的统一上报模型。所有接入格式（json-v1 / json-v2 / line）最终都转换为本对象。
 */
public final class UnifiedReport {

    private final String deviceId;
    private final String metric;
    private final double value;
    private final Instant businessTime;
    private final Instant receivedAt;
    private final String sourceFormat;
    private final String dedupKey;

    public UnifiedReport(String deviceId, String metric, double value,
                         Instant businessTime, Instant receivedAt,
                         String sourceFormat, String dedupKey) {
        this.deviceId = deviceId;
        this.metric = metric;
        this.value = value;
        this.businessTime = businessTime;
        this.receivedAt = receivedAt;
        this.sourceFormat = sourceFormat;
        this.dedupKey = dedupKey;
    }

    public String getDeviceId() { return deviceId; }
    public String getMetric() { return metric; }
    public double getValue() { return value; }
    public Instant getBusinessTime() { return businessTime; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getSourceFormat() { return sourceFormat; }
    public String getDedupKey() { return dedupKey; }
}
