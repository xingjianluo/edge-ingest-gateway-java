package com.github.xingjianluo.edgeingestgatewayjava.model;

import java.util.Map;

/**
 * 归一化后的统一上报模型。
 *
 * @param idempotencyKey 幂等键（reportId 优先，否则业务指纹）
 * @param deviceId       设备标识
 * @param eventTimeMillis 业务时间（设备采样时间，epoch 毫秒）
 * @param reportId       设备侧报文号，可空
 * @param metrics        指标名 -> 数值
 * @param attributes     附加属性，可空
 */
public record UnifiedEvent(
        String idempotencyKey,
        String deviceId,
        long eventTimeMillis,
        String reportId,
        Map<String, Double> metrics,
        Map<String, String> attributes
) {
}
