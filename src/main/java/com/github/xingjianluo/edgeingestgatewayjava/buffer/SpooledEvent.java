package com.github.xingjianluo.edgeingestgatewayjava.buffer;

/**
 * 落盘缓冲中的一条待上报记录。
 *
 * @param seq               单调递增的入库序号（与墙上时钟无关，时钟回拨不影响）
 * @param idempotencyKey    幂等键
 * @param deviceId          设备标识
 * @param eventTimeMillis   业务时间（补发排序依据）
 * @param ingestWallClock   入库墙上时钟（仅用于排查，不参与任何判定）
 * @param payloadJson       归一化事件的 JSON 快照
 */
public record SpooledEvent(
        long seq,
        String idempotencyKey,
        String deviceId,
        long eventTimeMillis,
        long ingestWallClock,
        String payloadJson
) {
}
