package com.github.xingjianluo.edgeingestgatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedEvent;

/**
 * 单一上报格式 -> 统一模型的归一化器。
 * 只负责格式识别与字段校验；序列号由缓冲层在落盘时分配。
 */
public interface Normalizer {

    /** 是否能处理该原始报文。 */
    boolean supports(JsonNode root);

    /** 归一化（含字段级校验，非法时抛 IngestException）。 */
    UnifiedEvent normalize(JsonNode root);

    /** 格式名称（用于日志/测试输出）。 */
    String formatName();
}
