package com.github.xingjianluo.edgeingestgatewayjava.resume;

import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

import java.util.List;

/**
 * 远端上报目标（模拟云端）。要求实现幂等：同一批次重复提交不得产生重复效果。
 */
public interface RemoteSink {

    /**
     * 投递一批数据。
     * @return 全部成功返回 true；失败返回 false（调用方稍后重试整批，幂等保证不重复）。
     */
    boolean deliver(List<UnifiedReport> batch);

    /** 模拟连通性，用于断线/恢复场景。 */
    void setAvailable(boolean available);

    /** 已被云端接收的去重键数量（测试断言用）。 */
    int deliveredUniqueCount();
}
