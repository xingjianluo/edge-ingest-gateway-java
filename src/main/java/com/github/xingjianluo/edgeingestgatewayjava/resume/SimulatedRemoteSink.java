package com.github.xingjianluo.edgeingestgatewayjava.resume;

import com.github.xingjianluo.edgeingestgatewayjava.model.UnifiedReport;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link RemoteSink} 的内存模拟实现：
 * <ul>
 *   <li>{@link #setAvailable(boolean)} 模拟断线/恢复；不可用时投递失败；</li>
 *   <li>用 dedupKey 集合保证幂等：同一批次重试或重复投递不会产生重复数据；</li>
 *   <li>记录按投递顺序的“已送达”序列，便于断言补发顺序与去重结果。</li>
 * </ul>
 */
public class SimulatedRemoteSink implements RemoteSink {

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final Set<String> deliveredKeys = new LinkedHashSet<>();
    private final List<UnifiedReport> deliveredInOrder = new ArrayList<>();

    @Override
    public synchronized boolean deliver(List<UnifiedReport> batch) {
        if (!available.get()) {
            return false;
        }
        for (UnifiedReport r : batch) {
            if (deliveredKeys.add(r.getDedupKey())) {
                deliveredInOrder.add(r);
            }
        }
        return true;
    }

    @Override
    public void setAvailable(boolean available) {
        this.available.set(available);
    }

    @Override
    public synchronized int deliveredUniqueCount() {
        return deliveredKeys.size();
    }

    /** 按送达顺序的快照（测试断言用）。 */
    public synchronized List<UnifiedReport> deliveredSnapshot() {
        return List.copyOf(deliveredInOrder);
    }
}
