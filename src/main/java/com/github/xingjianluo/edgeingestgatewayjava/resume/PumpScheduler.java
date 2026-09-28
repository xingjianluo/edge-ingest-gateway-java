package com.github.xingjianluo.edgeingestgatewayjava.resume;

import com.github.xingjianluo.edgeingestgatewayjava.config.GatewayProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 定时补发调度器：周期性触发一轮 pump。
 * 断线期间每轮读到数据但投递失败，数据留在本地；链路恢复后自动补发，无需人工干预。
 */
@Component
public class PumpScheduler {

    private final UploadCoordinator coordinator;

    public PumpScheduler(UploadCoordinator coordinator, GatewayProperties props) {
        this.coordinator = coordinator;
    }

    @Scheduled(fixedDelayString = "${edge.gateway.pump-interval-millis:500}")
    public void tick() {
        coordinator.pumpOnce();
    }
}
