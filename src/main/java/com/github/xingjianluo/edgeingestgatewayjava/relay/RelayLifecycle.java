package com.github.xingjianluo.edgeingestgatewayjava.relay;

import com.github.xingjianluo.edgeingestgatewayjava.config.GatewayProperties;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 应用启动后（含重启恢复完成）自动开始后台续传，关闭时停止。
 */
@Component
public class RelayLifecycle {

    private final RelayService relayService;
    private final GatewayProperties properties;

    public RelayLifecycle(RelayService relayService, GatewayProperties properties) {
        this.relayService = relayService;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        relayService.start(properties.getRelayIntervalMillis());
    }

    @PreDestroy
    public void onStop() {
        relayService.stop();
    }
}
