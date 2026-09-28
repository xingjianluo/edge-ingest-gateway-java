package com.github.xingjianluo.edgeingestgatewayjava.config;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalSpoolBuffer;
import com.github.xingjianluo.edgeingestgatewayjava.clock.SystemGatewayClock;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ReportNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.resume.SimulatedRemoteSink;
import com.github.xingjianluo.edgeingestgatewayjava.resume.UploadCoordinator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 网关组件装配。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    @Bean
    public SystemGatewayClock gatewayClock() {
        return new SystemGatewayClock();
    }

    @Bean
    public ReportNormalizer reportNormalizer(SystemGatewayClock clock) {
        return new ReportNormalizer(clock);
    }

    @Bean(destroyMethod = "close")
    public LocalSpoolBuffer localSpoolBuffer(GatewayProperties props, SystemGatewayClock clock) {
        return new LocalSpoolBuffer(Path.of(props.getSpoolDir()), props.getCapacity(), clock);
    }

    @Bean
    public SimulatedRemoteSink remoteSink() {
        return new SimulatedRemoteSink();
    }

    @Bean
    public IngestService ingestService(ReportNormalizer normalizer, LocalSpoolBuffer buffer) {
        return new IngestService(normalizer, buffer);
    }

    @Bean
    public UploadCoordinator uploadCoordinator(LocalSpoolBuffer buffer,
                                               SimulatedRemoteSink sink,
                                               GatewayProperties props) {
        return new UploadCoordinator(buffer, sink, props.getBatchSize());
    }
}
