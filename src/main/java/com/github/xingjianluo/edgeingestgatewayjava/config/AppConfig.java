package com.github.xingjianluo.edgeingestgatewayjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.EventStore;
import com.github.xingjianluo.edgeingestgatewayjava.buffer.LocalFileEventStore;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.FlatJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.IngestService;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.ProbeV2Normalizer;
import com.github.xingjianluo.edgeingestgatewayjava.ingest.StandardJsonNormalizer;
import com.github.xingjianluo.edgeingestgatewayjava.relay.FakeUpstreamClient;
import com.github.xingjianluo.edgeingestgatewayjava.relay.RelayService;
import com.github.xingjianluo.edgeingestgatewayjava.relay.UpstreamClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/**
 * Spring Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class AppConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public List<Normalizer> normalizers() {
        // 顺序即格式匹配优先级
        return List.of(new StandardJsonNormalizer(), new FlatJsonNormalizer(), new ProbeV2Normalizer());
    }

    @Bean
    public EventStore eventStore(GatewayProperties props, ObjectMapper objectMapper) {
        return new LocalFileEventStore(Path.of(props.getDataDir()), props.getBufferCapacity(),
                props.getWriteLockTimeoutMillis(), objectMapper);
    }

    @Bean
    public UpstreamClient upstreamClient() {
        return new FakeUpstreamClient();
    }

    @Bean
    public RelayService relayService(EventStore store, UpstreamClient upstreamClient) {
        return new RelayService(store, upstreamClient);
    }    @Bean
    public IngestService ingestService(List<Normalizer> normalizers, EventStore eventStore,
                                       ObjectMapper objectMapper, GatewayProperties props) {
        return new IngestService(normalizers, eventStore, objectMapper,
                props.getMaxFutureSkewMillis());
    }
}
