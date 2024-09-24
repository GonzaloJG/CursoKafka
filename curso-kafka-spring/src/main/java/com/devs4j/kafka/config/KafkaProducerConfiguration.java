package com.devs4j.kafka.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {

    private Map<String, Object> producerProperties() {
        Map<String, Object> props=new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public KafkaTemplate<String, String> createTemplate() {
        DefaultKafkaProducerFactory<String, String> producerFactory= new DefaultKafkaProducerFactory<>(producerProperties());
        producerFactory.addListener(new MicrometerProducerListener<>(meterRegistry()));
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry prometheusMeterRegistry=new PrometheusMeterRegistry (PrometheusConfig.DEFAULT);
        return prometheusMeterRegistry;
    }
}
