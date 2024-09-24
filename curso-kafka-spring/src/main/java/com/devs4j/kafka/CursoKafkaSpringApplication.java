package com.devs4j.kafka;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableScheduling
public class CursoKafkaSpringApplication { //implements CommandLineRunner

    private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private MeterRegistry meterRegistry;

    //autoStartup = "false" para decirle que no empiece directamente a leer mensajes
    @KafkaListener(id="devs4jId", autoStartup = "true", topics ="devs4j-topic", containerFactory = "listenerContainerFactory", groupId ="devs4j-group",
            properties = {"max.poll.interval.ms:4000",
                    "max.poll.records:10"})
    public void listen(List<ConsumerRecord<String, String>> messages) {
        log.info("Message received: {}", messages.size());
        log.info("Start reading messages");
        for (ConsumerRecord<String, String> message : messages) {
            log.info("Partition= {}, Offset {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(),message.value());
        }
        log.info("Batch complete");
    }

    public static void main(String[] args) {
        SpringApplication.run(CursoKafkaSpringApplication.class, args);
    }

    // Enviar mensajes de forma asincrona
    /*@Override
    public void run(String... args) throws Exception {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Hello World");
        future.addCallback(new KafkaSendCallback<String, String>(){

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Successfully sent message: {}", result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.info("Error sending message: {}", ex.getMessage());
            }

            @Override
            public void onFailure(KafkaProducerException e) {
                log.info("Error sending message: {}", e.getMessage());
            }
        });
    }*/

    // Enviar mensajes de forma sincrona
    /*@Override
    public void run(String... args) throws Exception {
        kafkaTemplate.send("devs4j-topic", "Hello World").get(100, TimeUnit.MILLISECONDS);
    }*/

    //Enviar 100 mensajes de forma asincrona
    /*@Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
        //Iniciar y detener un consumer
        log.info("Waiting for start");
        Thread.sleep(5000);
        log.info("Starting");
        registry.getListenerContainer("devs4jId").start();
        Thread.sleep(5000);
        registry.getListenerContainer("devs4jId").stop();

    }*/

    @Scheduled(fixedRate = 2000, initialDelay = 100)
    public void sendKafkaMessages(){
        for (int i = 0; i < 200; i++) {
            kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void printMetrics() {
        //Obtener listado de metricas
        List<Meter> metrics = meterRegistry.getMeters();
        for (Meter meter : metrics) {
            log.info("Meter = {}", meter.getId().getName());
        }
        //Leer una metrica en concreto, metrica de obtener paquetes totales enviados
        double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
        log.info("Count {} ",count);
    }
}
