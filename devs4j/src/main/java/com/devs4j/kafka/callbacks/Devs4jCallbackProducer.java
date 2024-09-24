package com.devs4j.kafka.callbacks;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Devs4jCallbackProducer {
    private static final Logger log = LoggerFactory.getLogger(Devs4jCallbackProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
        props.put("acks","all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "1");

        try (Producer<String, String> producer=new KafkaProducer<>(props)) {
            for(int i= 0; i< 1000000 ;i++) {
                producer.send(new ProducerRecord<String, String>("devs4j-topic",String.valueOf(i),"devs4j-value"),
                       (recordMetadata, e) -> {
                           if (e != null) {
                               log.info("There was an error {}", e.getMessage());
                           }
                           log.info("Offset = {}, Partition = {}, Topic = {}", recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic());
                       });
            }
            producer.flush();
        }
        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);
    }
}
