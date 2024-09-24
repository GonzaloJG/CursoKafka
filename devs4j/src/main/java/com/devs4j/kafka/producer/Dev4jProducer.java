package com.devs4j.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Dev4jProducer {
    private static final Logger log = LoggerFactory.getLogger(Dev4jProducer.class);
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
        props.put("acks","all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "1");

        try (Producer<String, String> producer=new KafkaProducer<>(props)) {
            for(int i= 0; i< 100 ;i++) {
                /*producer.send(new ProducerRecord<>("devs4j-topic",String.valueOf(i),"devs4j-value")).get();*/
                producer.send(new ProducerRecord<>("devs4j-topic",(i%2==0)?"key-2.1":"key-3.1", String.valueOf(i)));
            }
            producer.flush();
        } /*catch (InterruptedException | ExecutionException e) {
            log.error("Message producer interrupted", e);
        }*/
        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);
    }
}
