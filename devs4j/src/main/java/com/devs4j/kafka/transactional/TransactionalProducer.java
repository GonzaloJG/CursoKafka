package com.devs4j.kafka.transactional;

import com.devs4j.kafka.producer.Dev4jProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionalProducer {
    private static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("transactional.id", "devs4j-producer-id");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "1");

        try (Producer<String, String> producer=new KafkaProducer<>(props)) {
            try{
                producer.initTransactions();
                producer.beginTransaction();
                for(int i= 0; i< 100000 ;i++) {
                    producer.send(new ProducerRecord<>("devs4j-topic",String.valueOf(i),"devs4j-value"));
                    if( i == 50000)
                        throw new Exception("Unexpected exception");
                }
                producer.commitTransaction();
                producer.flush();
            } catch (Exception e) {
                log.error("Error ", e);
                producer.abortTransaction();
            }
        }
        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);
    }
}
