package com.devs4j.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Devs4jConsumer {

    private static final Logger log = LoggerFactory.getLogger(Devs4jConsumer.class);

    public static void main(String[] args) {
        Properties props=new Properties();

        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.setProperty("enable.auto.commit","false");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            /*TopicPartition topicPartition = new TopicPartition("devs4j-group", 4); //añadir al topic 4 particiones
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, 50);  //Leer a partir del offset 50*/
            while(true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records)
                    log.info("offset = {}, partition = {}, key = {}, value ={}", record.offset(), record.partition(),record.key(),record.value());
                    //consumer.commitAsync();
            }
        }
    }
}
