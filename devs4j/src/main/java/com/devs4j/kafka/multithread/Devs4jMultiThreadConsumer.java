package com.devs4j.kafka.multithread;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Devs4jMultiThreadConsumer {

    public static void main(String[] args) {
        Properties props=new Properties();

        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for (int i=0; i<5; i++) {
            Devs4jThreadConsumer consumer = new Devs4jThreadConsumer(new KafkaConsumer<>(props));
            executor.execute(consumer);
        }
        while(!executor.isTerminated());


    }
}
