package com.devs4j.kafka;

import com.devs4j.kafka.models.Devs4jTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
@EnableScheduling
public class CursoKafkaSpringMskApplication  { //implements CommandLineRunner

    private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringMskApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private RestHighLevelClient client;

    @KafkaListener(topics = "devs4j-transactions", groupId = "devs4jGroup", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord<String, String>> messages) throws JsonProcessingException {
        for (ConsumerRecord<String, String> message : messages) {
//           Devs4jTransaction transaction = mapper.readValue(message.value(), Devs4jTransaction.class);
            log.info("Partition = {}, Offset= {}, Key = {}, Message= {}", message.partition(), message.offset(), message.key(), message.value());
            IndexRequest indexRequest = buildIndexRequest(
                    String.format("%s-%s-%s", message.partition(), message.key(), message.offset()), message.value());
            client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    log.info("Successfully indexed {}", indexResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }

    }

    private IndexRequest buildIndexRequest(String key, String value) {
        IndexRequest request = new IndexRequest("devs4j-transactions");
        request.id(key);
        request.source(value, XContentType.JSON);
        return request;
    }

    @Scheduled(fixedRate = 15000)
    public void sendMessage() throws JsonProcessingException {
        Faker faker = new Faker();
        for (int i = 0; i < 10000000; i++) {
            Devs4jTransaction transaction = new Devs4jTransaction();
            transaction.setUsername(faker.name().username());
            transaction.setNombre(faker.name().firstName());
            transaction.setApellido(faker.name().lastName());
            transaction.setMonto(faker.number().randomDouble(4, 0, 20000000));
            kafkaTemplate.send("devs4j-transactions", transaction.getUsername(), mapper.writeValueAsString(transaction));
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(CursoKafkaSpringMskApplication.class, args);
    }

  /*  @Autowired
    @Override
    public void run(String... args) throws Exception {

        IndexRequest request= new IndexRequest("devs4j-transactions");
        request.id("44");
        request.source("""
                {
                \"nombre\":\"Sammie\",
                \"apellido\":\"Goldner\",
                \"username\":\"hugh.vonrueden\",
                \"monto\":9622235.2009
                }
                """, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        log.info("Response id = {}", response.getId());
    }*/
}
