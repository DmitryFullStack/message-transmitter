package com.luxoft.kirilin.kafkatransmitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.luxoft.kirilin.kafkatransmitter.config.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;

@SpringBootApplication
public class KafkaTransmitterApplication {

    @Autowired
    private KafkaSourceConfigHolder sourceConfigHolder;
    @Autowired
    private ObjectMapper objectMapper;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaTransmitterApplication.class, args);
        KafkaSource<String, Person> kafkaSource = context.getBean(KafkaSource.class);

        kafkaSource.pipeline()
                .map(p -> p.getFirstName().length())
                .filter(x -> x > 1)
                .forEach(System.out::println);
    }

    @Bean
    public KafkaSource<String, Person> kafkaSource() {
        return new KafkaSource<>(sourceConfigHolder, objectMapper,
                String.class, Person.class);
    }

}
