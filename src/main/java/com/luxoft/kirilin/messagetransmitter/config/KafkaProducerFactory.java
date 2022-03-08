package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class KafkaProducerFactory {

    public static <T> KafkaProducer<String, T> getProducer(Map<String, Object> props,
                                                ObjectMapper objectMapper) {
        return new KafkaProducer<>(props,
                new JsonSerializer<>(objectMapper), new JsonSerializer<T>(objectMapper));
    }
}
