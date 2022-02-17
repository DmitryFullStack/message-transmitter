package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class KafkaProducerFactory {

    public static <T> KafkaProducer getProducer(Class<T> valueClass, Map<String, Object> props,
                                                ObjectMapper objectMapper) {
        Serializer valueSerializer;
        if (valueClass.isAssignableFrom(String.class)) {
            valueSerializer = new StringSerializer();
        } else {
            valueSerializer = new JsonSerializer<T>(objectMapper);
        }
        KafkaProducer<String, T> kafkaProducer = new KafkaProducer<String, T>(props,
                new JsonSerializer<>(objectMapper), valueSerializer);
        return kafkaProducer;
    }
}
