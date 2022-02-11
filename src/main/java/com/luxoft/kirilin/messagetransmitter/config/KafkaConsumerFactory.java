package com.luxoft.kirilin.messagetransmitter.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.Map;

public class KafkaConsumerFactory {

    public static <V> KafkaConsumer getConsumer(
            Class<V> valueClass,
            Map<String, Object> props,
            String topicName,
            ObjectMapper objectMapper){
        var keyDeserializer = new StringDeserializer();
        Deserializer valueDeserializer;
        if (valueClass.isAssignableFrom(String.class)) {
            valueDeserializer = new StringDeserializer();
        }
        else {
            JsonDeserializer<V> deserializer = new JsonDeserializer<>(valueClass, objectMapper);
            deserializer.setUseTypeHeaders(false);
            deserializer.addTrustedPackages("*");
            valueDeserializer = deserializer;
        }
        KafkaConsumer<String, V> kafkaConsumer = new KafkaConsumer<String, V>(props, keyDeserializer, valueDeserializer);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        return kafkaConsumer;
    }
}
