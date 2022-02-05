package com.luxoft.kirilin.kafkatransmitter.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.Map;

public class KafkaConsumerFactory {

    public static <K, V> KafkaConsumer getConsumer(
            Class<K> keyClass,
            Class<V> valueClass,
            Map<String, Object> props,
            String topicName,
            ObjectMapper objectMapper){
        var keyDeserializer = keyClass.isAssignableFrom(String.class) ? new StringDeserializer() : new JsonDeserializer();
        Deserializer valueDeserializer;
        if (valueClass.isAssignableFrom(String.class)) {
            valueDeserializer = new StringDeserializer();
        }
        else {
            JsonDeserializer deserializer = new JsonDeserializer<V>(valueClass, objectMapper);
            deserializer.setUseTypeHeaders(false);
            deserializer.addTrustedPackages("*");
            valueDeserializer = deserializer;
        }
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<K, V>(props, keyDeserializer, valueDeserializer);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        return kafkaConsumer;
    }
}
