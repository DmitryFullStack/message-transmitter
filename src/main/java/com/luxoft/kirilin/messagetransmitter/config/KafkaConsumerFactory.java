package com.luxoft.kirilin.messagetransmitter.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class KafkaConsumerFactory {

    public static <V> KafkaConsumer getConsumer(
            Map<String, Object> props,
            List<String> topics){
        KafkaConsumer<?, V> kafkaConsumer;
//        if (nonNull(props.get(VALUE_DESERIALIZER)) && !props.get(VALUE_DESERIALIZER).equals(StringDeserializer.class)) {
            kafkaConsumer = new KafkaConsumer<>(props);
//        }
//        else {
//            JsonDeserializer<V> deserializer = new SkippedCorruptedRecordDeserializer<>(valueClass, objectMapper);
//            deserializer.setUseTypeHeaders(false);
//            deserializer.addTrustedPackages("*");
//            kafkaConsumer = new KafkaConsumer<>(props, keyDeserializer, new ErrorHandlingDeserializer<>(deserializer));
//        }
        kafkaConsumer.subscribe(topics);
        return kafkaConsumer;
    }
}
