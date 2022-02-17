package com.luxoft.kirilin.messagetransmitter.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Map;

public class KafkaConsumerFactory {

    public static <V> KafkaConsumer getConsumer(Map<String, Object> props, List<String> topics) {
        KafkaConsumer<?, V> kafkaConsumer;
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(topics);
        return kafkaConsumer;
    }
}
