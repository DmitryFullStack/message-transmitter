package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaSourceConfigHolder.class)
public class KafkaTransmitterConfig {

    @Autowired
    private KafkaSourceConfigHolder sourceConfigHolder;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, true);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    @ConditionalOnProperty(name = "transmitter.broker.consumer.bootstrap-servers")
    public KafkaSource<String, Person> kafkaSource() {
        return new KafkaSource<>(sourceConfigHolder, objectMapper(),
                String.class, Person.class);
    }

//    @Bean
//    public <K, V> KafkaSource<K, V> kafkaSource(){
//        var kafkaConsumer = new KafkaConsumer<>(
//                sourceConfigHolder.getBroker().buildConsumerProperties(),
//                new StringDeserializer(),
//                new StringDeserializer());
//        kafkaConsumer.subscribe(List.of(sourceConfigHolder.getSourceTopic()));
//        return new KafkaSource(kafkaConsumer);
//    }

}
