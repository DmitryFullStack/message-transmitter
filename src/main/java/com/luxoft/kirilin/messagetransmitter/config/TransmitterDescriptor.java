package com.luxoft.kirilin.messagetransmitter.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.Collections;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransmitterDescriptor {

    private String name;
    private List<String> sourceTopic = Collections.emptyList();

    @NestedConfigurationProperty
    private final KafkaProperties.Consumer consumer = new KafkaProperties.Consumer();

    @NestedConfigurationProperty
    private final KafkaProperties.Producer producer = new KafkaProperties.Producer();

}
