package com.luxoft.kirilin.messagetransmitter.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.Collections;
import java.util.List;

@ConfigurationProperties(prefix = "transmitter")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransmitterConfigHolder {

    private Boolean enabled = false;

    private List<String> sourceTopic = Collections.emptyList();;

    @NestedConfigurationProperty
    private KafkaProperties broker;

    private List<TransmitterDescriptor> routes;
}
