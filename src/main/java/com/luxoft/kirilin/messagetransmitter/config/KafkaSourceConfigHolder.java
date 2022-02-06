package com.luxoft.kirilin.messagetransmitter.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "transmitter")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KafkaSourceConfigHolder {

    private Boolean enabled = false;

    private String sourceTopic = "";

    @NestedConfigurationProperty
    private KafkaProperties broker;
}
