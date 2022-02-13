package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.List;
import java.util.function.Consumer;

public interface Transporter<T, V> {
    TransmitterStreamBuilder<T> pipeline();

    void forEach(Consumer cons, List<Object> actions);

    Transporter<T, V> to(List<Object> actions, List<String> topics);

    Transporter<T, V> deserializeExHandler(Consumer<DeserializationException> handler);

    Transporter<T, V> mappingExHandler(Consumer<JsonMappingException> handler);
}
