package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.List;
import java.util.function.Consumer;

public interface Transporter<T> {
    TransmitterStreamBuilder<T> pipeline();

    void forEach(Consumer cons, List<Object> actions);

    Transporter<T> deserializeExHandler(Consumer<DeserializationException> handler);

    Transporter<T> mappingExHandler(Consumer<JsonMappingException> handler);
}
