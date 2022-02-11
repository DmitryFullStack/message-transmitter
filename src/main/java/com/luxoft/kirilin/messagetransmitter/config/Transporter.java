package com.luxoft.kirilin.messagetransmitter.config;

import java.util.List;
import java.util.function.Consumer;

public interface Transporter<T, V> {
    TransmitterStreamBuilder<T> pipeline();

    void forEach(Consumer cons, List<Object> actions);

    Transporter<T, V> to(List<Object> actions, List<String> topics);
}
