package com.luxoft.kirilin.messagetransmitter.config;

import java.util.List;
import java.util.function.Consumer;

public interface RecordSource<V> {
    TransmitterStreamBuilder<V> pipeline();

    void process(Consumer cons, List<Object> actions);
}
