package com.luxoft.kirilin.messagetransmitter.config;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


public class TransmitterStreamBuilder<T> {

    private final KafkaSource kafkaSource;
    private final List<Object> actions;


    public TransmitterStreamBuilder(KafkaSource kafkaSource, List<Object> actions) {
        this.kafkaSource = kafkaSource;
        this.actions = actions;
    }

    public void forEach(Consumer<T> cons) {
        kafkaSource.process(cons, actions);
    }


    public <U> TransmitterStreamBuilder<U> map(Function<T, U> mapper) {
        actions.add(mapper);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }

        public <U> TransmitterStreamBuilder<U> flatMap(Function<? super T, ? extends Collection<? extends U>> mapper) {
        actions.add(mapper);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }

    public TransmitterStreamBuilder<T> forEachAndThen(Consumer<T> consumer) {
        actions.add(consumer);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }

    public TransmitterStreamBuilder<T> filter(Predicate<T> pred) {
        actions.add(pred);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }

    public void to(Consumer<T> cons) {
        kafkaSource.process(cons, actions);
    }
}
