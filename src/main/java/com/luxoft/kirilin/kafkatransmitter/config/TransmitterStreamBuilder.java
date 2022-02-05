package com.luxoft.kirilin.kafkatransmitter.config;

import lombok.SneakyThrows;
import net.jodah.typetools.TypeResolver;

import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.isNull;


public class TransmitterStreamBuilder<T> {

    private final KafkaSource kafkaSource;
    private final List<Object> actions;


    public TransmitterStreamBuilder(KafkaSource kafkaSource, List<Object> actions) {
        this.kafkaSource = kafkaSource;
        this.actions = actions;
    }

    @SneakyThrows
    public void forEach(Consumer<T> cons) {
        kafkaSource.process(cons, actions);
    }

    public <U> TransmitterStreamBuilder<U> map(Function<T, U> mapper) {
        actions.add(mapper);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }


    public TransmitterStreamBuilder<T> filter(Predicate<T> pred) {
        actions.add(pred);
        return new TransmitterStreamBuilder<>(kafkaSource, actions);
    }

}
