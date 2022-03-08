package com.luxoft.kirilin.messagetransmitter.config;

import net.jodah.typetools.TypeResolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;


public class TransmitterRecipeBuilder<T> {

    private final KafkaTransporter kafkaSource;
    private final List<Object> actions;
    private final Class<T> token;
    private boolean deliveryGuarantee;


    public TransmitterRecipeBuilder(KafkaTransporter kafkaSource, List<Object> actions,
                                    Class<T> token, boolean deliveryGuarantee) {
        this.kafkaSource = kafkaSource;
        this.actions = actions;
        this.token = token;
        this.deliveryGuarantee = deliveryGuarantee;
    }

    public void forEach(Consumer<T> cons) {
        kafkaSource.forEach(cons, actions);
    }


    public <U> TransmitterRecipeBuilder<U> map(Function<T, U> mapper) {
        actions.add(mapper);
        Class<U> aClass = (Class<U>) TypeResolver.resolveRawArguments(Function.class, mapper.getClass())[1];
        return new TransmitterRecipeBuilder<>(kafkaSource, actions, aClass, deliveryGuarantee);
    }

        public <U> TransmitterRecipeBuilder<U> flatMap(Function<? super T, ? extends Collection<? extends U>> mapper) {
        actions.add(mapper);
        Class<U> aClass = (Class<U>) TypeResolver.resolveRawArguments(Function.class, mapper.getClass())[1];
        return new TransmitterRecipeBuilder<>(kafkaSource, actions, aClass, deliveryGuarantee);
    }

    public TransmitterRecipeBuilder<T> forEachAndThen(Consumer<T> consumer) {
        actions.add(consumer);
        return new TransmitterRecipeBuilder<>(kafkaSource, actions, token, deliveryGuarantee);
    }

    public TransmitterRecipeBuilder<T> filter(Predicate<T> pred) {
        actions.add(pred);
        return new TransmitterRecipeBuilder<>(kafkaSource, actions, token, deliveryGuarantee);
    }

    public TransmitterRecipeBuilder<T> deliveryGuarantee() {
        deliveryGuarantee = true;
        return this;
    }

    public  Transporter sendTo(String ... topics) {
        if(deliveryGuarantee) kafkaSource.enableOutbox(token);
        return kafkaSource.to(actions, Arrays.asList(topics), deliveryGuarantee);
    }
}
