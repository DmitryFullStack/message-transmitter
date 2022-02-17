package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
public class KafkaTransporter<T, V> implements Transporter<T, V>, ApplicationListener<ApplicationReadyEvent> {

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    private final AtomicBoolean closed;
    private boolean used = false;
    private String name;
    private ObjectMapper objectMapper;
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<?, V> kafkaProducer;
    private Class<T> sourceClass;
    private Class<?> sourceDeserializerClass;
    private Consumer<DeserializationException> deserializationExceptionHandler;
    private Consumer<JsonMappingException> jsonMappingExceptionHandler;
    private List<Consumer> actions = new ArrayList<>();
    private List<String> sourceTopics;

    public KafkaTransporter(String name, Map<String, Object> consumerProps, Map<String, Object> producerProps,
                            List<String> sourceTopics, ObjectMapper objectMapper,
                            Class<T> sourceClass, Class<? super V> targetClass) {
        this.name = name;
        this.closed = new AtomicBoolean(false);
        this.sourceClass = sourceClass;
        this.objectMapper = objectMapper;
        this.sourceTopics = sourceTopics;
        this.kafkaProducer = KafkaProducerFactory.getProducer(targetClass,
                producerProps, this.objectMapper);
        this.kafkaConsumer = KafkaConsumerFactory.getConsumer(consumerProps, sourceTopics);
        this.sourceDeserializerClass = !Objects.equals(consumerProps.get(VALUE_DESERIALIZER), StringDeserializer.class)
                ? (Class<?>) consumerProps.get(VALUE_DESERIALIZER) : null;
    }

    public KafkaTransporter(Boolean enabled) {
        this.closed = new AtomicBoolean(true);
    }

    @Override
    public TransmitterStreamBuilder<T> pipeline() {
        this.used = true;
        return new TransmitterStreamBuilder<>(this, new ArrayList<>());
    }

    @Override
    public void forEach(Consumer cons, List<Object> conveyor) {
        final Consumer pipelineConsumer = record -> {
            Object result = record;
            recordTransporter(cons, conveyor, result);
        };
        this.actions.add(pipelineConsumer);
    }

    Transporter<T, V> to(List<Object> conveyorActions, List<String> topics) {
        Consumer<V> sender = record -> {
            for (String topic : topics) {
                kafkaProducer.send(new ProducerRecord<>(topic, record));
            }
        };
        Consumer pipelineConsumer = record -> {
            Object result = record;
            recordTransporter(sender, conveyorActions, result);
        };
        this.actions.add(pipelineConsumer);
        return this;
    }

    @Override
    public Transporter<T, V> deserializeExHandler(Consumer<DeserializationException> handler) {
        this.deserializationExceptionHandler = handler;
        return this;
    }

    @Override
    public Transporter<T, V> mappingExHandler(Consumer<JsonMappingException> handler) {
        this.jsonMappingExceptionHandler = handler;
        return this;
    }

    private void recordTransporter(Consumer cons, List<Object> actions, Object finalizer) {
        for (Object pipelineAction : actions) {
            finalizer = pipe(finalizer, pipelineAction);
            if (isNull(finalizer)) {
                break;
            }
            if (Collection.class.isAssignableFrom(finalizer.getClass())) {
                for (Object elem : Collection.class.cast(finalizer)) {
                    recordTransporter(cons, actions.subList(actions.indexOf(pipelineAction) + 1, actions.size()), elem);
                }
                break;
            }
        }
        if (nonNull(finalizer)) {
            cons.accept(finalizer);
        }
    }

    @Nullable
    private Object pipe(Object result, Object pipelineAction) {
        if (pipelineAction instanceof Function) {
            return ((Function) pipelineAction).apply(result);
        }
        if (pipelineAction instanceof Predicate) {
            if (((Predicate) pipelineAction).test(result)) {
                return result;
            }
        }
        if (pipelineAction instanceof Consumer) {
            ((Consumer) pipelineAction).accept(result);
            return result;
        }
        return null;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if(!closed.get() && used){
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(() -> {
                log.info("Transmitter " + this.name + " start to listen for topics: " + String.join(", ", this.sourceTopics));
                try {
                    while (!closed.get()) {
                        if (nonNull(sourceDeserializerClass)){
                            Consumer<Consumer> forEach = StreamSupport.stream(kafkaConsumer.poll(Duration.ofSeconds(1L)).spliterator(), false)
                                    .map(ConsumerRecord::value)::forEach;
                            for (Consumer action : this.actions) {
                                forEach.accept(action);
                            }
                            continue;
                        }
                        withDefaultSerializerHandling();
                    }
                } catch (WakeupException e) {
                    if (!closed.get()) throw e;
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    kafkaConsumer.close();
                }
            });
        }
    }

    private void withDefaultSerializerHandling() {
        List<String> records = StreamSupport.stream(kafkaConsumer.poll(Duration.ofSeconds(1L)).spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
        List<Object> deserializedRecords = new ArrayList<>(records.size());
        for (String record : records) {
            try {
                deserializedRecords.add(objectMapper.readValue(record, sourceClass));
            }catch (JsonMappingException ex){
                if (nonNull(jsonMappingExceptionHandler)) {
                    jsonMappingExceptionHandler.accept(ex);
                }
                else {
                    log.error(ex.getMessage(), ex);
                }
            }catch (DeserializationException ex){
                if (nonNull(deserializationExceptionHandler)) {
                    deserializationExceptionHandler.accept(ex);
                }else {
                    log.error(ex.getMessage(), ex);
                }
            }catch (Throwable ex){
                log.error(ex.getMessage(), ex);
            }
        }
        if(!deserializedRecords.isEmpty()){
            Consumer<Consumer> forEach = deserializedRecords::forEach;
            for (Consumer action : this.actions) {
                forEach.accept(action);
            }
        }
    }
}
