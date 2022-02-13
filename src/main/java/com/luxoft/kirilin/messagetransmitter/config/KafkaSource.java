package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
public class KafkaSource<T, V> implements Transporter<T, V> {

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    private final AtomicBoolean closed;
    private KafkaSourceConfigHolder sourceConfigHolder;
    private ObjectMapper objectMapper;
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<?, V> kafkaProducer;
    private Class<T> sourceClass;
    private Class<?> sourceDeserializerClass;
    private Consumer<DeserializationException> deserializationExceptionHandler;
    private Consumer<JsonMappingException> jsonMappingExceptionHandler;

    public KafkaSource(KafkaSourceConfigHolder sourceConfigHolder, ObjectMapper objectMapper,
                       Class<T> sourceClass, Class<? super V> targetClass) {
        this.closed = new AtomicBoolean(false);
        this.sourceConfigHolder = sourceConfigHolder;
        this.sourceClass = sourceClass;
        this.objectMapper = objectMapper;
        this.kafkaProducer = KafkaProducerFactory.getProducer(targetClass,
                this.sourceConfigHolder.getBroker().buildProducerProperties(),
                this.objectMapper);
        this.kafkaConsumer = KafkaConsumerFactory.getConsumer(
                this.sourceConfigHolder.getBroker().buildConsumerProperties(),
                sourceConfigHolder.getSourceTopic());
        this.sourceDeserializerClass = !Objects.equals(sourceConfigHolder.getBroker().buildConsumerProperties().get(VALUE_DESERIALIZER), StringDeserializer.class)
                ? (Class<?>) sourceConfigHolder.getBroker().buildConsumerProperties().get(VALUE_DESERIALIZER) : null;
    }

    public KafkaSource(Boolean enabled) {
        this.closed = new AtomicBoolean(true);
    }

    @Override
    public TransmitterStreamBuilder<T> pipeline() {
        return new TransmitterStreamBuilder<>(this, new ArrayList<>());
    }

    @Override
    public void forEach(Consumer cons, List<Object> actions) {
        if(!closed.get()){
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(() -> {
                log.info("Kafka consumer starting...");
                final Consumer pipelineConsumer = record -> {
                    Object result = record;
                    recordTransporter(cons, actions, result);
                };
                try {
                    while (!closed.get()) {
                        if (nonNull(sourceDeserializerClass)){
                            StreamSupport.stream(kafkaConsumer.poll(Duration.ofSeconds(1L)).spliterator(), false)
                                    .map(ConsumerRecord::value)
                                    .forEach(pipelineConsumer);
                            continue;
                        }
                        withDefaultSerializerHandling(pipelineConsumer);
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

    private void withDefaultSerializerHandling(Consumer pipelineConsumer) {
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
            deserializedRecords.forEach(pipelineConsumer);
        }
    }

    @Override
    public Transporter<T, V> to(List<Object> actions, List<String> topics) {
        Consumer<V> sender = record -> {
            for (String topic : topics) {
                kafkaProducer.send(new ProducerRecord<>(topic, record));
            }
        };
        forEach(sender, actions);
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

    private void recordTransporter(Consumer cons, List<Object> actions, Object result) {
        for (Object pipelineAction : actions) {
            result = pipe(result, pipelineAction);
            if (isNull(result)) {
                break;
            }
            if (Collection.class.isAssignableFrom(result.getClass())) {
                for (Object elem : Collection.class.cast(result)) {
                    recordTransporter(cons, actions.subList(actions.indexOf(pipelineAction) + 1, actions.size()), elem);
                }
                break;
            }
        }
        if (nonNull(result)) {
            cons.accept(result);
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

}
