package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luxoft.kirilin.messagetransmitter.config.troutbox.GuaranteedDeliveryExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
public class KafkaTransporter<T> implements Transporter<T>, ApplicationListener<ApplicationReadyEvent> {

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    private final AtomicBoolean closed;
    private boolean used = false;
    private boolean deliveryGuarantee = false;
    private String name;
    private ObjectMapper objectMapper;
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<?, ?> kafkaProducer;
    private Class<T> sourceClass;
    private Set<Class<?>> serializingClasses = new HashSet<>();
    private Class<?> sourceDeserializerClass;
    private Consumer<DeserializationException> deserializationExceptionHandler;
    private Consumer<JsonMappingException> jsonMappingExceptionHandler;
    private List<Consumer> actions = new ArrayList<>();
    private List<String> sourceTopics;
    private List<String> targetTopics;
    private GuaranteedDeliveryExecutorService guaranteedDeliveryService;
    private ConfigurableApplicationContext context;

    public KafkaTransporter(String name, Map<String, Object> consumerProps, Map<String, Object> producerProps,
                            List<String> sourceTopics, ObjectMapper objectMapper,
                            Class<T> sourceClass, ConfigurableApplicationContext context) {
        this.name = name;
        this.context = context;
        this.closed = new AtomicBoolean(false);
        this.sourceClass = sourceClass;
        this.objectMapper = objectMapper;
        this.sourceTopics = sourceTopics;
        this.kafkaProducer = KafkaProducerFactory.getProducer(producerProps, this.objectMapper);
        this.kafkaConsumer = KafkaConsumerFactory.getConsumer(consumerProps, sourceTopics);
        this.sourceDeserializerClass = !Objects.equals(consumerProps.get(VALUE_DESERIALIZER), StringDeserializer.class)
                ? (Class<?>) consumerProps.get(VALUE_DESERIALIZER) : null;
    }

    public KafkaTransporter(Boolean enabled) {
        this.closed = new AtomicBoolean(true);
    }

    @Override
    public TransmitterRecipeBuilder<T> pipeline() {
        this.used = true;
        return new TransmitterRecipeBuilder<>(this, new ArrayList<>(), sourceClass, false);
    }

    @Override
    public void forEach(Consumer cons, List<Object> conveyor) {
        final Consumer pipelineConsumer = record -> {
            Object result = record;
            recordTransporter(cons, conveyor, result);
        };
        this.actions.add(pipelineConsumer);
    }

    void enableOutbox(Class<?> serialized){
        deliveryGuarantee = true;
        GuaranteedDeliveryExecutorService guaranteedDeliveryService = context.getBean(GuaranteedDeliveryExecutorService.class);
        guaranteedDeliveryService.setObjectMapper(objectMapper);
        this.guaranteedDeliveryService = guaranteedDeliveryService;
        serializingClasses.add(serialized);
    }

    Transporter<T> to(List<Object> conveyorActions, List<String> topics, boolean deliveryGuarantee) {
        targetTopics = topics;
        Consumer sender = deliveryGuarantee
                ? record -> guaranteedDeliveryService.addForGuaranteedDelivery(record, name)
                : record -> sending(record, topics.toArray(String[]::new));

        Consumer pipelineConsumer = record -> {
            recordTransporter(sender, conveyorActions, record);
        };
        this.actions.add(pipelineConsumer);
        return this;
    }

    @SneakyThrows
    protected void sending(Object record, String [] topicNames) {
        for (String topicName : topicNames) {
            try {
                kafkaProducer.send(new ProducerRecord(topicName, record))
                        .get(5, TimeUnit.SECONDS);
            }catch (ExecutionException exception){
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public Transporter<T> deserializeExHandler(Consumer<DeserializationException> handler) {
        this.deserializationExceptionHandler = handler;
        return this;
    }

    @Override
    public Transporter<T> mappingExHandler(Consumer<JsonMappingException> handler) {
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
        if(deliveryGuarantee){
            txnoInitialize();
        }
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

    public void txnoInitialize() {
        GuaranteedDeliveryExecutorService executor = this.context.getBean(GuaranteedDeliveryExecutorService.class);
        executor.addOutbox(context, name,
                record -> {
                    for (String topicName : this.targetTopics) {
                        try {
                            kafkaProducer.send(new ProducerRecord(topicName, record))
                                    .get(5, TimeUnit.SECONDS);
                        }catch (ExecutionException | InterruptedException | TimeoutException exception){
                            throw new RuntimeException(exception);
                        }
                    }
                });
    }

}
