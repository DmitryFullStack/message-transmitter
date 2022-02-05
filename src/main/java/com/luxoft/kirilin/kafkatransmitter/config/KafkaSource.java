package com.luxoft.kirilin.kafkatransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
public class KafkaSource<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaSourceConfigHolder sourceConfigHolder;
    private final ObjectMapper objectMapper;
    private final KafkaConsumer<K, V> kafkaConsumer;

    public KafkaSource(KafkaSourceConfigHolder sourceConfigHolder, ObjectMapper objectMapper,
                       Class<? super K> keyClass, Class<? super V> valueClass) {
        this.sourceConfigHolder = sourceConfigHolder;
        this.objectMapper = objectMapper;
        this.kafkaConsumer = KafkaConsumerFactory.getConsumer(keyClass, valueClass,
                this.sourceConfigHolder.getBroker().buildConsumerProperties(),
                sourceConfigHolder.getSourceTopic(), this.objectMapper);
    }

    public TransmitterStreamBuilder<V> pipeline() {
        return new TransmitterStreamBuilder<>(this, new ArrayList<>());
    }

    @SneakyThrows
    public void process(Consumer cons, List<Object> actions) {
        log.info("Kafka consumer starting...");
        final Consumer pipelineConsumer = record -> {
            Object result = record;
            for (Object pipelineAction : actions) {
                result = pipe(result, pipelineAction);
                if (isNull(result)) {
                    break;
                }
            }
            if (nonNull(result)) {
                cons.accept(result);
            }
        };
        try {
            while (!closed.get()) {
                StreamSupport.stream(kafkaConsumer.poll(Duration.ofSeconds(1L)).spliterator(), false)
                        .map(ConsumerRecord::value)
                        .forEach(pipelineConsumer);
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            kafkaConsumer.close();
        }
    }

    @Nullable
    private Object pipe(Object result, Object pipelineAction) {
        if(pipelineAction instanceof Function){
            return ((Function) pipelineAction).apply(result);
        }
        if(pipelineAction instanceof Predicate){
            if (((Predicate) pipelineAction).test(result)) {
                return result;
            }
        }
        return null;
    }

}
