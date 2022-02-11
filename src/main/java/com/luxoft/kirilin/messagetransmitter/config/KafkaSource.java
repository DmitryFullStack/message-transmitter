package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
public class KafkaSource<T, V> implements Transporter<T, V> {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaSourceConfigHolder sourceConfigHolder;
    private final ObjectMapper objectMapper;
    private final KafkaConsumer<?, T> kafkaConsumer;
    private final KafkaProducer<?, V> kafkaProducer;

    public KafkaSource(KafkaSourceConfigHolder sourceConfigHolder, ObjectMapper objectMapper,
                       Class<? super T> expectedClass, Class<? super V> targetClass) {
        this.sourceConfigHolder = sourceConfigHolder;
        this.objectMapper = objectMapper;
        this.kafkaProducer = KafkaProducerFactory.getProducer(targetClass,
                this.sourceConfigHolder.getBroker().buildProducerProperties(),
                this.objectMapper);
        this.kafkaConsumer = KafkaConsumerFactory.getConsumer(expectedClass,
                this.sourceConfigHolder.getBroker().buildConsumerProperties(),
                sourceConfigHolder.getSourceTopic(), this.objectMapper);
    }

    @Override
    public TransmitterStreamBuilder<T> pipeline() {
        return new TransmitterStreamBuilder<>(this, new ArrayList<>());
    }

    @Override
    public void forEach(Consumer cons, List<Object> actions) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            log.info("Kafka consumer starting...");
            final Consumer pipelineConsumer = record -> {
                Object result = record;
                recordTransporter(cons, actions, result);
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
        });
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
