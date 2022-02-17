package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

@ConditionalOnProperty(name = "transmitter.enabled", havingValue = "true")
public class TransporterApplicationContextInitializer implements ApplicationContextInitializer {

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        ObjectMapper mapper = objectMapper();
        TransmitterConfigHolder kafkaSourceConfigHolder = fetchTransmitterConfigHolder(applicationContext);
        Set<Class<?>> allClasses = getAllClassesFromPackage();
        for (Class<?> clazz : allClasses) {
            for (Field declaredField : clazz.getDeclaredFields()) {
                if (Transporter.class.isAssignableFrom(declaredField.getType()) && !declaredField.getGenericType().equals(declaredField.getType()))  {
                    Class<?>[] genericTypes = getGenericTypes(declaredField);
                    Optional<TransmitterDescriptor> transmitterDescriptor = kafkaSourceConfigHolder.getRoutes().stream().filter(desc -> desc.getName().equals(declaredField.getName()))
                            .findFirst();
                    if(transmitterDescriptor.isPresent()){
                        KafkaSource<?, ?> kafkaSource = kafkaSourceConfigHolder.getEnabled()
                                ? createNewKafkaSource(mapper, transmitterDescriptor.get().getConsumer(),
                                                        transmitterDescriptor.get().getProducer(),
                                                        transmitterDescriptor.get().getSourceTopic(),
                                                        genericTypes[0], genericTypes[1])
                                : new KafkaSource<>(false);
                        applicationContext.getBeanFactory().registerSingleton(declaredField.getName(), kafkaSource);
                    }else {
                        KafkaSource<?, ?> kafkaSource = kafkaSourceConfigHolder.getEnabled()
                                ? createNewKafkaSource(mapper, kafkaSourceConfigHolder.getBroker().getConsumer(),
                                                        kafkaSourceConfigHolder.getBroker().getProducer(),
                                                        kafkaSourceConfigHolder.getSourceTopic(),
                                                        genericTypes[0], genericTypes[1])
                                : new KafkaSource<>(false);
                        applicationContext.getBeanFactory().registerSingleton(declaredField.getName(), kafkaSource);
                    }
                }
            }
        }
    }

    private Class<?>[] getGenericTypes(Field declaredField){
        ParameterizedType genericType = (ParameterizedType) declaredField.getGenericType();
        Class<?>[] genericTypes = new Class<?>[2];
        genericTypes[0] = (Class<?>) genericType.getActualTypeArguments()[0];
        genericTypes[1] = (Class<?>) genericType.getActualTypeArguments()[1];
        return genericTypes;
    }

    private TransmitterConfigHolder fetchTransmitterConfigHolder(ConfigurableApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        return Binder.get(environment).bind("transmitter", TransmitterConfigHolder.class).get();
    }

    private Set<Class<?>> getAllClassesFromPackage() {
        Reflections scanner = new Reflections(findRootPackageForScan(), new SubTypesScanner(false));
        return scanner.getSubTypesOf(Object.class);
    }

    private KafkaSource<?, ?> createNewKafkaSource(ObjectMapper mapper, KafkaProperties.Consumer consumer,
                                                   KafkaProperties.Producer producer, List<String> sourceTopic,
                                                   Class<?> firstActualTypeArgument, Class<?> secondActualTypeArgument) {
        return new KafkaSource<>(consumer.buildProperties(), producer.buildProperties(),
                sourceTopic, mapper, firstActualTypeArgument, secondActualTypeArgument);
    }

    private String findRootPackageForScan() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        String[] split = stackTrace[stackTrace.length - 1].getClassName().split("\\.");
        StringJoiner root = new StringJoiner(".");
        for (int i = 0; i < split.length - 1; i++) {
            root.add(split[i]);
        }
        return root.toString();
    }

    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, true);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
