package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.reflections.Reflections;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Set;
import java.util.StringJoiner;

@ConditionalOnProperty(name = "transmitter.enabled", havingValue = "true")
public class TransporterApplicationContextInitializer implements ApplicationContextInitializer {


    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        AnnotationConfigApplicationContext tempContext = new AnnotationConfigApplicationContext(KafkaTransmitterConfig.class);
        ObjectMapper mapper = tempContext.getBean(ObjectMapper.class);
        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        KafkaSourceConfigHolder kafkaSourceConfigHolder = Binder.get(environment).bind("transmitter", KafkaSourceConfigHolder.class).get();

        String package2Scan = findBootClass(applicationContext);
        Reflections scanner = new Reflections(package2Scan);
        Set<Class<? extends Object>> allClasses = scanner.getTypesAnnotatedWith(Component.class);
        allClasses.addAll(scanner.getTypesAnnotatedWith(Service.class));
        allClasses.addAll(scanner.getTypesAnnotatedWith(Repository.class));
        allClasses.addAll(scanner.getTypesAnnotatedWith(Controller.class));
        allClasses.addAll(scanner.getTypesAnnotatedWith(Configuration.class));
        for (Class<?> clazz : allClasses) {
            for (Field declaredField : clazz.getDeclaredFields()) {
                declaredField.setAccessible(true);
                if (Transporter.class.isAssignableFrom(declaredField.getType()))  {
//                    Class<?>[] classes = TypeResolver.resolveRawArguments(Transporter.class, declaredField.getType());
                    ParameterizedType genericType = (ParameterizedType) declaredField.getGenericType();
                    Class<?> firstActualTypeArgument = (Class<?>) genericType.getActualTypeArguments()[0];
                    Class<?> secondActualTypeArgument = (Class<?>) genericType.getActualTypeArguments()[1];
                    KafkaSource<?, ?> kafkaSource = kafkaSourceConfigHolder.getEnabled() ? new KafkaSource<>(kafkaSourceConfigHolder, mapper, firstActualTypeArgument, secondActualTypeArgument) : new KafkaSource<>(false);
                    applicationContext.getBeanFactory().registerSingleton(declaredField.getName(), kafkaSource);
                }
            }
        }
        tempContext.close();
    }

    public String findBootClass(ConfigurableApplicationContext context) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        String[] split = stackTrace[stackTrace.length - 1].getClassName().split("\\.");
        StringJoiner root = new StringJoiner(".");
        for (int i = 0; i < split.length - 1; i++) {
            root.add(split[i]);
        }
        return root.toString();
    }
}
