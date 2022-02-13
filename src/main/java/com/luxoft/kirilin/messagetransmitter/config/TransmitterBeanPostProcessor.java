package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;


@RequiredArgsConstructor
public class TransmitterBeanPostProcessor implements BeanPostProcessor {

    private final ConfigurableApplicationContext context;
    private final ObjectMapper mapper;

    @SneakyThrows
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();

        KafkaSourceConfigHolder kafkaSourceConfigHolder = context.getBean(KafkaSourceConfigHolder.class);

        for (Field declaredField : beanClass.getDeclaredFields()) {
            declaredField.setAccessible(true);
            if (Transporter.class.isAssignableFrom(declaredField.getType())) {
//                    Class<?>[] classes = TypeResolver.resolveRawArguments(Transporter.class, declaredField.getType());
                ParameterizedType genericType = (ParameterizedType) declaredField.getGenericType();
                Class<?> firstActualTypeArgument = (Class<?>) genericType.getActualTypeArguments()[0];
                Class<?> secondActualTypeArgument = (Class<?>) genericType.getActualTypeArguments()[1];
                KafkaSource<?, ?> kafkaSource = kafkaSourceConfigHolder.getEnabled()
                        ? new KafkaSource<>(kafkaSourceConfigHolder, mapper, firstActualTypeArgument, secondActualTypeArgument)
                        : new KafkaSource<>(false);
                declaredField.set(bean, kafkaSource);
            }
        }


        return bean;
    }
}
