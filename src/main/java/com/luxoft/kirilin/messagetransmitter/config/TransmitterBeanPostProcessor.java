package com.luxoft.kirilin.messagetransmitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Optional;


@RequiredArgsConstructor
public class TransmitterBeanPostProcessor implements BeanPostProcessor {

    private final ConfigurableApplicationContext context;
    private final ObjectMapper mapper;

    @SneakyThrows
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();

        TransmitterConfigHolder kafkaSourceConfigHolder = context.getBean(TransmitterConfigHolder.class);

        for (Field declaredField : beanClass.getDeclaredFields()) {
            declaredField.setAccessible(true);
            if (Transporter.class.isAssignableFrom(declaredField.getType())) {
                ParameterizedType genericType = (ParameterizedType) declaredField.getGenericType();
                Class<?> firstActualTypeArgument = (Class<?>) genericType.getActualTypeArguments()[0];
                Optional<TransmitterDescriptor> transmitterDescriptor = kafkaSourceConfigHolder.getRoutes().stream().filter(desc -> desc.getName().equals(declaredField.getName()))
                        .findFirst();
                if(transmitterDescriptor.isPresent()){
                    KafkaTransporter<?> kafkaSource = kafkaSourceConfigHolder.getEnabled()
                            ? new KafkaTransporter<>(transmitterDescriptor.get().getName(), transmitterDescriptor.get().getConsumer().buildProperties(),
                            transmitterDescriptor.get().getProducer().buildProperties(),
                            transmitterDescriptor.get().getSourceTopic(),
                            mapper, firstActualTypeArgument, context)
                            : new KafkaTransporter<>(false);
                    declaredField.set(bean, kafkaSource);
                }else {
                    KafkaTransporter<?> kafkaSource = kafkaSourceConfigHolder.getEnabled()
                            ? new KafkaTransporter<>(declaredField.getName(), kafkaSourceConfigHolder.getBroker().buildConsumerProperties(),
                            kafkaSourceConfigHolder.getBroker().buildProducerProperties(),
                            kafkaSourceConfigHolder.getSourceTopic(), mapper,
                            firstActualTypeArgument, context)
                            : new KafkaTransporter<>(false);
                    declaredField.set(bean, kafkaSource);
                }
            }
        }
        return bean;
    }
}
