package com.luxoft.kirilin.messagetransmitter.test;

import com.luxoft.kirilin.messagetransmitter.config.Transporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class TestService {

    @Autowired
    private Transporter<Person, Unit> first;
    @Autowired
    private Transporter<Person, Unit> transmitter;

    @EventListener(ContextRefreshedEvent.class)
    public void work(ContextRefreshedEvent event){
        first
                .mappingExHandler(ex -> System.out.println("MAPPING!!!" + ex.getMessage()))
                .deserializeExHandler(ex -> System.out.println("DESERIALIZED!!!" + ex.getMessage()))
                .pipeline()
                .filter(person -> person.getAge() > 18)
                .map(person -> new Unit(String.format("%s.%s%d", person.getLastName(),
                        person.getFirstName().toCharArray()[0], person.getAge()),
                        UUID.randomUUID()))
                .forEachAndThen(System.out::println)
                .sendTo("test_deal");

        first
                .pipeline()
                .filter(person -> person.getAge() > 18)
                .forEachAndThen(System.out::println)
                .sendTo("test_deal");
    }
}
