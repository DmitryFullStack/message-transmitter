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
    private Transporter<Person, Unit> transporter;

    @EventListener(ContextRefreshedEvent.class)
    public void work(ContextRefreshedEvent event){
        transporter
                .mappingExHandler(ex -> System.out.println("MAPPING!!!" + ex.getMessage()))
                .deserializeExHandler(ex -> System.out.println("DESERIALIZED!!!" + ex.getMessage()))
                .pipeline()
                .filter(person -> person.getAge() > 18)
                .map(person -> new Unit(String.format("%s.%s%d", person.getLastName(),
                        person.getFirstName().toCharArray()[0], person.getAge()),
                        UUID.randomUUID()))
                .forEachAndThen(System.out::println)
                .send("test_deal");
    }
}
