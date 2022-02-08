package com.luxoft.kirilin.messagetransmitter;

import com.luxoft.kirilin.messagetransmitter.config.Person;
import com.luxoft.kirilin.messagetransmitter.config.RecordSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@SpringBootApplication
public class MessageTransmitterApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MessageTransmitterApplication.class, args);
        RecordSource<Person> kafkaSource = context.getBean(RecordSource.class);

        kafkaSource.pipeline()
                .map(p -> p.getFirstName().length())
                .filter(x -> x > 1)
                .flatMap(integer -> List.of(1, 2, 3))
                .map(String::valueOf)
                .forEachAndThen(System.out::println)
                .forEach(System.out::println);
    }

}
