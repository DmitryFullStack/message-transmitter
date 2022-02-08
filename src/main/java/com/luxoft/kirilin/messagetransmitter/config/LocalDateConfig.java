package com.luxoft.kirilin.messagetransmitter.config;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Component
public class LocalDateConfig implements Converter<String, LocalDateTime> {

    private static final String SUPPORTED_FORMAT = "yyyy-MM-dd";

    @Override
    public LocalDateTime convert(String s) {

            try {
                return LocalDate.parse(s, DateTimeFormatter.ISO_DATE).atStartOfDay();
            } catch (DateTimeParseException ex) {
                // deliberate empty block so that all parsers run
            }

        throw new DateTimeException(String.format("unable to parse (%s) supported formats are %s",
                s, SUPPORTED_FORMAT));
    }
}
