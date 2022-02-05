package com.luxoft.kirilin.kafkatransmitter.config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.function.Predicate;

@Data
@AllArgsConstructor
public class PredicateContainer<T> {
    private Predicate predicate;
    private Class<?> token;
}
