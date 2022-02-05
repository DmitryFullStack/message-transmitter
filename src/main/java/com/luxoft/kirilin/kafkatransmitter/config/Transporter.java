package com.luxoft.kirilin.kafkatransmitter.config;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public interface Transporter {

    <T, R> Transporter map(Function<? super T, ? extends R> func);

    <T> Transporter filter(UnaryOperator<T> func);

    <T> Collection<T> to(Transporter source);

}
