package com.luxoft.kirilin.kafkatransmitter.config;

import lombok.Getter;
import lombok.SneakyThrows;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class TransmitterStream<T> {
    @Getter
    private final Consumer<Consumer<T>> action;

    public TransmitterStream(Consumer<Consumer<T>> action) {
        this.action = action;
    }

//    private Class<?> token;

    public <R> TransmitterStream<R> map(Function<T, R> mapper, Class<?> token) {
//        Consumer<Consumer<U>> consumerConsumer = consumer -> action.accept(e -> consumer.accept(mapper.apply(e)));
        return new TransmitterStream<>(consumer -> forEachInner(e -> consumer.accept(
                mapper.apply(e))));
    }

    public void forEachInner(Consumer<T> cons) {
        action.accept(cons);
    }

    @SneakyThrows
    public void forEach(Consumer<T> cons) {
            action.accept(cons);
    }

    public static <R>TransmitterStream<R> of(Iterable<R> elements) {
        // just redirect to Iterable::forEach
//        Consumer<Consumer> forEach = action1 -> elements.forEach(action1);
//        kafkaSource.setAction(forEach);
        return new TransmitterStream<>(elements::forEach);
    }

    @SafeVarargs
    public static <T> TransmitterStream<T> of(T... elements) {
        return of(Arrays.asList(elements));
    }

    public static TransmitterStream<Integer> range(int from, int to) {
        return new TransmitterStream<>(cons -> {
            for (int i = from; i < to; i++) cons.accept(i);
        });
    }

    public TransmitterStream<T> filter(Predicate<T> pred, Class<?> token) {
        return new TransmitterStream<>(cons -> forEachInner(e -> {;
            Object x = (Object)e;

            var n = token.cast(x);
            if (pred.test((T) n))
                cons.accept((T) n);
        }));
    }

//    public <U> TransmitterStream flatMap(Function<T, TransmitterStream<U>> mapper) {
//        return new TransmitterStream(cons -> forEachInner(e -> mapper.apply(e).forEachInner(cons)));
//    }
//
//    public TransmitterStream<T> peek(Consumer<T> action) {
//        return new TransmitterStream<>(cons -> forEachInner(e -> {
//            action.accept(e);
//            cons.accept(e);
//        }));
//    }
//
//    public TransmitterStream<T> skip(long n) {
//        return new TransmitterStream<>(cons -> {
//            long[] count = {0};
//            forEachInner(e -> {
//                if (++count[0] > n)
//                    cons.accept(e);
//            });
//        });
//    }
//
//    public T reduce(T identity, BinaryOperator<T> op) {
//        class Box {
//            T val = identity;
//        }
//        Box b = new Box();
//        forEachInner(e -> b.val = op.apply(b.val, e));
//        return b.val;
//    }
//
//    public Optional<T> reduce(BinaryOperator<T> op) {
//        class Box {
//            boolean isPresent;
//            T val;
//        }
//        Box b = new Box();
//        forEachInner(e -> {
//            if (b.isPresent) b.val = op.apply(b.val, e);
//            else {
//                b.val = e;
//                b.isPresent = true;
//            }
//        });
//        return b.isPresent ? Optional.empty() : Optional.of(b.val);
//    }
//
//    public long count() {
//        return map(e -> 1L).reduce(0L, Long::sum);
//    }
//
//    public Optional<T> maxBy(Comparator<T> cmp) {
//        return reduce(BinaryOperator.maxBy(cmp));
//    }
//
//    public Optional<T> minBy(Comparator<T> cmp) {
//        return reduce(BinaryOperator.minBy(cmp));
//    }
}
