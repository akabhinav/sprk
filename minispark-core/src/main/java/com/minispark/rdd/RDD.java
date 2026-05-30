package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.TaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Resilient Distributed Dataset: an immutable, partitioned collection of
 * elements built up by chaining transformations.
 *
 * <p>Three things every concrete RDD must define:
 * <ol>
 *   <li>{@link #getPartitions()} — how the dataset is sliced.</li>
 *   <li>{@link #compute(Partition, TaskContext)} — how to materialize one slice.</li>
 *   <li>{@link #getDependencies()} — what parent RDD(s) feed this one.</li>
 * </ol>
 *
 * <p><b>Why transformations are lazy:</b> calling {@code map} returns a new
 * RDD object that holds the function; nothing runs. The DAG only executes
 * when an <i>action</i> (collect, count, reduce, ...) demands a value. This
 * laziness is what lets the scheduler see the whole pipeline at once, fuse
 * narrow operations into a single stage, and recompute lost partitions from
 * lineage instead of replicating state.
 *
 * Real Spark equivalent: org.apache.spark.rdd.RDD
 */
public abstract class RDD<T> implements Serializable {

    private static final AtomicInteger ID_GEN = new AtomicInteger();

    private final int id = ID_GEN.incrementAndGet();
    // Transient: the context is a driver-side object and must not ship to executors.
    private final transient MiniSparkContext sc;

    protected RDD(MiniSparkContext sc) {
        this.sc = sc;
    }

    public final int id() { return id; }
    public final MiniSparkContext context() { return sc; }

    public abstract List<Partition> getPartitions();
    public abstract Iterator<T> compute(Partition split, TaskContext ctx);
    public abstract List<Dependency<?>> getDependencies();

    /**
     * If non-null, hints the {@link com.minispark.shuffle.Partitioner} this RDD is
     * already partitioned by — e.g. a {@code ShuffledRDD}. Used by Phase 2's
     * scheduler to avoid redundant shuffles.
     */
    public com.minispark.shuffle.Partitioner partitioner() { return null; }

    // ----- transformations (lazy) -----

    public <U> RDD<U> map(SerializableFunction<T, U> f) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new TransformingIterator<>(it, f));
    }

    public RDD<T> filter(SerializablePredicate<T> p) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new FilteringIterator<>(it, p));
    }

    public <U> RDD<U> flatMap(SerializableFunction<T, Iterator<U>> f) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new FlatteningIterator<>(it, f));
    }

    /**
     * Pair-RDD entry point. {@link com.minispark.api.PairRDDFunctions} adds
     * key/value operations like {@code reduceByKey}, {@code groupByKey},
     * {@code join} — each of which introduces a shuffle.
     */
    public <K, V> com.minispark.api.PairRDDFunctions<K, V> mapToPair(
            SerializableFunction<T, com.minispark.api.Tuple2<K, V>> toPair) {
        RDD<com.minispark.api.Tuple2<K, V>> mapped = map(toPair);
        return new com.minispark.api.PairRDDFunctions<>(mapped);
    }

    // ----- actions (eager) -----

    public List<T> collect() {
        return sc.runJob(this, (ctx, it) -> {
            List<T> out = new ArrayList<>();
            it.forEachRemaining(out::add);
            return out;
        }).stream().flatMap(List::stream).toList();
    }

    public long count() {
        return sc.runJob(this, (ctx, it) -> {
            long n = 0;
            while (it.hasNext()) { it.next(); n++; }
            return n;
        }).stream().mapToLong(Long::longValue).sum();
    }

    public T reduce(SerializableBiFunction<T, T, T> op) {
        List<T> partials = sc.runJob(this, (ctx, it) -> {
            if (!it.hasNext()) return null;
            T acc = it.next();
            while (it.hasNext()) acc = op.apply(acc, it.next());
            return acc;
        });
        T result = null;
        for (T p : partials) {
            if (p == null) continue;
            result = (result == null) ? p : op.apply(result, p);
        }
        if (result == null) throw new NoSuchElementException("reduce on empty RDD");
        return result;
    }

    public void foreach(SerializableConsumer<T> f) {
        sc.runJob(this, (ctx, it) -> { it.forEachRemaining(f::accept); return null; });
    }

    // ----- serializable functional interfaces -----
    // Closures cross the SchedulerBackend seam, so they must be Serializable from day one.

    @FunctionalInterface public interface SerializableFunction<A, B> extends Function<A, B>, Serializable {}
    @FunctionalInterface public interface SerializableBiFunction<A, B, C> extends BiFunction<A, B, C>, Serializable {}
    @FunctionalInterface public interface SerializablePredicate<A> extends Predicate<A>, Serializable {}
    @FunctionalInterface public interface SerializableConsumer<A> extends java.util.function.Consumer<A>, Serializable {}

    // ----- tiny iterator adapters -----

    private static final class TransformingIterator<A, B> implements Iterator<B> {
        private final Iterator<A> src; private final Function<A, B> f;
        TransformingIterator(Iterator<A> src, Function<A, B> f) { this.src = src; this.f = f; }
        public boolean hasNext() { return src.hasNext(); }
        public B next() { return f.apply(src.next()); }
    }

    private static final class FilteringIterator<A> implements Iterator<A> {
        private final Iterator<A> src; private final Predicate<A> p;
        private A nxt; private boolean ready;
        FilteringIterator(Iterator<A> src, Predicate<A> p) { this.src = src; this.p = p; }
        public boolean hasNext() {
            while (!ready && src.hasNext()) {
                A v = src.next();
                if (p.test(v)) { nxt = v; ready = true; }
            }
            return ready;
        }
        public A next() {
            if (!hasNext()) throw new NoSuchElementException();
            ready = false; A v = nxt; nxt = null; return v;
        }
    }

    private static final class FlatteningIterator<A, B> implements Iterator<B> {
        private final Iterator<A> src; private final Function<A, Iterator<B>> f;
        private Iterator<B> current = java.util.Collections.emptyIterator();
        FlatteningIterator(Iterator<A> src, Function<A, Iterator<B>> f) { this.src = src; this.f = f; }
        public boolean hasNext() {
            while (!current.hasNext() && src.hasNext()) current = f.apply(src.next());
            return current.hasNext();
        }
        public B next() {
            if (!hasNext()) throw new NoSuchElementException();
            return current.next();
        }
    }

}
