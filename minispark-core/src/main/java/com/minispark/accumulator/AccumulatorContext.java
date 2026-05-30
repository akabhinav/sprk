package com.minispark.accumulator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Driver-side registry of accumulators. The backend, on receiving accumulator
 * deltas in a {@code StatusUpdate}, looks each up by id here and folds the
 * delta in.
 *
 * <p>Static (JVM-scoped) because accumulator ids are minted from a global
 * sequence and the executor backend doesn't carry a per-context reference.
 * Matches real Spark's {@code AccumulatorContext}.
 */
public final class AccumulatorContext {
    private static final AtomicLong ID_GEN = new AtomicLong();
    private static final Map<Long, Accumulator<?>> REGISTRY = new ConcurrentHashMap<>();

    private AccumulatorContext() {}

    public static long nextId() { return ID_GEN.incrementAndGet(); }

    public static void register(Accumulator<?> acc) { REGISTRY.put(acc.id(), acc); }

    public static Accumulator<?> lookup(long id) { return REGISTRY.get(id); }

    public static void mergeAll(Map<Long, Object> deltas) {
        if (deltas == null || deltas.isEmpty()) return;
        for (Map.Entry<Long, Object> e : deltas.entrySet()) {
            Accumulator<?> a = REGISTRY.get(e.getKey());
            if (a != null) a.mergeDelta(e.getValue());
        }
    }
}
