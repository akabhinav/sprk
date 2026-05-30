package com.minispark.broadcast;

import java.io.Serializable;

/**
 * A read-only value shipped to executors <i>once</i> and cached there, rather
 * than being serialized into every task closure that references it.
 *
 * <p>Why it exists: if a 100 MB lookup table is captured by a lambda, naive
 * execution serializes those 100 MB into every one of (say) 10 000 tasks — a
 * terabyte of redundant transfer. A broadcast sends it once per executor and
 * every task on that executor reads the cached copy. The handle itself is
 * tiny (just an id + the driver's location), so closures that capture a
 * {@code Broadcast} stay small.
 *
 * Real Spark equivalent: org.apache.spark.broadcast.Broadcast
 */
public interface Broadcast<T> extends Serializable {
    long id();

    /** The broadcast value, fetched-and-cached on first access per executor. */
    T value();
}
