package com.minispark.rdd;

import java.io.Serializable;

/**
 * Marker for a parent dependency of one RDD on another.
 *
 * <p>The single most important design choice in Spark is the split between
 * narrow and wide dependencies. Narrow deps pipeline within a stage; wide
 * (shuffle) deps materialize and become a stage boundary. Phase 2 fills in
 * the wide-dependency subclass.
 *
 * Real Spark equivalent: org.apache.spark.Dependency
 */
public abstract class Dependency<T> implements Serializable {
    public abstract RDD<T> rdd();
}
