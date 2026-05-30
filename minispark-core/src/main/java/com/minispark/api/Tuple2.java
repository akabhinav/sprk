package com.minispark.api;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable 2-tuple, named with Scala-style accessors so that the surface
 * API matches Spark's documentation when the reader graduates to the real
 * codebase.
 *
 * Real Spark equivalent: scala.Tuple2
 */
public final class Tuple2<A, B> implements Serializable {
    private final A _1;
    private final B _2;

    public Tuple2(A a, B b) { this._1 = a; this._2 = b; }

    public A _1() { return _1; }
    public B _2() { return _2; }

    @Override public boolean equals(Object o) {
        if (!(o instanceof Tuple2<?, ?> t)) return false;
        return Objects.equals(_1, t._1) && Objects.equals(_2, t._2);
    }

    @Override public int hashCode() { return Objects.hash(_1, _2); }

    @Override public String toString() { return "(" + _1 + "," + _2 + ")"; }
}
