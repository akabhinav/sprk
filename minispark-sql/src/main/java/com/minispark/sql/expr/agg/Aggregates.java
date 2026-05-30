package com.minispark.sql.expr.agg;

import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.io.Serializable;

/**
 * The built-in aggregate functions. Each is a {@link AggregateFunction} over a
 * child {@link Expression} (except {@code count(*)}). Buffers are plain boxed
 * values (or a small record for AVG) so they serialize across the shuffle.
 *
 * Real Spark equivalent: the Count/Sum/Average/Min/Max aggregate expressions.
 */
public final class Aggregates {

    private Aggregates() {}

    /** {@code count(*)} — number of rows, ignores nulls only if given a child. */
    public static final class Count implements AggregateFunction {
        private final Expression child; // null => count(*)
        public Count(Expression child) { this.child = child; }
        public static Count star() { return new Count(null); }

        @Override public Object initialize() { return 0L; }
        @Override public Object update(Object buf, Row input) {
            if (child != null && child.eval(input) == null) return buf; // count(col) skips nulls
            return (Long) buf + 1;
        }
        @Override public Object merge(Object a, Object b) { return (Long) a + (Long) b; }
        @Override public Object evaluate(Object buf) { return buf; }
        @Override public DataType resultType(StructType s) { return DataType.LONG; }
        @Override public String name() { return child == null ? "count(*)" : "count(" + child.name() + ")"; }
        @Override public AggregateFunction bind(StructType s,
                java.util.function.BiFunction<Expression, StructType, Expression> b) {
            return child == null ? this : new Count(b.apply(child, s));
        }
    }

    /** {@code sum(child)} — LONG for integral children, else DOUBLE; null group → null. */
    public static final class Sum implements AggregateFunction {
        private final Expression child;
        private final boolean integral;
        public Sum(Expression child, StructType inputSchema) {
            this.child = child;
            // Before the analyzer binds the child it's unresolved and has no
            // knowable type; default to integral and let bind() recompute.
            this.integral = !child.resolved() || child.dataType(inputSchema) != DataType.DOUBLE;
        }
        @Override public Object initialize() { return null; } // null until first non-null seen
        @Override public Object update(Object buf, Row input) {
            Object v = child.eval(input);
            if (v == null) return buf;
            double add = ((Number) v).doubleValue();
            return buf == null ? add : (Double) buf + add;
        }
        @Override public Object merge(Object a, Object b) {
            if (a == null) return b;
            if (b == null) return a;
            return (Double) a + (Double) b;
        }
        @Override public Object evaluate(Object buf) {
            if (buf == null) return null;
            return integral ? (Object) (long) (double) (Double) buf : buf;
        }
        @Override public DataType resultType(StructType s) {
            return integral ? DataType.LONG : DataType.DOUBLE;
        }
        @Override public String name() { return "sum(" + child.name() + ")"; }
        @Override public AggregateFunction bind(StructType s,
                java.util.function.BiFunction<Expression, StructType, Expression> b) {
            return new Sum(b.apply(child, s), s);
        }
    }

    /** {@code avg(child)} — carries (sum,count) so partial averages merge correctly. */
    public static final class Avg implements AggregateFunction {
        private final Expression child;
        public Avg(Expression child) { this.child = child; }

        record SC(double sum, long count) implements Serializable {}

        @Override public Object initialize() { return new SC(0, 0); }
        @Override public Object update(Object buf, Row input) {
            Object v = child.eval(input);
            if (v == null) return buf;
            SC sc = (SC) buf;
            return new SC(sc.sum + ((Number) v).doubleValue(), sc.count + 1);
        }
        @Override public Object merge(Object a, Object b) {
            SC x = (SC) a, y = (SC) b;
            return new SC(x.sum + y.sum, x.count + y.count);
        }
        @Override public Object evaluate(Object buf) {
            SC sc = (SC) buf;
            return sc.count == 0 ? null : sc.sum / sc.count;
        }
        @Override public DataType resultType(StructType s) { return DataType.DOUBLE; }
        @Override public String name() { return "avg(" + child.name() + ")"; }
        @Override public AggregateFunction bind(StructType s,
                java.util.function.BiFunction<Expression, StructType, Expression> b) {
            return new Avg(b.apply(child, s));
        }
    }

    /** {@code min(child)} / {@code max(child)} over a Comparable child; null group → null. */
    public static final class MinMax implements AggregateFunction {
        private final Expression child;
        private final boolean isMin;
        private final DataType type;
        public MinMax(Expression child, boolean isMin, StructType inputSchema) {
            this.child = child;
            this.isMin = isMin;
            // Unknown until the child is bound; bind() reconstructs with the real type.
            this.type = child.resolved() ? child.dataType(inputSchema) : DataType.INT;
        }
        @Override public Object initialize() { return null; }
        @Override @SuppressWarnings({"unchecked", "rawtypes"})
        public Object update(Object buf, Row input) {
            Object v = child.eval(input);
            if (v == null) return buf;
            if (buf == null) return v;
            int cmp = ((Comparable) buf).compareTo(v);
            return (isMin ? cmp <= 0 : cmp >= 0) ? buf : v;
        }
        @Override @SuppressWarnings({"unchecked", "rawtypes"})
        public Object merge(Object a, Object b) {
            if (a == null) return b;
            if (b == null) return a;
            int cmp = ((Comparable) a).compareTo(b);
            return (isMin ? cmp <= 0 : cmp >= 0) ? a : b;
        }
        @Override public Object evaluate(Object buf) { return buf; }
        @Override public DataType resultType(StructType s) { return type; }
        @Override public String name() { return (isMin ? "min(" : "max(") + child.name() + ")"; }
        @Override public AggregateFunction bind(StructType s,
                java.util.function.BiFunction<Expression, StructType, Expression> b) {
            return new MinMax(b.apply(child, s), isMin, s);
        }
    }
}
