package com.minispark.sql.execution;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Shared helpers for the shuffle keys used by aggregation, join, and sort.
 * Centralizes two correctness concerns that were previously duplicated (and
 * buggy) across three exec nodes:
 *
 * <ul>
 *   <li><b>Numeric normalization</b> — boxed numeric values are normalized so
 *       that {@code Integer 5}, {@code Long 5}, and {@code Double 5.0} hash and
 *       compare equal. Without this, a join/group across columns of different
 *       inferred numeric types (e.g. one CSV column INT, another LONG) would
 *       silently never match.</li>
 *   <li><b>SQL null semantics in equi-joins</b> — a null key must NOT match
 *       another null (unlike Java equality). {@link #joinKey} maps any tuple
 *       containing a null to a unique, never-equal key so null-keyed rows never
 *       join. (Grouping, by contrast, DOES put all nulls in one group, so
 *       {@link #groupKey} keeps nulls.)</li>
 * </ul>
 */
final class Keys {

    private Keys() {}

    /** Normalize a value for hashing/equality: integral→Long, floating→Double. */
    static Object normalize(Object v) {
        if (v instanceof Integer || v instanceof Long) return ((Number) v).longValue();
        if (v instanceof Float || v instanceof Double) return ((Number) v).doubleValue();
        return v;
    }

    /** A hashable key for GROUP BY: nulls preserved (all nulls group together). */
    static ValueKey groupKey(Object[] raw) {
        Object[] norm = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) norm[i] = normalize(raw[i]);
        return new ValueKey(norm);
    }

    /**
     * A hashable key for an equi-join. If any component is null the key is made
     * <b>unique</b> (carrying a fresh nonce), so the row never matches any other
     * row — SQL's "null never equals anything" — yet still flows through the
     * cogroup so an OUTER join can emit it null-padded. A non-null key is
     * normalized like {@link #groupKey} so numeric types match across
     * Integer/Long/Double.
     */
    static ValueKey joinKey(Object[] raw) {
        for (Object o : raw) {
            if (o == null) return ValueKey.unique();   // isolated: matches nothing
        }
        Object[] norm = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) norm[i] = normalize(raw[i]);
        return new ValueKey(norm);
    }

    /** Serializable, value-equal key over a normalized tuple (or a unique nonce). */
    static final class ValueKey implements Serializable {
        final Object[] values;
        // null for value keys; a globally-unique id for null-containing join keys.
        // A random UUID (not a per-JVM counter) so two null-key rows produced on
        // DIFFERENT executors can never collide across the shuffle.
        final java.util.UUID nonce;
        ValueKey(Object[] values) { this.values = values; this.nonce = null; }
        private ValueKey(java.util.UUID nonce) { this.values = null; this.nonce = nonce; }
        static ValueKey unique() { return new ValueKey(java.util.UUID.randomUUID()); }
        @Override public boolean equals(Object o) {
            if (!(o instanceof ValueKey k)) return false;
            if (nonce != null || k.nonce != null) return java.util.Objects.equals(nonce, k.nonce);
            return Arrays.equals(values, k.values);
        }
        @Override public int hashCode() { return nonce != null ? nonce.hashCode() : Arrays.hashCode(values); }
    }
}
