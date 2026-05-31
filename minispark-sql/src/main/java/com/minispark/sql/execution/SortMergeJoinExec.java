package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.rdd.ZippedPartitionsRDD2;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Equi-join via the classic <b>sort-merge</b> algorithm: both sides shuffled
 * through the <i>same</i> partitioner so matching keys land in matching
 * reducer partitions, then zipped per-partition into one task that sorts both
 * sides by key and merges with two cursors.
 *
 * <p><b>Why a third strategy.</b> {@link ShuffledHashJoinExec} builds an
 * in-memory multimap per group of keys; for very wide groups that's a memory
 * cliff. Sort-merge streams a sorted sequence on each side and never
 * materialises a full hash table per key — at the cost of an O(n log n) sort
 * per partition. Real Spark makes SMJ the default for non-broadcast joins
 * (the sort is paid back by allowing the shuffle data to spill to disk and
 * stream straight through). In MiniSpark we sort in-memory, so the streaming
 * benefit is pedagogical rather than memory-real, but the operator and its
 * planner integration are the same.
 *
 * <p>Supports INNER, LEFT, RIGHT, FULL — FULL just needs both sides' cursors
 * to advance through unmatched groups.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.joins.SortMergeJoinExec.
 */
public final class SortMergeJoinExec implements PhysicalPlan {

    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;
    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final int leftWidth;
    private final int rightWidth;
    private final MiniSparkContext sc;
    private final int numPartitions;

    public SortMergeJoinExec(List<Expression> leftKeys, List<Expression> rightKeys,
                             JoinType joinType, StructType schema,
                             PhysicalPlan left, PhysicalPlan right,
                             MiniSparkContext sc, int numPartitions) {
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinType = joinType;
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.leftWidth = left.schema().size();
        this.rightWidth = right.schema().size();
        this.sc = sc;
        this.numPartitions = numPartitions;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() { return "SortMergeJoinExec " + joinType + " " + schema; }

    @Override
    public RDD<Row> execute() {
        // Shuffle both sides through the SAME HashPartitioner so partition i
        // on the left holds exactly the keys whose hash places them in
        // partition i on the right. That's the co-partitioning invariant SMJ
        // depends on — without it, sorting a partition wouldn't give us
        // contiguous matching keys across the two sides.
        HashPartitioner partitioner = new HashPartitioner(numPartitions);
        List<Expression> lk = leftKeys, rk = rightKeys;

        RDD<Tuple2<Keys.ValueKey, Row>> leftPairs = left.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<Keys.ValueKey, Row>>) row ->
                        new Tuple2<>(key(lk, row), row));
        RDD<Tuple2<Keys.ValueKey, Row>> rightPairs = right.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<Keys.ValueKey, Row>>) row ->
                        new Tuple2<>(key(rk, row), row));

        ShuffledRDD<Keys.ValueKey, Row> leftShuffled =
                new ShuffledRDD<>(sc, leftPairs, partitioner);
        ShuffledRDD<Keys.ValueKey, Row> rightShuffled =
                new ShuffledRDD<>(sc, rightPairs, partitioner);

        JoinType jt = this.joinType;
        int lw = this.leftWidth;
        int rw = this.rightWidth;

        return new ZippedPartitionsRDD2<>(sc, leftShuffled, rightShuffled,
                (ZippedPartitionsRDD2.ZipFunction<
                        Tuple2<Keys.ValueKey, Row>,
                        Tuple2<Keys.ValueKey, Row>,
                        Row>) (lIt, rIt) -> mergeJoin(lIt, rIt, jt, lw, rw));
    }

    private static Keys.ValueKey key(List<Expression> keys, Row row) {
        Object[] k = new Object[keys.size()];
        for (int i = 0; i < keys.size(); i++) k[i] = keys.get(i).eval(row);
        return Keys.joinKey(k);
    }

    /**
     * Drain both sides, sort by key, then merge with two cursors. Each key
     * group on the left/right is collected into a list because join semantics
     * require the cartesian product per matching key, which can't be streamed
     * one record at a time when both sides repeat.
     */
    private static Iterator<Row> mergeJoin(
            Iterator<Tuple2<Keys.ValueKey, Row>> leftIt,
            Iterator<Tuple2<Keys.ValueKey, Row>> rightIt,
            JoinType jt, int lw, int rw) {

        List<Tuple2<Keys.ValueKey, Row>> leftAll = drain(leftIt);
        List<Tuple2<Keys.ValueKey, Row>> rightAll = drain(rightIt);

        // Order by ValueKey — unique-nonce keys (from null join keys) sort
        // last among themselves and never compare equal to a value key, so a
        // null-keyed row never matches but still appears at the tail where
        // OUTER joins can null-pad it.
        Comparator<Tuple2<Keys.ValueKey, Row>> byKey = Comparator.comparing(Tuple2::_1, KEY_ORDER);
        leftAll.sort(byKey);
        rightAll.sort(byKey);

        boolean emitUnmatchedLeft  = jt == JoinType.LEFT  || jt == JoinType.FULL;
        boolean emitUnmatchedRight = jt == JoinType.RIGHT || jt == JoinType.FULL;

        List<Row> out = new ArrayList<>();
        int li = 0, ri = 0;
        while (li < leftAll.size() && ri < rightAll.size()) {
            Keys.ValueKey lKey = leftAll.get(li)._1();
            Keys.ValueKey rKey = rightAll.get(ri)._1();
            int cmp = KEY_ORDER.compare(lKey, rKey);
            if (cmp < 0) {
                if (emitUnmatchedLeft) out.add(concat(leftAll.get(li)._2(), null, lw, rw));
                li++;
            } else if (cmp > 0) {
                if (emitUnmatchedRight) out.add(concat(null, rightAll.get(ri)._2(), lw, rw));
                ri++;
            } else {
                // Matching key group on both sides — collect the run on each side
                // and emit the cartesian product.
                int lStart = li, rStart = ri;
                while (li < leftAll.size() && KEY_ORDER.compare(leftAll.get(li)._1(), lKey) == 0) li++;
                while (ri < rightAll.size() && KEY_ORDER.compare(rightAll.get(ri)._1(), rKey) == 0) ri++;
                for (int a = lStart; a < li; a++) {
                    for (int b = rStart; b < ri; b++) {
                        out.add(concat(leftAll.get(a)._2(), rightAll.get(b)._2(), lw, rw));
                    }
                }
            }
        }
        if (emitUnmatchedLeft) {
            while (li < leftAll.size()) out.add(concat(leftAll.get(li++)._2(), null, lw, rw));
        }
        if (emitUnmatchedRight) {
            while (ri < rightAll.size()) out.add(concat(null, rightAll.get(ri++)._2(), lw, rw));
        }
        return out.iterator();
    }

    private static <T> List<T> drain(Iterator<T> it) {
        List<T> l = new ArrayList<>();
        while (it.hasNext()) l.add(it.next());
        return l;
    }

    /**
     * Total order over {@link Keys.ValueKey}: value-keys compare by their
     * normalised tuple (Object[]); null-key rows (unique-nonce keys) sort
     * after every value-key and stably among themselves by nonce string. This
     * matches the equi-join semantics — null keys can never match a value key
     * AND never match another null key, but OUTER joins must still see them.
     */
    private static final Comparator<Keys.ValueKey> KEY_ORDER = (a, b) -> {
        boolean aNull = isNullKey(a);
        boolean bNull = isNullKey(b);
        if (aNull && bNull) return nonceOf(a).compareTo(nonceOf(b));
        if (aNull) return 1;
        if (bNull) return -1;
        Object[] av = valuesOf(a);
        Object[] bv = valuesOf(b);
        int n = Math.min(av.length, bv.length);
        for (int i = 0; i < n; i++) {
            int c = compareValues(av[i], bv[i]);
            if (c != 0) return c;
        }
        return Integer.compare(av.length, bv.length);
    };

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Comparable ca && a.getClass().isInstance(b)) return ca.compareTo(b);
        // Mixed-type fallback (already rare since Keys.normalize unifies numerics):
        // string-compare so we still produce a total order rather than throwing.
        return a.toString().compareTo(b.toString());
    }

    // Inspect the package-private ValueKey shape via reflection only for sorting.
    // Cheap: two-field record-like class, accessed once per row.
    private static boolean isNullKey(Keys.ValueKey k) { return k.values == null; }
    private static Object[] valuesOf(Keys.ValueKey k) { return k.values; }
    private static String nonceOf(Keys.ValueKey k) { return k.nonce.toString(); }

    private static Row concat(Row left, Row right, int lw, int rw) {
        Object[] out = new Object[lw + rw];
        if (left != null) for (int i = 0; i < lw; i++) out[i] = left.get(i);
        if (right != null) for (int i = 0; i < rw; i++) out[lw + i] = right.get(i);
        return new Row(out);
    }
}
