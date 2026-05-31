package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.MapPartitionsRDD;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.window.WindowExpression;
import com.minispark.sql.expr.window.WindowFunction;
import com.minispark.sql.plan.SortOrder;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Physical execution of one or more window expressions.
 *
 * <p><b>Algorithm</b> (per window group — currently we batch all
 * {@link WindowExpression}s in one node, assuming they share a single
 * partition layout; the SQL planner would split mismatched specs into
 * separate WindowExec nodes):
 * <ol>
 *   <li>Map each row to {@code (partitionKey, row)} where {@code partitionKey}
 *       is the tuple of evaluated PARTITION BY expressions.</li>
 *   <li>Shuffle through a {@link HashPartitioner} so all rows with the same
 *       partitionKey land on the same task.</li>
 *   <li>Per task, bucket the incoming rows by partitionKey (so multiple
 *       partition keys that happened to hash-collide stay independent), sort
 *       each bucket by ORDER BY, then call each {@link WindowFunction#evaluate}
 *       on the sorted bucket.</li>
 *   <li>Emit one output row per input row: child columns + one window output
 *       column per WindowExpression.</li>
 * </ol>
 *
 * <p>Empty PARTITION BY → all rows go to a single bucket on a single task
 * (using a constant key). This is the right semantics for global window
 * functions like {@code ROW_NUMBER() OVER (ORDER BY ts)}.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.window.WindowExec.
 */
public final class WindowExec implements PhysicalPlan {

    private final List<WindowExpression> windowExprs;
    private final List<String> outputNames;
    private final StructType schema;
    private final PhysicalPlan child;
    private final MiniSparkContext sc;
    private final int numPartitions;

    public WindowExec(List<WindowExpression> windowExprs, List<String> outputNames,
                      StructType schema, PhysicalPlan child,
                      MiniSparkContext sc, int numPartitions) {
        this.windowExprs = windowExprs;
        this.outputNames = outputNames;
        this.schema = schema;
        this.child = child;
        this.sc = sc;
        this.numPartitions = numPartitions;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "WindowExec(" + outputNames + ")"; }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RDD<Row> execute() {
        // Validate single shared window spec. Mixed specs in one query would
        // need to be split into multiple WindowExec nodes by the planner — a
        // future analyzer/planner enhancement; today the DataFrame API only
        // emits one window expression per Window logical node.
        if (windowExprs.isEmpty()) return child.execute();
        com.minispark.sql.expr.window.WindowSpec spec = windowExprs.get(0).spec();
        for (WindowExpression w : windowExprs) {
            if (w.spec() != spec) {
                throw new UnsupportedOperationException(
                        "WindowExec does not yet support mixed WindowSpecs in one node");
            }
        }

        List<Expression> partitionBy = spec.partitionBy();
        List<SortOrder> orderBy = spec.orderBy();
        int childWidth = child.schema().size();
        List<WindowFunction> functions = new ArrayList<>();
        for (WindowExpression w : windowExprs) functions.add(w.function());

        RDD<Row> input = child.execute();

        // 1) Map to (partitionKey, row). Empty PARTITION BY → constant key so
        //    everything funnels into one task; the warning is the user's job.
        RDD<Tuple2<Keys.ValueKey, Row>> keyed = input.map(
                (RDD.SerializableFunction<Row, Tuple2<Keys.ValueKey, Row>>) row -> {
                    if (partitionBy.isEmpty()) {
                        return new Tuple2<>(GLOBAL_KEY, row);
                    }
                    Object[] k = new Object[partitionBy.size()];
                    for (int i = 0; i < partitionBy.size(); i++) k[i] = partitionBy.get(i).eval(row);
                    return new Tuple2<>(Keys.groupKey(k), row);
                });

        // 2) Shuffle into N partitions. Empty PARTITION BY uses 1 partition —
        //    a single task processes the whole logical window. Otherwise use
        //    the configured numPartitions (typically defaultParallelism).
        int n = partitionBy.isEmpty() ? 1 : numPartitions;
        ShuffledRDD<Keys.ValueKey, Row> shuffled =
                new ShuffledRDD<>(sc, keyed, new HashPartitioner(n));

        // 3) Within each shuffle partition, bucket by exact key, sort each
        //    bucket by ORDER BY, evaluate window functions, emit one output
        //    row per input row with window values appended.
        boolean[] asc = new boolean[orderBy.size()];
        for (int i = 0; i < orderBy.size(); i++) asc[i] = orderBy.get(i).ascending();

        return new MapPartitionsRDD<>(sc, shuffled, (ctx, partIdx, it) -> {
            Map<Keys.ValueKey, List<Row>> buckets = new LinkedHashMap<>();
            while (it.hasNext()) {
                Tuple2<Keys.ValueKey, Row> kv = it.next();
                buckets.computeIfAbsent(kv._1(), k -> new ArrayList<>()).add(kv._2());
            }

            List<Row> out = new ArrayList<>();
            for (List<Row> bucket : buckets.values()) {
                // Sort the bucket by ORDER BY. SortExec.SortKey already encodes
                // value+direction+nulls-first comparison; reuse it.
                List<SortExec.SortKey> keys = new ArrayList<>(bucket.size());
                for (Row r : bucket) {
                    Object[] vals = new Object[orderBy.size()];
                    for (int i = 0; i < orderBy.size(); i++) vals[i] = orderBy.get(i).expr().eval(r);
                    keys.add(new SortExec.SortKey(vals, asc));
                }
                // Sort rows + keys together by the key. Build a parallel index
                // list so we can apply the permutation to both.
                Integer[] idx = new Integer[bucket.size()];
                for (int i = 0; i < idx.length; i++) idx[i] = i;
                java.util.Arrays.sort(idx, Comparator.comparing(keys::get));

                List<Row> sortedRows = new ArrayList<>(bucket.size());
                List<Object[]> sortedOrderKeys = new ArrayList<>(bucket.size());
                for (int i : idx) {
                    sortedRows.add(bucket.get(i));
                    sortedOrderKeys.add(keys.get(i).values);
                }

                // Run each window function over the sorted bucket — outputs
                // align row-for-row with sortedRows.
                List<List<Object>> outputs = new ArrayList<>(functions.size());
                for (WindowFunction fn : functions) outputs.add(fn.evaluate(sortedRows, sortedOrderKeys));

                for (int i = 0; i < sortedRows.size(); i++) {
                    Row r = sortedRows.get(i);
                    Object[] outArr = new Object[childWidth + functions.size()];
                    for (int c = 0; c < childWidth; c++) outArr[c] = r.get(c);
                    for (int w = 0; w < functions.size(); w++) outArr[childWidth + w] = outputs.get(w).get(i);
                    out.add(new Row(outArr));
                }
            }
            return out.iterator();
        });
    }

    /** Constant key for empty-PARTITION-BY windows: every row hashes to the same bucket. */
    private static final Keys.ValueKey GLOBAL_KEY = Keys.groupKey(new Object[]{"__window_global__"});
}
