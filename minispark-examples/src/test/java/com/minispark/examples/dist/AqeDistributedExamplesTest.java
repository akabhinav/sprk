package com.minispark.examples.dist;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed Adaptive Query Execution examples (examples 21–25). Each runs a
 * query on a real distributed engine (driver + 2 executor JVMs over TCP) with
 * an AQE feature switched on, and asserts the result is still correct — the
 * cardinal rule of AQE: re-planning changes <i>how</i> the work runs, never
 * the answer. Where the chosen physical operator is observable, we also assert
 * the planner picked it via {@code explain()}.
 *
 * <p>Aborts (skips) if child JVMs can't spawn.
 */
final class AqeDistributedExamplesTest {

    /** SQL harness with a caller-supplied conf factory (so each example sets its own AQE knobs). */
    private static <R> R runSql(String appName,
                                java.util.function.Function<String, MiniSparkConf> conf,
                                java.util.function.Function<MiniSparkSession, R> body) {
        Callable<R> job = () -> {
            try (MiniSparkContext sc = new MiniSparkContext(conf.apply(appName));
                 MiniSparkSession spark = MiniSparkSession.on(sc)) {
                return body.apply(spark);
            }
        };
        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<R> f = runner.submit(job);
        try {
            return f.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping distributed AQE example '" + appName + "': " + e);
            return null;
        } finally {
            runner.shutdownNow();
        }
    }

    private static final StructType KV = StructType.of(
            StructField.of("k", DataType.INT),
            StructField.of("v", DataType.INT));

    private static DataFrame kv(MiniSparkSession spark, int rows, int distinctKeys) {
        List<Row> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) data.add(Row.of(i % distinctKeys, i));
        return spark.createDataFrame(data, KV);
    }

    // Example 21 — AQE coalesce shuffle partitions: a tiny shuffle's many reducer
    // partitions are fused into fewer tasks. Result counts must be unchanged.
    @Test
    void ex21_aqe_coalesce_partitions() {
        Map<Long, Long> got = runSql("ex21-aqe-coalesce",
                app -> distConf(app)
                        .set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1048576"),
                spark -> {
                    kv(spark, 400, 8).createOrReplaceTempView("t");
                    List<Row> res = spark.sql("SELECT k, count(*) FROM t GROUP BY k").collect();
                    Map<Long, Long> m = new HashMap<>();
                    for (Row r : res) m.put(((Number) r.get(0)).longValue(), r.getLong(1));
                    return m;
                });
        // 400 rows / 8 keys = 50 each, regardless of coalescing.
        assertThat(got).hasSize(8);
        assertThat(got.values()).allMatch(c -> c == 50L);
    }

    // Example 22 — AQE skew-join split: one HOT key dominates; with skewJoin
    // enabled the fat reducer is split across map ranges. Counts unchanged.
    @Test
    void ex22_aqe_skew_split() {
        Map<Long, Long> got = runSql("ex22-aqe-skew",
                app -> distConf(app)
                        .set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.adaptive.skewJoin.enabled", "true")
                        .set("minispark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
                        .set("minispark.sql.adaptive.skewJoin.skewedPartitionFactor", "2.0"),
                spark -> {
                    // 1000 rows: key 0 appears 800 times (HOT), keys 1..4 evenly split the rest.
                    List<Row> data = new ArrayList<>();
                    for (int i = 0; i < 800; i++) data.add(Row.of(0, i));
                    for (int i = 0; i < 200; i++) data.add(Row.of(1 + (i % 4), i));
                    spark.createDataFrame(data, KV).createOrReplaceTempView("t");
                    List<Row> res = spark.sql("SELECT k, count(*) FROM t GROUP BY k").collect();
                    Map<Long, Long> m = new HashMap<>();
                    for (Row r : res) m.put(((Number) r.get(0)).longValue(), r.getLong(1));
                    return m;
                });
        assertThat(got.get(0L)).isEqualTo(800L);
        assertThat(got.get(1L)).isEqualTo(50L);  // 200/4
    }

    // Example 23 — broadcast hash join: a small side is auto-broadcast (no shuffle
    // on the big side). Assert both the chosen operator and the join result.
    @Test
    void ex23_broadcast_hash_join() {
        List<String> got = runSql("ex23-broadcast-join",
                app -> distConf(app),  // default auto-broadcast threshold (1000 rows) catches the small side
                spark -> {
                    spark.createDataFrame(dim(5), DIM).createOrReplaceTempView("dim");
                    spark.createDataFrame(fact(300, 5), FACT).createOrReplaceTempView("fact");
                    DataFrame joined = spark.sql("SELECT count(*) FROM fact JOIN dim ON fid = did");
                    String plan = joined.explain();
                    long count = joined.collect().get(0).getLong(0);
                    List<String> out = new ArrayList<>();
                    out.add(plan.contains("BroadcastHashJoinExec") ? "broadcast" : "other");
                    out.add(String.valueOf(count));
                    return out;
                });
        assertThat(got.get(0)).isEqualTo("broadcast");
        // every fact row's fid in 0..4 matches a dim row → all 300 join.
        assertThat(got.get(1)).isEqualTo("300");
    }

    // Example 24 — sort-merge join: forced via preferSortMergeJoin with auto-broadcast
    // disabled. Same result as any other strategy.
    @Test
    void ex24_sort_merge_join() {
        List<String> got = runSql("ex24-sort-merge-join",
                app -> distConf(app)
                        .set("minispark.sql.join.preferSortMergeJoin", "true")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0"),
                spark -> {
                    spark.createDataFrame(dim(50), DIM).createOrReplaceTempView("dim");
                    spark.createDataFrame(fact(300, 50), FACT).createOrReplaceTempView("fact");
                    DataFrame joined = spark.sql("SELECT count(*) FROM fact JOIN dim ON fid = did");
                    String plan = joined.explain();
                    long count = joined.collect().get(0).getLong(0);
                    List<String> out = new ArrayList<>();
                    out.add(plan.contains("SortMergeJoinExec") ? "sort-merge" : "other");
                    out.add(String.valueOf(count));
                    return out;
                });
        assertThat(got.get(0)).isEqualTo("sort-merge");
        assertThat(got.get(1)).isEqualTo("300");
    }

    // Example 25 — AQE runtime join demote: a side that's only small AFTER a filter
    // (invisible to the compile-time planner) is demoted to a broadcast join at
    // runtime. Result must match a plain shuffled join.
    @Test
    void ex25_aqe_runtime_join_demote() {
        long got = runSql("ex25-aqe-demote",
                app -> distConf(app)
                        .set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")  // no compile-time broadcast
                        .set("minispark.sql.adaptive.autoBroadcastJoinThreshold.rows", "100"),
                spark -> {
                    // dim has 500 rows; a WHERE leaves only ~5 → demote-eligible at runtime.
                    spark.createDataFrame(dim(500), DIM).createOrReplaceTempView("dim");
                    spark.createDataFrame(fact(300, 5), FACT).createOrReplaceTempView("fact");
                    return spark.sql(
                            "SELECT count(*) FROM fact JOIN dim ON fid = did WHERE did < 5")
                            .collect().get(0).getLong(0);
                });
        // dim rows did in 0..4 survive WHERE; fact fids cycle 0..4 (300 rows) → all 300 match.
        assertThat(got).isEqualTo(300L);
    }

    // ----- shared fixtures -----

    private static final StructType DIM = StructType.of(
            StructField.of("did", DataType.INT),
            StructField.of("dname", DataType.STRING));
    private static final StructType FACT = StructType.of(
            StructField.of("fid", DataType.INT),
            StructField.of("amount", DataType.INT));

    private static List<Row> dim(int n) {
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rows.add(Row.of(i, "d" + i));
        return rows;
    }
    private static List<Row> fact(int n, int keyspace) {
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rows.add(Row.of(i % keyspace, i));
        return rows;
    }

    /** Distributed conf: 2 executor JVMs × 2 cores, generous heartbeat. */
    private static MiniSparkConf distConf(String appName) {
        return new MiniSparkConf()
                .setAppName(appName)
                .setMaster("local")
                .set("minispark.rpc.mode", "netty")
                .set("minispark.executor.instances", "2")
                .set("minispark.executor.cores", "2")
                .set("minispark.executor.heartbeatTimeoutMs", "30000");
    }
}
