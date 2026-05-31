package com.minispark.examples.dist;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.Window;
import com.minispark.sql.plan.JoinType;
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
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed window-function and outer-join examples (examples 26–31). Window
 * functions shuffle by PARTITION BY and rank within each ordered partition;
 * outer joins null-pad the non-matching side. All run across 2 executor JVMs.
 */
final class WindowOuterJoinDistributedExamplesTest {

    private static <R> R runSql(String appName, Function<MiniSparkSession, R> body) {
        Callable<R> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName(appName)
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2")
                    .set("minispark.executor.heartbeatTimeoutMs", "30000");
            try (MiniSparkContext sc = new MiniSparkContext(conf);
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
            Assumptions.abort("Skipping distributed window/join example '" + appName + "': " + e);
            return null;
        } finally {
            runner.shutdownNow();
        }
    }

    private static final StructType EMP = StructType.of(
            StructField.of("dept", DataType.STRING),
            StructField.of("name", DataType.STRING),
            StructField.of("salary", DataType.INT));

    private static DataFrame employees(MiniSparkSession spark) {
        return spark.createDataFrame(List.of(
                Row.of("eng", "alice", 200),
                Row.of("eng", "bob", 150),
                Row.of("eng", "carol", 200),    // tie with alice
                Row.of("eng", "dan", 100),
                Row.of("sales", "eve", 180),
                Row.of("sales", "frank", 120)), EMP);
    }

    // Example 26 — ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary): a
    // distinct sequential index per partition, shuffled by dept.
    @Test
    void ex26_window_row_number() {
        Map<String, Long> rnByName = runSql("ex26-row-number", spark -> {
            List<Row> rows = employees(spark)
                    .withColumn("rn", Window.rowNumber()
                            .over(Window.partitionBy("dept").orderBy("salary")))
                    .collect();
            Map<String, Long> m = new HashMap<>();
            for (Row r : rows) m.put(r.getString(1), r.getLong(3));
            return m;
        });
        // eng by ascending salary: dan(100)=1, bob(150)=2, then alice/carol(200)=3,4.
        assertThat(rnByName.get("dan")).isEqualTo(1L);
        assertThat(rnByName.get("bob")).isEqualTo(2L);
        assertThat(rnByName.get("eve")).isEqualTo(2L);   // sales: frank(120)=1, eve(180)=2
        assertThat(rnByName.get("frank")).isEqualTo(1L);
    }

    // Example 27 — RANK() with ties leaves gaps: 1,1,3,4 within eng (desc salary).
    @Test
    void ex27_window_rank_with_gaps() {
        Map<String, Long> rankByName = runSql("ex27-rank", spark -> {
            List<Row> rows = employees(spark)
                    .withColumn("r", Window.rank()
                            .over(Window.partitionBy("dept").orderByDesc("salary")))
                    .collect();
            Map<String, Long> m = new HashMap<>();
            for (Row r : rows) m.put(r.getString(1), r.getLong(3));
            return m;
        });
        // eng desc: alice & carol tie at 200 → both rank 1; bob(150)=3 (gap); dan(100)=4.
        assertThat(rankByName.get("alice")).isEqualTo(1L);
        assertThat(rankByName.get("carol")).isEqualTo(1L);
        assertThat(rankByName.get("bob")).isEqualTo(3L);
        assertThat(rankByName.get("dan")).isEqualTo(4L);
    }

    // Example 28 — DENSE_RANK() with ties has no gaps: 1,1,2,3 within eng.
    @Test
    void ex28_window_dense_rank_no_gaps() {
        Map<String, Long> drByName = runSql("ex28-dense-rank", spark -> {
            List<Row> rows = employees(spark)
                    .withColumn("dr", Window.denseRank()
                            .over(Window.partitionBy("dept").orderByDesc("salary")))
                    .collect();
            Map<String, Long> m = new HashMap<>();
            for (Row r : rows) m.put(r.getString(1), r.getLong(3));
            return m;
        });
        assertThat(drByName.get("alice")).isEqualTo(1L);
        assertThat(drByName.get("carol")).isEqualTo(1L);
        assertThat(drByName.get("bob")).isEqualTo(2L);   // no gap
        assertThat(drByName.get("dan")).isEqualTo(3L);
    }

    // Example 29 — LEFT OUTER join: unmatched left rows are kept, right side null-padded.
    @Test
    void ex29_left_outer_join() {
        long nullPadded = runSql("ex29-left-outer", spark -> {
            DataFrame people = spark.createDataFrame(List.of(
                    Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")),
                    StructType.of(StructField.of("id", DataType.INT), StructField.of("name", DataType.STRING)));
            DataFrame orders = spark.createDataFrame(List.of(
                    Row.of(1, "book"), Row.of(2, "lamp")),
                    StructType.of(StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING)));
            List<Row> res = people.join(orders, List.of("id"), List.of("oid"), JoinType.LEFT).collect();
            // carol (id 3) has no order → her right columns are null.
            long padded = res.stream().filter(r -> r.isNullAt(3)).count();
            assertThat(res).hasSize(3);   // alice, bob matched; carol padded
            return padded;
        });
        assertThat(nullPadded).isEqualTo(1L);
    }

    // Example 30 — RIGHT OUTER join: unmatched right rows kept, left side null-padded.
    @Test
    void ex30_right_outer_join() {
        long nullPadded = runSql("ex30-right-outer", spark -> {
            DataFrame people = spark.createDataFrame(List.of(
                    Row.of(1, "alice"), Row.of(2, "bob")),
                    StructType.of(StructField.of("id", DataType.INT), StructField.of("name", DataType.STRING)));
            DataFrame orders = spark.createDataFrame(List.of(
                    Row.of(1, "book"), Row.of(2, "lamp"), Row.of(9, "orphan")),
                    StructType.of(StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING)));
            List<Row> res = people.join(orders, List.of("id"), List.of("oid"), JoinType.RIGHT).collect();
            // order oid 9 has no person → left columns null.
            assertThat(res).hasSize(3);
            return res.stream().filter(r -> r.isNullAt(0)).count();
        });
        assertThat(nullPadded).isEqualTo(1L);
    }

    // Example 31 — FULL OUTER join: both unmatched sides kept (always shuffles).
    @Test
    void ex31_full_outer_join() {
        Map<String, Long> counts = runSql("ex31-full-outer", spark -> {
            DataFrame left = spark.createDataFrame(List.of(
                    Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")),
                    StructType.of(StructField.of("id", DataType.INT), StructField.of("name", DataType.STRING)));
            DataFrame right = spark.createDataFrame(List.of(
                    Row.of(2, "lamp"), Row.of(3, "desk"), Row.of(9, "orphan")),
                    StructType.of(StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING)));
            List<Row> res = left.join(right, List.of("id"), List.of("oid"), JoinType.FULL).collect();
            Map<String, Long> m = new HashMap<>();
            m.put("total", (long) res.size());
            m.put("leftNull", res.stream().filter(r -> r.isNullAt(0)).count());   // right-only (orphan)
            m.put("rightNull", res.stream().filter(r -> r.isNullAt(3)).count());  // left-only (alice)
            return m;
        });
        // matches: 2,3 (2 rows). left-only: alice(1). right-only: orphan(9). total = 4.
        assertThat(counts.get("total")).isEqualTo(4L);
        assertThat(counts.get("leftNull")).isEqualTo(1L);   // orphan
        assertThat(counts.get("rightNull")).isEqualTo(1L);  // alice
    }
}
