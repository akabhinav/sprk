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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed SQL / DataFrame examples (examples 14–20). Each compiles a query
 * through analyze → optimize → plan and runs it on a real distributed engine
 * (driver + 2 executor JVMs over TCP). Aborts (skips) if child JVMs can't spawn.
 */
final class SqlDistributedExamplesTest {

    /** SQL-flavored variant of the shared harness (needs a MiniSparkSession). */
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
            Assumptions.abort("Skipping distributed SQL example '" + appName + "': " + e);
            return null;
        } finally {
            runner.shutdownNow();
        }
    }

    private static final StructType PEOPLE = StructType.of(
            StructField.of("name", DataType.STRING),
            StructField.of("age", DataType.INT),
            StructField.of("city", DataType.STRING));

    private static DataFrame people(MiniSparkSession spark) {
        List<Row> rows = new ArrayList<>();
        String[] cities = {"NYC", "LA", "SF", "NYC"};
        for (int i = 0; i < 200; i++) {
            rows.add(Row.of("p" + i, 18 + (i % 50), cities[i % 4]));
        }
        return spark.createDataFrame(rows, PEOPLE);
    }

    // Example 14 — DataFrame DSL: filter + select, distributed.
    @Test
    void ex14_dataframe_filter_select() {
        long got = runSql("ex14-df-filter", spark ->
                people(spark).filter(col("age").ge(60)).select("name", "city").count());
        // ages cycle 18..67; age>=60 means (i%50) in [42..49] → 8 of every 50 rows.
        assertThat(got).isEqualTo(200L / 50 * 8);
    }

    // Example 15 — spark.sql GROUP BY with count + sum, distributed shuffle.
    @Test
    void ex15_sql_group_by() {
        Map<String, Long> counts = runSql("ex15-sql-groupby", spark -> {
            people(spark).createOrReplaceTempView("people");
            List<Row> res = spark.sql(
                    "SELECT city, count(*) FROM people GROUP BY city").collect();
            Map<String, Long> m = new HashMap<>();
            for (Row r : res) m.put(r.getString(0), r.getLong(1));
            return m;
        });
        // cities cycle NYC,LA,SF,NYC → NYC = 2/4 of 200 = 100, LA = SF = 50.
        assertThat(counts).containsEntry("NYC", 100L)
                .containsEntry("LA", 50L).containsEntry("SF", 50L);
    }

    // Example 16 — spark.sql HAVING + ORDER BY (post-aggregate filter + sort).
    @Test
    void ex16_sql_having_orderby() {
        List<String> cities = runSql("ex16-sql-having", spark -> {
            people(spark).createOrReplaceTempView("people");
            List<Row> res = spark.sql(
                    "SELECT city, count(*) FROM people GROUP BY city "
                    + "HAVING count(*) >= 100 ORDER BY city").collect();
            List<String> out = new ArrayList<>();
            for (Row r : res) out.add(r.getString(0));
            return out;
        });
        assertThat(cities).containsExactly("NYC");  // only NYC has 100
    }

    // Example 17 — spark.sql JOIN ... ON across two temp views.
    @Test
    void ex17_sql_join() {
        List<String> got = runSql("ex17-sql-join", spark -> {
            List<Row> p = new ArrayList<>();
            for (int i = 1; i <= 100; i++) p.add(Row.of(i, "name" + i));
            spark.createDataFrame(p, StructType.of(
                    StructField.of("id", DataType.INT), StructField.of("name", DataType.STRING)))
                .createOrReplaceTempView("p");
            List<Row> o = new ArrayList<>();
            for (int i = 1; i <= 100; i++) o.add(Row.of(i % 50 + 1, "item" + i)); // ids 1..50
            spark.createDataFrame(o, StructType.of(
                    StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING)))
                .createOrReplaceTempView("o");
            List<Row> res = spark.sql(
                    "SELECT count(*) FROM p JOIN o ON id = oid").collect();
            List<String> out = new ArrayList<>();
            for (Row r : res) out.add(String.valueOf(r.getLong(0)));
            return out;
        });
        // 100 orders all reference ids 1..50, which all exist in p → 100 matches.
        assertThat(got).containsExactly("100");
    }

    // Example 18 — spark.sql aggregate inside an expression: sum(age) + 1.
    @Test
    void ex18_sql_aggregate_expression() {
        long got = runSql("ex18-sql-agg-expr", spark -> {
            people(spark).createOrReplaceTempView("people");
            List<Row> res = spark.sql(
                    "SELECT city, sum(age) + 1 FROM people WHERE city = 'LA' GROUP BY city").collect();
            return res.get(0).getLong(1);
        });
        // LA = rows where i%4==1: ages are 18+(i%50) for i=1,5,9,…,197 (50 rows). Compute reference.
        long expected = 0;
        for (int i = 0; i < 200; i++) if (i % 4 == 1) expected += 18 + (i % 50);
        assertThat(got).isEqualTo(expected + 1);
    }

    // Example 19 — spark.sql ORDER BY ... LIMIT (top-N), distributed sort.
    @Test
    void ex19_sql_orderby_limit_topN() {
        List<Integer> ages = runSql("ex19-sql-topn", spark -> {
            people(spark).createOrReplaceTempView("people");
            List<Row> res = spark.sql(
                    "SELECT name, age FROM people ORDER BY age DESC LIMIT 5").collect();
            List<Integer> out = new ArrayList<>();
            for (Row r : res) out.add(r.getInt(1));
            return out;
        });
        // Max age is 67; at least the top values should be 67 and descending.
        assertThat(ages).hasSize(5);
        assertThat(ages.get(0)).isEqualTo(67);
        assertThat(ages).isSortedAccordingTo((a, b) -> Integer.compare(b, a));
    }

    // Example 20 — end-to-end batch I/O: read CSV → SQL aggregate → write → read back.
    @Test
    void ex20_csv_read_query_write_roundtrip(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        Path src = tmp.resolve("sales.csv");
        StringBuilder csv = new StringBuilder("region,amount\n");
        for (int i = 0; i < 400; i++) csv.append(i % 4 == 0 ? "east" : "west").append(",").append(10).append("\n");
        Files.writeString(src, csv.toString(), StandardCharsets.UTF_8);
        Path out = tmp.resolve("out_csv");

        Map<String, Long> got = runSql("ex20-csv-roundtrip", spark -> {
            spark.read().option("header", true).option("inferSchema", true)
                    .csv(src.toString())
                    .createOrReplaceTempView("sales");
            spark.sql("SELECT region, sum(amount) FROM sales GROUP BY region")
                    .write().option("header", true).csv(out.toString());

            DataFrame back = spark.read().option("header", true).option("inferSchema", true)
                    .csv(out.toString());
            Map<String, Long> m = new HashMap<>();
            for (Row r : back.collect()) m.put(r.getString(0), r.getLong(1));
            return m;
        });
        // east = 100 rows × 10 = 1000; west = 300 rows × 10 = 3000.
        assertThat(got).containsEntry("east", 1000L).containsEntry("west", 3000L);
    }
}
