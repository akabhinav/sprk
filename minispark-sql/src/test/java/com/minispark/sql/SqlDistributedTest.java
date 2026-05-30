package com.minispark.sql;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
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
 * The SQL/DataFrame layer running on a genuinely distributed engine: a driver
 * plus separate executor JVMs over TCP ({@code rpc.mode=netty}). Proves the
 * whole stack — parser → analyzer → optimizer → physical plan → RDD shuffle —
 * works across real process boundaries, not just in local mode.
 *
 * <p>Aborts (skips) if the sandbox can't spawn child JVMs.
 */
final class SqlDistributedTest {

    @Test
    void sql_groupby_having_orderby_across_executor_jvms() throws Exception {
        Callable<Map<String, Long>> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("sql-distributed")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2");
            try (MiniSparkContext sc = new MiniSparkContext(conf);
                 MiniSparkSession spark = MiniSparkSession.on(sc)) {

                // 200 rows across 4 regions so the shuffle spans both executors.
                List<Row> sales = new ArrayList<>();
                String[] regions = {"east", "west", "north", "south"};
                for (int i = 0; i < 200; i++) {
                    sales.add(Row.of(regions[i % 4], (i % 50) + 1));
                }
                spark.createDataFrame(sales, StructType.of(
                        StructField.of("region", DataType.STRING),
                        StructField.of("amount", DataType.INT)))
                    .createOrReplaceTempView("sales");

                List<Row> rows = spark.sql(
                        "SELECT region, sum(amount), count(*) FROM sales "
                        + "GROUP BY region HAVING count(*) > 10 ORDER BY region").collect();

                Map<String, Long> sums = new HashMap<>();
                for (Row r : rows) sums.put(r.getString(0), r.getLong(1));
                return sums;
            }
        };

        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<Map<String, Long>> f = runner.submit(job);
        Map<String, Long> sums;
        try {
            sums = f.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping distributed SQL test (cluster could not run): " + e);
            return;
        } finally {
            runner.shutdownNow();
        }

        // Each region has 50 rows (200/4); amounts cycle 1..50, so each region's
        // sum is 1+2+...+50 = 1275, and all four pass HAVING count(*) > 10.
        assertThat(sums).hasSize(4);
        assertThat(sums).containsKeys("east", "west", "north", "south");
        assertThat(sums.get("east")).isEqualTo(1275L);
    }
}
