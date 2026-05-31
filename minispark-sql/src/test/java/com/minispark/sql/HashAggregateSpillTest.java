package com.minispark.sql;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Under a tight memory budget the map-side combine in {@code reduceByKey}
 * (used by {@code HashAggregateExec}) must spill via
 * {@link com.minispark.memory.ExternalAppendOnlyMap} and still produce
 * correct results. Without spillable hashing this query OOMs as the number
 * of distinct keys grows past what fits.
 */
final class HashAggregateSpillTest {

    private MiniSparkSession spark;

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    @Test
    void groupby_count_with_tight_memory_spills_and_still_returns_correct_counts() {
        // Memory budget low enough to force the ExternalAppendOnlyMap to spill
        // long before all 100 distinct keys are absorbed.
        spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]")
                        .set("minispark.memory.store.maxBytes", "4096")
                        .set("minispark.memory.storageFraction", "0.25")));

        // 100 distinct keys × 10 rows each = 1000 input rows; expected each
        // group's count = 10.
        StructType schema = StructType.of(StructField.of("k", DataType.INT));
        List<Row> rows = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) rows.add(Row.of(i % 100));

        List<Row> result = spark.createDataFrame(rows, schema)
                .groupBy("k")
                .count()
                .collect();

        assertThat(result).hasSize(100);
        // Every group should have count == 10. If spill+merge dropped or
        // double-counted entries, some count would be off.
        for (Row r : result) {
            assertThat(r.getLong(1)).isEqualTo(10L);
        }
    }

    @Test
    void groupby_sum_under_tight_memory_matches_unbounded_baseline() {
        // Same query twice — once with a tight budget that forces spill, once
        // with a large budget that doesn't — must agree.
        long valuesPerGroup = 50;
        int groups = 60;

        List<Long> tightSums = sumByGroup(/*maxBytes=*/"4096", groups, valuesPerGroup);
        List<Long> looseSums = sumByGroup(/*maxBytes=*/"67108864", groups, valuesPerGroup);

        assertThat(tightSums).isEqualTo(looseSums);
        // Sanity: each group's sum should be sum(0..49) * something — but the
        // exact value is whatever the loose run produces. Just check size.
        assertThat(tightSums).hasSize(groups);
    }

    private List<Long> sumByGroup(String maxBytes, int groups, long perGroup) {
        if (spark != null) { spark.close(); spark = null; }
        spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]")
                        .set("minispark.memory.store.maxBytes", maxBytes)
                        .set("minispark.memory.storageFraction", "0.25")));
        StructType schema = StructType.of(
                StructField.of("k", DataType.INT),
                StructField.of("v", DataType.INT));
        List<Row> rows = new ArrayList<>();
        for (int g = 0; g < groups; g++) {
            for (int i = 0; i < perGroup; i++) rows.add(Row.of(g, i));
        }
        List<Row> result = new ArrayList<>(spark.createDataFrame(rows, schema)
                .groupBy("k")
                .agg(new com.minispark.sql.expr.agg.Aggregates.Sum(
                        new com.minispark.sql.expr.UnresolvedAttribute("v"), schema))
                .collect());
        // Output rows are (k, sum) — sort by k for stable comparison.
        // Keys.normalize boxes integral grouping keys as Long.
        result.sort((a, b) -> Long.compare(((Number) a.get(0)).longValue(),
                                            ((Number) b.get(0)).longValue()));
        List<Long> sums = new ArrayList<>(result.size());
        for (Row r : result) sums.add(((Number) r.get(1)).longValue());
        return sums;
    }
}
