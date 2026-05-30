package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/** GROUP BY ... agg(...) lowering to a shuffle + reduce on the engine. */
final class AggregateTest {

    private MiniSparkSession spark;

    private final StructType sales = StructType.of(
            StructField.of("region", DataType.STRING),
            StructField.of("product", DataType.STRING),
            StructField.of("amount", DataType.INT));

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("agg-test", "local[3]"); }
    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    private DataFrame salesDf() {
        return spark.createDataFrame(List.of(
                Row.of("east", "a", 10),
                Row.of("east", "b", 20),
                Row.of("west", "a", 30),
                Row.of("west", "b", 40),
                Row.of("east", "a", 5)
        ), sales);
    }

    @Test
    void group_by_with_sum_count_avg() {
        List<Row> rows = salesDf()
                .groupBy(col("region"))
                .agg(functions.sum(col("amount")),
                     functions.count(),
                     functions.avg(col("amount")))
                .collect();

        // Schema: region, sum(amount), count(*), avg(amount)
        Map<String, Long> sumByRegion = new HashMap<>();
        Map<String, Long> countByRegion = new HashMap<>();
        Map<String, Double> avgByRegion = new HashMap<>();
        for (Row r : rows) {
            sumByRegion.put(r.getString(0), r.getLong(1));
            countByRegion.put(r.getString(0), r.getLong(2));
            avgByRegion.put(r.getString(0), r.getDouble(3));
        }
        assertThat(sumByRegion).containsEntry("east", 35L).containsEntry("west", 70L);
        assertThat(countByRegion).containsEntry("east", 3L).containsEntry("west", 2L);
        assertThat(avgByRegion.get("east")).isCloseTo(35.0 / 3, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(avgByRegion.get("west")).isEqualTo(35.0);
    }

    @Test
    void group_by_multiple_columns_with_min_max() {
        List<Row> rows = salesDf()
                .groupBy(col("region"), col("product"))
                .agg(functions.min(col("amount")), functions.max(col("amount")))
                .collect();

        // east/a → min 5, max 10; east/b → 20,20; west/a → 30,30; west/b → 40,40
        assertThat(rows).hasSize(4);
        for (Row r : rows) {
            if (r.getString(0).equals("east") && r.getString(1).equals("a")) {
                assertThat(r.getInt(2)).isEqualTo(5);
                assertThat(r.getInt(3)).isEqualTo(10);
            }
        }
    }

    @Test
    void aggregate_schema_names_reflect_functions() {
        StructType s = salesDf().groupBy(col("region"))
                .agg(functions.sum(col("amount"))).schema();
        assertThat(s.names()).containsExactly("region", "sum(amount)");
        assertThat(s.type(1)).isEqualTo(DataType.LONG);
    }
}
