package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL window functions via the DataFrame API: ROW_NUMBER, RANK, DENSE_RANK,
 * with PARTITION BY and ORDER BY.
 */
final class WindowFunctionTest {

    private MiniSparkSession spark;

    private final StructType schema = StructType.of(
            StructField.of("dept",   DataType.STRING),
            StructField.of("name",   DataType.STRING),
            StructField.of("salary", DataType.INT));

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("window-test", "local[3]"); }
    @AfterEach  void tearDown() { if (spark != null) spark.close(); }

    private DataFrame employees() {
        return spark.createDataFrame(List.of(
                Row.of("eng", "alice",   200),
                Row.of("eng", "bob",     150),
                Row.of("eng", "carol",   200),    // tie with alice
                Row.of("eng", "dan",     100),
                Row.of("sales", "eve",   180),
                Row.of("sales", "frank", 120)
        ), schema);
    }

    // ---------- ROW_NUMBER ----------

    @Test
    void row_number_partitioned_and_ordered_assigns_1_to_n_per_partition() {
        List<Row> rows = employees()
                .withColumn("rn", Window.rowNumber()
                        .over(Window.partitionBy("dept").orderBy("salary")))
                .collect();

        // 4 eng rows numbered 1..4 by ascending salary; 2 sales rows numbered 1..2.
        Map<String, List<Row>> byDept = groupByDept(rows);
        assertThat(byDept.get("eng")).hasSize(4);
        assertThat(byDept.get("sales")).hasSize(2);

        // Sort each dept by row_number and check ascending salary.
        for (Map.Entry<String, List<Row>> e : byDept.entrySet()) {
            List<Row> deptRows = e.getValue();
            deptRows.sort(Comparator.comparingLong(r -> r.getLong(3)));   // rn col = 3
            for (int i = 0; i < deptRows.size(); i++) {
                assertThat(deptRows.get(i).getLong(3)).isEqualTo(i + 1);
            }
            // Sorted by rn → salaries also non-decreasing.
            int prev = Integer.MIN_VALUE;
            for (Row r : deptRows) {
                assertThat(r.getInt(2)).isGreaterThanOrEqualTo(prev);
                prev = r.getInt(2);
            }
        }
    }

    // ---------- RANK (with gaps) ----------

    @Test
    void rank_assigns_same_value_to_ties_then_jumps_by_tie_group_size() {
        // ORDER BY salary DESC within eng: 200, 200, 150, 100 → ranks 1, 1, 3, 4.
        List<Row> rows = employees()
                .withColumn("r", Window.rank()
                        .over(Window.partitionBy("dept").orderByDesc("salary")))
                .collect();
        Map<String, Map<String, Long>> nameToRankByDept = nameToColumnByDept(rows);

        Map<String, Long> eng = nameToRankByDept.get("eng");
        // alice and carol tied at 200 — both rank 1, bob rank 3, dan rank 4. Gap after ties.
        long aliceR = eng.get("alice");
        long carolR = eng.get("carol");
        assertThat(aliceR).isEqualTo(1L);
        assertThat(carolR).isEqualTo(1L);
        assertThat(eng.get("bob")).isEqualTo(3L);   // skipped 2
        assertThat(eng.get("dan")).isEqualTo(4L);
    }

    // ---------- DENSE_RANK (no gaps) ----------

    @Test
    void dense_rank_assigns_same_value_to_ties_then_increments_by_one() {
        // Same input, dense version: 200, 200, 150, 100 → 1, 1, 2, 3.
        List<Row> rows = employees()
                .withColumn("dr", Window.denseRank()
                        .over(Window.partitionBy("dept").orderByDesc("salary")))
                .collect();
        Map<String, Long> eng = nameToColumnByDept(rows).get("eng");
        assertThat(eng.get("alice")).isEqualTo(1L);
        assertThat(eng.get("carol")).isEqualTo(1L);
        assertThat(eng.get("bob")).isEqualTo(2L);    // no skip!
        assertThat(eng.get("dan")).isEqualTo(3L);
    }

    // ---------- empty PARTITION BY: one global partition ----------

    @Test
    void row_number_without_partition_by_numbers_across_whole_dataset() {
        List<Row> rows = employees()
                .withColumn("rn", Window.rowNumber().over(Window.orderBy("salary")))
                .collect();
        // All 6 rows should appear with rn values 1..6 — exactly once each, no gaps.
        java.util.Set<Long> rns = new java.util.HashSet<>();
        for (Row r : rows) rns.add(r.getLong(3));
        assertThat(rns).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L);
        // And the row with the smallest salary (dan, 100) must have rn=1.
        Row danRow = rows.stream().filter(r -> "dan".equals(r.getString(1))).findFirst().orElseThrow();
        assertThat(danRow.getLong(3)).isEqualTo(1L);
    }

    // ---------- schema is correctly extended ----------

    @Test
    void output_schema_appends_window_column_with_long_type() {
        DataFrame df = employees().withColumn("rn",
                Window.rowNumber().over(Window.partitionBy("dept").orderBy("salary")));
        StructType out = df.schema();
        assertThat(out.size()).isEqualTo(4);
        assertThat(out.name(3)).isEqualTo("rn");
        assertThat(out.type(3)).isEqualTo(DataType.LONG);
        // Original columns survive in original order.
        assertThat(out.name(0)).isEqualTo("dept");
        assertThat(out.name(1)).isEqualTo("name");
        assertThat(out.name(2)).isEqualTo("salary");
    }

    // ---------- helpers ----------

    private static Map<String, List<Row>> groupByDept(List<Row> rows) {
        Map<String, List<Row>> out = new HashMap<>();
        for (Row r : rows) out.computeIfAbsent(r.getString(0), k -> new java.util.ArrayList<>()).add(r);
        return out;
    }

    /** Builds {dept → {name → window_col_value}}, reading the window column at index 3. */
    private static Map<String, Map<String, Long>> nameToColumnByDept(List<Row> rows) {
        Map<String, Map<String, Long>> out = new HashMap<>();
        for (Row r : rows) {
            out.computeIfAbsent(r.getString(0), k -> new HashMap<>())
                    .put(r.getString(1), r.getLong(3));
        }
        return out;
    }
}
