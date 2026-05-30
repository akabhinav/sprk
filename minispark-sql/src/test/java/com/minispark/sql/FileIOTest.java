package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/** Batch read/write: CSV (header + inferSchema), JSON, and a full round-trip. */
final class FileIOTest {

    private MiniSparkSession spark;

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("io-test", "local[2]"); }
    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    @Test
    void csv_with_header_and_inferred_schema(@TempDir Path tmp) throws Exception {
        Path csv = tmp.resolve("people.csv");
        Files.writeString(csv,
                "name,age,score\nalice,30,9.5\nbob,25,8\ncarol,40,7.0\n", StandardCharsets.UTF_8);

        DataFrame df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(csv.toString());

        // Inferred: name STRING, age INT, score DOUBLE.
        StructType s = df.schema();
        assertThat(s.names()).containsExactly("name", "age", "score");
        assertThat(s.type(0)).isEqualTo(DataType.STRING);
        assertThat(s.type(1)).isEqualTo(DataType.INT);
        assertThat(s.type(2)).isEqualTo(DataType.DOUBLE);

        List<Row> over28 = df.filter(col("age").gt(28)).collect();
        assertThat(over28).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("alice", "carol");
    }

    @Test
    void csv_without_header_uses_default_column_names(@TempDir Path tmp) throws Exception {
        Path csv = tmp.resolve("nohdr.csv");
        Files.writeString(csv, "alice,30\nbob,25\n", StandardCharsets.UTF_8);

        DataFrame df = spark.read().option("inferSchema", true).csv(csv.toString());
        assertThat(df.schema().names()).containsExactly("c0", "c1");
        assertThat(df.count()).isEqualTo(2L);
    }

    @Test
    void explicit_schema_skips_inference(@TempDir Path tmp) throws Exception {
        Path csv = tmp.resolve("typed.csv");
        Files.writeString(csv, "x,y\n1,2\n3,4\n", StandardCharsets.UTF_8);
        StructType schema = StructType.of(
                StructField.of("x", DataType.LONG), StructField.of("y", DataType.LONG));

        DataFrame df = spark.read().option("header", true).schema(schema).csv(csv.toString());
        long sumX = 0;
        for (Row r : df.collect()) sumX += r.getLong(0);
        assertThat(sumX).isEqualTo(4L); // 1 + 3
    }

    @Test
    void json_lines_inferred(@TempDir Path tmp) throws Exception {
        Path json = tmp.resolve("people.json");
        Files.writeString(json,
                "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":25}\n",
                StandardCharsets.UTF_8);

        DataFrame df = spark.read().json(json.toString());
        assertThat(df.schema().names()).containsExactly("name", "age");
        assertThat(df.schema().type(1)).isEqualTo(DataType.INT);
        assertThat(df.count()).isEqualTo(2L);
    }

    @Test
    void csv_round_trip_read_query_write_read(@TempDir Path tmp) throws Exception {
        Path src = tmp.resolve("src.csv");
        Files.writeString(src,
                "region,amount\neast,10\neast,20\nwest,30\n", StandardCharsets.UTF_8);

        DataFrame df = spark.read().option("header", true).option("inferSchema", true)
                .csv(src.toString());
        df.createOrReplaceTempView("sales");

        DataFrame agg = spark.sql("SELECT region, sum(amount) FROM sales GROUP BY region");
        Path out = tmp.resolve("out_csv");
        agg.write().option("header", true).csv(out.toString());

        // Read the written CSV back and check the aggregates survived the round trip.
        DataFrame back = spark.read().option("header", true).option("inferSchema", true)
                .csv(out.toString());
        Map<String, Long> sums = new HashMap<>();
        for (Row r : back.collect()) sums.put(r.getString(0), r.getLong(1));
        assertThat(sums).containsEntry("east", 30L).containsEntry("west", 30L);
    }

    @Test
    void json_write_then_read_round_trip(@TempDir Path tmp) throws Exception {
        DataFrame df = spark.createDataFrame(List.of(
                Row.of("alice", 30), Row.of("bob", 25)),
                StructType.of(StructField.of("name", DataType.STRING),
                              StructField.of("age", DataType.INT)));
        Path out = tmp.resolve("out_json");
        df.write().json(out.toString());

        DataFrame back = spark.read().json(out.toString());
        Map<String, Integer> ages = new HashMap<>();
        for (Row r : back.collect()) ages.put(r.getString(back.schema().indexOf("name")),
                r.getInt(back.schema().indexOf("age")));
        assertThat(ages).containsEntry("alice", 30).containsEntry("bob", 25);
    }
}
