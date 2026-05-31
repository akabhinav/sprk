package com.minispark.examples;

import com.minispark.accumulator.Accumulator;
import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.Window;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.minispark.sql.Column.col;

/**
 * Runs the <b>full example battery</b> — every feature category — against a
 * running MiniYarn cluster, asserting each result and printing {@code PASS}/{@code FAIL}.
 * Designed for the multi-node Docker harness: each check exercises real
 * cross-process (and, in containers, cross-host) serialization of closures,
 * broadcasts, shuffle records and results. File-I/O checks (saveAsTextFile,
 * checkpoint, CSV round-trip) use a shared directory that must be mounted on
 * every node — exactly the constraint a real cluster has.
 *
 * <pre>
 *   AllExamplesDriver &lt;rmHost&gt; &lt;rmPort&gt; &lt;driverHost&gt; [sharedDir=/shared]
 * </pre>
 *
 * Exits non-zero if any check fails, so a container/CI run surfaces failures.
 */
public final class AllExamplesDriver {

    private static String rm, dh, shared;
    private static int rp;
    private static int pass = 0, fail = 0;
    private static final List<String> failed = new ArrayList<>();

    private static MiniSparkConf base(String name) {
        return new MiniSparkConf().setAppName(name)
                .setMaster("miniyarn://" + rm + ":" + rp)
                .set("minispark.driver.host", dh)
                .set("minispark.executor.instances", "2")
                .set("minispark.executor.cores", "2")
                .set("minispark.executor.memoryMB", "256");
    }

    private static void check(String name, boolean ok) {
        if (ok) { pass++; System.out.println("PASS " + name); }
        else { fail++; failed.add(name); System.out.println("FAIL " + name); }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: AllExamplesDriver <rmHost> <rmPort> <driverHost> [sharedDir]");
            System.exit(2);
        }
        rm = args[0]; rp = Integer.parseInt(args[1]); dh = args[2];
        shared = args.length > 3 ? args[3] : "/shared";
        writeInputs();

        rddAndFeatures();
        sortShuffle();
        memoryAndDisk();
        spill();
        fairPool();
        aqe();
        sql();

        System.out.println("ALL_EXAMPLES pass=" + pass + " fail=" + fail
                + (failed.isEmpty() ? "" : " FAILED=" + failed));
        System.exit(fail == 0 ? 0 : 1);
    }

    /** Seed the shared volume with the inputs the file-I/O checks read. */
    private static void writeInputs() throws Exception {
        Files.createDirectories(Path.of(shared));
        Files.writeString(Path.of(shared, "book.txt"),
                "the quick brown fox\nthe lazy dog and the fox\nfox fox dog the the the\n",
                StandardCharsets.UTF_8);
        StringBuilder csv = new StringBuilder("region,amount\n");
        for (int i = 0; i < 400; i++) csv.append(i % 4 == 0 ? "east" : "west").append(",10\n");
        Files.writeString(Path.of(shared, "sales.csv"), csv.toString(), StandardCharsets.UTF_8);
    }

    // ---- RDD core + features + file I/O ----
    private static void rddAndFeatures() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-rdd"))) {
            check("ex01_sum_sq_evens", sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8)
                    .filter((RDD.SerializablePredicate<Integer>) n -> n % 2 == 0)
                    .map((RDD.SerializableFunction<Integer, Long>) n -> (long) n * n)
                    .reduce((RDD.SerializableBiFunction<Long, Long, Long>) Long::sum)
                    == IntStream.rangeClosed(1, 1000).filter(n -> n % 2 == 0).mapToLong(n -> (long) n * n).sum());
            check("ex02_flatmap_count", sc.parallelize(List.of("a b c", "d e"), 2)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            l -> Arrays.stream(l.split(" ")).iterator()).count() == 5);
            Map<String, Integer> wc = new HashMap<>();
            for (Tuple2<String, Integer> t : sc.parallelize(List.of("a b a", "b b c"), 2)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            l -> Arrays.stream(l.split(" ")).iterator())
                    .mapToPair(w -> new Tuple2<>(w, 1)).reduceByKey(Integer::sum).collect()) wc.put(t._1(), t._2());
            check("ex03_wordcount", wc.get("a") == 2 && wc.get("b") == 3 && wc.get("c") == 1);
            check("ex04_groupByKey", new PairRDDFunctions<>(sc.parallelize(IntStream.range(0, 100).boxed().toList(), 4)
                    .mapToPair(i -> new Tuple2<>(i % 2 == 0 ? "e" : "o", i)).rdd()).groupByKey().collect()
                    .stream().allMatch(t -> t._2().size() == 50));
            check("ex05_join", new PairRDDFunctions<>(sc.parallelize(List.of(new Tuple2<>(1, "a"), new Tuple2<>(2, "b")), 2))
                    .join(sc.parallelize(List.of(new Tuple2<>(1, "x"), new Tuple2<>(1, "y"), new Tuple2<>(2, "z")), 2)).collect().size() == 3);
            List<Integer> ks = new ArrayList<>();
            for (Tuple2<Integer, String> t : new PairRDDFunctions<>(sc.parallelize(IntStream.range(0, 50).boxed().toList(), 5)
                    .mapToPair(i -> new Tuple2<>((i * 37) % 50, "v")).rdd()).sortByKey().collect()) ks.add(t._1());
            List<Integer> sorted = new ArrayList<>(ks); Collections.sort(sorted);
            check("ex06_sortByKey", ks.equals(sorted));
            List<Integer> dd = new ArrayList<>(); for (int i = 0; i < 1000; i++) dd.add(i % 17);
            check("ex07_distinct", new PairRDDFunctions<>(sc.parallelize(dd, 8).mapToPair(v -> new Tuple2<>(v, 1)).rdd())
                    .reduceByKey((x, y) -> x).count() == 17);
            RDD<Integer> cached = sc.parallelize(IntStream.rangeClosed(1, 500).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x * 2).cache();
            check("ex08_cache", cached.count() == 500 && cached.count() == 500);
            HashMap<Integer, Integer> tbl = new HashMap<>(); for (int k = 0; k < 10; k++) tbl.put(k, k * 100);
            Broadcast<HashMap<Integer, Integer>> bc = sc.broadcast(tbl);
            check("ex10_broadcast", sc.parallelize(IntStream.range(0, 200).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> bc.value().get(i % 10))
                    .filter((RDD.SerializablePredicate<Integer>) v -> v % 100 == 0).count() == 200);
            Accumulator<Long> ac = sc.longAccumulator("ev");
            sc.parallelize(IntStream.range(0, 1000).boxed().toList(), 8)
                    .foreach((RDD.SerializableConsumer<Integer>) i -> { if (i % 2 == 0) ac.add(1L); });
            check("ex11_accumulator", ac.value() == 500);
            check("ex33_take_first", sc.parallelize(IntStream.rangeClosed(1, 100).boxed().toList(), 8).take(5).equals(List.of(1, 2, 3, 4, 5))
                    && sc.parallelize(IntStream.rangeClosed(1, 100).boxed().toList(), 8).first() == 1);
            check("ex34_takeOrdered", sc.parallelize(IntStream.range(0, 1000).boxed().toList(), 8)
                    .takeOrdered(3, (RDD.SerializableComparator<Integer>) (x, y) -> Integer.compare(y, x)).equals(List.of(999, 998, 997)));
            Map<String, String> cgm = new HashMap<>();
            for (var t : new PairRDDFunctions<>(sc.parallelize(List.of(new Tuple2<>("x", 1), new Tuple2<>("x", 2), new Tuple2<>("y", 3)), 2))
                    .cogroup(sc.parallelize(List.of(new Tuple2<>("x", 10), new Tuple2<>("z", 20)), 2)).collect()) {
                List<Integer> L = new ArrayList<>(t._2()._1()), R = new ArrayList<>(t._2()._2());
                Collections.sort(L); Collections.sort(R); cgm.put(t._1(), L + "|" + R);
            }
            check("ex37_cogroup", cgm.get("x").equals("[1, 2]|[10]") && cgm.get("y").equals("[3]|[]") && cgm.get("z").equals("[]|[20]"));
            // file I/O on the shared volume
            sc.parallelize(IntStream.rangeClosed(1, 200).boxed().toList(), 4)
                    .map((RDD.SerializableFunction<Integer, String>) i -> "n" + i).saveAsTextFile(shared + "/out_text");
            check("ex35_saveAsTextFile", sc.textFile(shared + "/out_text").count() == 200);
            sc.setCheckpointDir(shared + "/ckpt");
            RDD<Integer> der = sc.parallelize(IntStream.rangeClosed(1, 500).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x * 2)
                    .filter((RDD.SerializablePredicate<Integer>) x -> x % 4 == 0);
            der.checkpoint();
            check("ex36_checkpoint", der.count() == 250 && der.count() == 250);
        }
    }

    private static void sortShuffle() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-sort").set("minispark.shuffle.manager", "sort"))) {
            List<Tuple2<String, Long>> r = new PairRDDFunctions<>(sc.parallelize(IntStream.range(0, 600).boxed().toList(), 8)
                    .mapToPair(i -> new Tuple2<>("k" + (i % 6), 1L)).rdd()).reduceByKey(Long::sum).collect();
            check("ex12_sort_shuffle", r.size() == 6 && r.stream().allMatch(t -> t._2() == 100L));
        }
    }

    private static void memoryAndDisk() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-md").set("minispark.memory.store.maxBytes", "8k"))) {
            RDD<int[]> c = sc.parallelize(IntStream.range(0, 64).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, int[]>) i -> { int[] b = new int[1024]; Arrays.fill(b, i); return b; })
                    .persist(com.minispark.storage.StorageLevel.MEMORY_AND_DISK);
            c.count();
            check("ex09_mem_and_disk", c.count() == 64);
        }
    }

    private static void spill() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-spill")
                .set("minispark.memory.store.maxBytes", "16k").set("minispark.memory.storageFraction", "0.25"))) {
            List<Tuple2<Integer, Long>> pr = new ArrayList<>(); for (int i = 0; i < 5000; i++) pr.add(new Tuple2<>(i % 500, 1L));
            List<Tuple2<Integer, Long>> r = new PairRDDFunctions<>(sc.parallelize(pr, 8)).reduceByKey(Long::sum).collect();
            check("ex32_spill", r.size() == 500 && r.stream().allMatch(t -> t._2() == 10L));
        }
    }

    private static void fairPool() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-fair").set("minispark.scheduler.mode", "FAIR"))) {
            sc.configurePool("hi", 4, 2); sc.setSchedulerPool("hi");
            long s = sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Long>) Integer::longValue)
                    .reduce((RDD.SerializableBiFunction<Long, Long, Long>) Long::sum);
            sc.clearSchedulerPool();
            check("ex38_fair_pool", s == 500500L);
        }
    }

    private static void aqe() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-aqe")
                .set("minispark.sql.adaptive.enabled", "true")
                .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1048576")
                .set("minispark.sql.adaptive.skewJoin.enabled", "true")
                .set("minispark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
                .set("minispark.sql.adaptive.skewJoin.skewedPartitionFactor", "2.0"));
             MiniSparkSession sp = MiniSparkSession.on(sc)) {
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < 800; i++) rows.add(Row.of(0, i));
            for (int i = 0; i < 200; i++) rows.add(Row.of(1 + (i % 4), i));
            sp.createDataFrame(rows, StructType.of(StructField.of("k", DataType.INT), StructField.of("v", DataType.INT)))
                    .createOrReplaceTempView("t");
            Map<Long, Long> m = new HashMap<>();
            for (Row r : sp.sql("SELECT k, count(*) FROM t GROUP BY k").collect()) m.put(((Number) r.get(0)).longValue(), r.getLong(1));
            check("ex21_22_aqe_coalesce_skew", m.get(0L) == 800L && m.get(1L) == 50L);
        }
    }

    private static void sql() {
        try (MiniSparkContext sc = new MiniSparkContext(base("a-sql")); MiniSparkSession sp = MiniSparkSession.on(sc)) {
            StructType P = StructType.of(StructField.of("name", DataType.STRING),
                    StructField.of("age", DataType.INT), StructField.of("city", DataType.STRING));
            List<Row> pr = new ArrayList<>(); String[] cs = {"NYC", "LA", "SF", "NYC"};
            for (int i = 0; i < 200; i++) pr.add(Row.of("p" + i, 18 + (i % 50), cs[i % 4]));
            DataFrame pdf = sp.createDataFrame(pr, P); pdf.createOrReplaceTempView("people");
            check("ex14_df_filter_select", pdf.filter(col("age").ge(60)).select("name", "city").count() == 32);
            Map<String, Long> bc = new HashMap<>();
            for (Row r : sp.sql("SELECT city, count(*) FROM people GROUP BY city").collect()) bc.put(r.getString(0), r.getLong(1));
            check("ex15_sql_groupby", bc.get("NYC") == 100 && bc.get("LA") == 50 && bc.get("SF") == 50);
            List<Row> hv = sp.sql("SELECT city, count(*) FROM people GROUP BY city HAVING count(*) >= 100 ORDER BY city").collect();
            check("ex16_having_orderby", hv.size() == 1 && hv.get(0).getString(0).equals("NYC"));
            List<Row> tn = sp.sql("SELECT name, age FROM people ORDER BY age DESC LIMIT 5").collect();
            check("ex19_orderby_limit", tn.size() == 5 && tn.get(0).getInt(1) == 67);
            check("ex26_27_28_window",
                    pdf.withColumn("rn", Window.rowNumber().over(Window.partitionBy("city").orderBy("age"))).count() == 200
                            && pdf.withColumn("r", Window.rank().over(Window.partitionBy("city").orderByDesc("age"))).count() == 200
                            && pdf.withColumn("d", Window.denseRank().over(Window.partitionBy("city").orderBy("age"))).count() == 200);
            StructType O = StructType.of(StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING));
            DataFrame ids = sp.createDataFrame(IntStream.rangeClosed(1, 3).mapToObj(i -> Row.of(i, "n" + i)).toList(),
                    StructType.of(StructField.of("id", DataType.INT), StructField.of("nm", DataType.STRING)));
            DataFrame ords = sp.createDataFrame(List.of(Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp")), O);
            check("ex17_inner_join", ids.join(ords, List.of("id"), List.of("oid"), JoinType.INNER).count() == 3);
            check("ex29_left", ids.join(ords, List.of("id"), List.of("oid"), JoinType.LEFT).collect().stream().filter(r -> r.isNullAt(3)).count() == 1);
            check("ex30_right", ids.join(sp.createDataFrame(List.of(Row.of(1, "b"), Row.of(9, "o")), O),
                    List.of("id"), List.of("oid"), JoinType.RIGHT).collect().stream().filter(r -> r.isNullAt(0)).count() == 1);
            check("ex31_full", ids.join(sp.createDataFrame(List.of(Row.of(2, "l"), Row.of(9, "o")), O),
                    List.of("id"), List.of("oid"), JoinType.FULL).count() == 4);
            check("ex44_left_semi", ids.join(ords, List.of("id"), List.of("oid"), JoinType.LEFT_SEMI).count() == 2);
            DataFrame x = sp.createDataFrame(List.of(Row.of(1), Row.of(2), Row.of(3)), StructType.of(StructField.of("a", DataType.INT)));
            DataFrame y = sp.createDataFrame(List.of(Row.of(10), Row.of(20)), StructType.of(StructField.of("b", DataType.INT)));
            check("ex42_cross_join", x.crossJoin(y).count() == 6);
            DataFrame bk = sp.createDataFrame(List.of(Row.of(0, 50, "lo"), Row.of(50, 100, "hi")),
                    StructType.of(StructField.of("lo", DataType.INT), StructField.of("hi", DataType.INT), StructField.of("lab", DataType.STRING)));
            DataFrame pts = sp.createDataFrame(IntStream.range(0, 100).mapToObj(Row::of).toList(),
                    StructType.of(StructField.of("p", DataType.INT)));
            check("ex43_non_equi", pts.join(bk, col("p").ge(col("lo")).and(col("p").lt(col("hi"))), JoinType.INNER).count() == 100);
            // CSV round-trip on the shared volume
            sp.read().option("header", true).option("inferSchema", true).csv(shared + "/sales.csv").createOrReplaceTempView("sales");
            sp.sql("SELECT region, sum(amount) FROM sales GROUP BY region").write().option("header", true).csv(shared + "/out_csv");
            Map<String, Long> back = new HashMap<>();
            for (Row r : sp.read().option("header", true).option("inferSchema", true).csv(shared + "/out_csv").collect())
                back.put(r.getString(0), r.getLong(1));
            check("ex20_csv_roundtrip", back.get("east") == 1000L && back.get("west") == 3000L);
        }
    }

    private AllExamplesDriver() {}
}
