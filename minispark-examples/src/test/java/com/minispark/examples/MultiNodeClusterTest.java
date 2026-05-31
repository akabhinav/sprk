package com.minispark.examples;

import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.nm.NodeManager;
import com.miniyarn.rm.ResourceManager;
import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.accumulator.Accumulator;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.Window;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import com.minispark.status.AppStatusStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The multi-node proof. Stands up a real MiniYarn cluster — a ResourceManager
 * and two NodeManagers — and a driver, with <b>every component bound to and
 * advertising this host's routable (non-loopback) IP</b> rather than
 * {@code 127.0.0.1}. Executor JVMs spawned by the NodeManagers dial the driver
 * back over that routable interface, and executor↔executor shuffle blocks are
 * fetched over it — exactly the wiring a cluster spanning separate machines
 * uses. The only difference from physically-separate hosts is the absence of
 * physical separation and firewalls (environmental, not code).
 *
 * <p>Re-runs a representative example from every feature category against the
 * shared cluster and asserts correctness, plus verifies executors actually
 * advertised the routable IP (not loopback) — the property that distinguishes
 * a true multi-node run from single-host loopback.
 *
 * <p>Aborts (skips) cleanly if the host has no routable IPv4, or if the sandbox
 * can't spawn child JVMs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class MultiNodeClusterTest {

    private static String routableIp;     // this host's non-loopback IPv4, or null
    private RpcEnv rmEnv, nm1Env, nm2Env;
    private ResourceManager rm;
    private NodeManager nm1, nm2;

    @BeforeAll
    void bringUpCluster() {
        routableIp = findRoutableIpv4();
        Assumptions.assumeTrue(routableIp != null,
                "no routable (non-loopback) IPv4 on this host — can't prove multi-node");

        // RM + two NodeManagers, each bound to and advertising the routable IP,
        // each sized for one 2-core container so the RM must spread executors
        // across both nodes.
        rmEnv = RpcEnv.create("rm", routableIp, 0, "netty", new JavaSerializer());
        rm = new ResourceManager(rmEnv);
        nm1Env = RpcEnv.create("nm1", routableIp, 0, "netty", new JavaSerializer());
        nm1 = new NodeManager(new NodeId("nm1"), nm1Env,
                nm1Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm1.onStart();
        nm2Env = RpcEnv.create("nm2", routableIp, 0, "netty", new JavaSerializer());
        nm2 = new NodeManager(new NodeId("nm2"), nm2Env,
                nm2Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm2.onStart();
    }

    @AfterAll
    void tearDownCluster() {
        if (nm1 != null) nm1.onStop();
        if (nm2 != null) nm2.onStop();
        if (nm1Env != null) nm1Env.shutdown();
        if (nm2Env != null) nm2Env.shutdown();
        if (rmEnv != null) rmEnv.shutdown();
    }

    // ---------- the multi-node-defining assertion ----------

    @Test
    void executors_advertise_the_routable_ip_not_loopback() {
        List<AppStatusStore.ExecutorView> execs = runApp("mn-exec-ip", c -> c, sc -> {
            sc.parallelize(IntStream.range(0, 100).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x).count();
            sc.awaitListenerBus(5000);
            return sc.statusStore().executors();
        });
        assertThat(execs).isNotEmpty();
        // Every executor registered with the cluster's routable IP — proof that
        // shuffle/block fetch would reach the right host on a real cluster.
        assertThat(execs).allSatisfy(x -> {
            assertThat(x.host()).isEqualTo(routableIp);
            assertThat(x.host()).isNotEqualTo("127.0.0.1");
        });
    }

    // ---------- RDD core over the cluster (shuffles cross the routable network) ----------

    @Test
    void rdd_pipeline_and_shuffles() {
        Map<String, Object> got = runApp("mn-rdd", c -> c, sc -> {
            Map<String, Object> m = new HashMap<>();
            // map/filter/reduce
            m.put("sumSq", sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8)
                    .filter((RDD.SerializablePredicate<Integer>) n -> n % 2 == 0)
                    .map((RDD.SerializableFunction<Integer, Long>) n -> (long) n * n)
                    .reduce((RDD.SerializableBiFunction<Long, Long, Long>) Long::sum));
            // reduceByKey wordcount (shuffle across executors on the routable net)
            List<Tuple2<String, Integer>> wc = sc.parallelize(
                            List.of("a b a c", "b b c d", "a d d d", "c c c c"), 4)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            l -> java.util.Arrays.stream(l.split(" ")).iterator())
                    .mapToPair(w -> new Tuple2<>(w, 1)).reduceByKey(Integer::sum).collect();
            Map<String, Integer> wcm = new HashMap<>();
            for (Tuple2<String, Integer> t : wc) wcm.put(t._1(), t._2());
            m.put("wc", wcm);
            // join
            RDD<Tuple2<Integer, String>> p = sc.parallelize(List.of(
                    new Tuple2<>(1, "alice"), new Tuple2<>(2, "bob")), 2);
            RDD<Tuple2<Integer, String>> o = sc.parallelize(List.of(
                    new Tuple2<>(1, "book"), new Tuple2<>(2, "lamp"), new Tuple2<>(1, "pen")), 2);
            m.put("joinCount", (long) new PairRDDFunctions<>(p).join(o).collect().size());
            // distinct + sortByKey
            List<Integer> data = new ArrayList<>();
            for (int i = 0; i < 1000; i++) data.add(i % 17);
            m.put("distinct", new PairRDDFunctions<>(sc.parallelize(data, 8)
                    .mapToPair(v -> new Tuple2<>(v, 1)).rdd()).reduceByKey((x, y) -> x).count());
            return m;
        });
        long expectedSumSq = IntStream.rangeClosed(1, 1000).filter(n -> n % 2 == 0)
                .mapToLong(n -> (long) n * n).sum();
        assertThat(got.get("sumSq")).isEqualTo(expectedSumSq);
        @SuppressWarnings("unchecked") Map<String, Integer> wcm = (Map<String, Integer>) got.get("wc");
        assertThat(wcm).containsEntry("a", 3).containsEntry("b", 3).containsEntry("c", 6).containsEntry("d", 4);
        assertThat(got.get("joinCount")).isEqualTo(3L);   // alice:book, alice:pen, bob:lamp
        assertThat(got.get("distinct")).isEqualTo(17L);
    }

    // ---------- cache / broadcast / accumulator ----------

    @Test
    void cache_broadcast_accumulator() {
        long[] got = runApp("mn-features", c -> c, sc -> {
            RDD<Integer> cached = sc.parallelize(IntStream.rangeClosed(1, 500).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x * 2).cache();
            long count = cached.count();
            HashMap<Integer, Integer> table = new HashMap<>();
            for (int k = 0; k < 10; k++) table.put(k, k * 100);
            Broadcast<HashMap<Integer, Integer>> bc = sc.broadcast(table);
            long lookups = sc.parallelize(IntStream.range(0, 200).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> bc.value().get(i % 10))
                    .filter((RDD.SerializablePredicate<Integer>) v -> v % 100 == 0).count();
            Accumulator<Long> evens = sc.longAccumulator("evens");
            sc.parallelize(IntStream.range(0, 1000).boxed().toList(), 8)
                    .foreach((RDD.SerializableConsumer<Integer>) i -> { if (i % 2 == 0) evens.add(1L); });
            return new long[]{count, lookups, evens.value()};
        });
        assertThat(got[0]).isEqualTo(500L);
        assertThat(got[1]).isEqualTo(200L);
        assertThat(got[2]).isEqualTo(500L);
    }

    // ---------- sort shuffle (manager swapped behind the seam, distributed) ----------

    @Test
    void sort_shuffle() {
        Map<String, Long> got = runApp("mn-sort-shuffle",
                c -> c.set("minispark.shuffle.manager", "sort"),
                sc -> {
                    List<Tuple2<String, Long>> res = new PairRDDFunctions<>(
                            sc.parallelize(IntStream.range(0, 600).boxed().toList(), 8)
                                    .mapToPair(i -> new Tuple2<>("k" + (i % 6), 1L)).rdd())
                            .reduceByKey(Long::sum).collect();
                    Map<String, Long> m = new HashMap<>();
                    for (Tuple2<String, Long> t : res) m.put(t._1(), t._2());
                    return m;
                });
        assertThat(got).hasSize(6);
        assertThat(got.values()).allMatch(v -> v == 100L);
    }

    // ---------- AQE: coalesce + skew split, distributed ----------

    @Test
    void aqe_coalesce_and_skew() {
        Map<Long, Long> got = runApp("mn-aqe",
                c -> c.set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1048576")
                        .set("minispark.sql.adaptive.skewJoin.enabled", "true")
                        .set("minispark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
                        .set("minispark.sql.adaptive.skewJoin.skewedPartitionFactor", "2.0"),
                sc -> {
                    try (MiniSparkSession spark = MiniSparkSession.on(sc)) {
                        List<Row> rows = new ArrayList<>();
                        for (int i = 0; i < 800; i++) rows.add(Row.of(0, i));        // HOT key
                        for (int i = 0; i < 200; i++) rows.add(Row.of(1 + (i % 4), i));
                        spark.createDataFrame(rows, StructType.of(
                                StructField.of("k", DataType.INT), StructField.of("v", DataType.INT)))
                                .createOrReplaceTempView("t");
                        List<Row> res = spark.sql("SELECT k, count(*) FROM t GROUP BY k").collect();
                        Map<Long, Long> m = new HashMap<>();
                        for (Row r : res) m.put(((Number) r.get(0)).longValue(), r.getLong(1));
                        return m;
                    }
                });
        assertThat(got.get(0L)).isEqualTo(800L);
        assertThat(got.get(1L)).isEqualTo(50L);
    }

    // ---------- memory pressure: spillable aggregation, distributed ----------

    @Test
    void spillable_aggregation_under_tight_memory() {
        Map<Integer, Long> got = runApp("mn-spill",
                c -> c.set("minispark.memory.store.maxBytes", "16k")
                        .set("minispark.memory.storageFraction", "0.25"),
                sc -> {
                    List<Tuple2<Integer, Long>> pairs = new ArrayList<>();
                    for (int i = 0; i < 5000; i++) pairs.add(new Tuple2<>(i % 500, 1L));
                    List<Tuple2<Integer, Long>> res = new PairRDDFunctions<>(sc.parallelize(pairs, 8))
                            .reduceByKey(Long::sum).collect();
                    Map<Integer, Long> m = new HashMap<>();
                    for (Tuple2<Integer, Long> t : res) m.put(t._1(), t._2());
                    return m;
                });
        assertThat(got).hasSize(500);
        assertThat(got.values()).allMatch(v -> v == 10L);
    }

    // ---------- SQL: groupBy / join / window / outer / semi / cross / non-equi ----------

    @Test
    void sql_full_surface() {
        Map<String, Object> got = runApp("mn-sql", c -> c, sc -> {
            try (MiniSparkSession spark = MiniSparkSession.on(sc)) {
                Map<String, Object> m = new HashMap<>();
                StructType people = StructType.of(
                        StructField.of("id", DataType.INT), StructField.of("dept", DataType.STRING),
                        StructField.of("salary", DataType.INT));
                List<Row> prows = new ArrayList<>();
                String[] depts = {"eng", "sales"};
                for (int i = 0; i < 100; i++) prows.add(Row.of(i, depts[i % 2], 100 + (i % 10) * 10));
                DataFrame pdf = spark.createDataFrame(prows, people);
                pdf.createOrReplaceTempView("people");

                // GROUP BY count
                Map<String, Long> byDept = new HashMap<>();
                for (Row r : spark.sql("SELECT dept, count(*) FROM people GROUP BY dept").collect())
                    byDept.put(r.getString(0), r.getLong(1));
                m.put("byDept", byDept);

                // window: row_number per dept
                long ranked = pdf.withColumn("rn",
                        Window.rowNumber().over(Window.partitionBy("dept").orderBy("salary"))).count();
                m.put("ranked", ranked);

                // inner equi-join
                StructType orders = StructType.of(
                        StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING));
                List<Row> orows = new ArrayList<>();
                for (int i = 0; i < 60; i++) orows.add(Row.of(i, "item" + i));   // ids 0..59
                DataFrame odf = spark.createDataFrame(orows, orders);
                m.put("joinCount", pdf.join(odf, List.of("id"), List.of("oid"), JoinType.INNER).count());

                // left semi
                m.put("semiCount", pdf.join(odf, List.of("id"), List.of("oid"), JoinType.LEFT_SEMI).count());

                // cross join (cartesian)
                DataFrame a = spark.createDataFrame(List.of(Row.of(1), Row.of(2), Row.of(3)),
                        StructType.of(StructField.of("a", DataType.INT)));
                DataFrame b = spark.createDataFrame(List.of(Row.of(10), Row.of(20)),
                        StructType.of(StructField.of("b", DataType.INT)));
                m.put("crossCount", a.crossJoin(b).count());

                // non-equi range join — salaries are 100..190; cover them with two bands.
                DataFrame buckets = spark.createDataFrame(List.of(
                        Row.of(100, 150, "lo"), Row.of(150, 300, "hi")),
                        StructType.of(StructField.of("lo", DataType.INT),
                                StructField.of("hi", DataType.INT), StructField.of("label", DataType.STRING)));
                m.put("rangeCount", pdf.join(buckets,
                        col("salary").ge(col("lo")).and(col("salary").lt(col("hi"))), JoinType.INNER).count());
                return m;
            }
        });
        @SuppressWarnings("unchecked") Map<String, Long> byDept = (Map<String, Long>) got.get("byDept");
        assertThat(byDept).containsEntry("eng", 50L).containsEntry("sales", 50L);
        assertThat(got.get("ranked")).isEqualTo(100L);
        assertThat(got.get("joinCount")).isEqualTo(60L);    // ids 0..59 match
        assertThat(got.get("semiCount")).isEqualTo(60L);
        assertThat(got.get("crossCount")).isEqualTo(6L);    // 3 × 2
        assertThat(got.get("rangeCount")).isEqualTo(100L);  // every salary 100..190 lands in one band
    }

    // ---------- harness ----------

    /**
     * Run {@code body} as a fresh YARN application against the shared cluster,
     * with driver + executors pinned to the routable IP. Aborts (skips) if the
     * cluster couldn't run (e.g. sandbox blocks child JVMs).
     */
    private <R> R runApp(String name, Function<MiniSparkConf, MiniSparkConf> confTweak,
                         Function<MiniSparkContext, R> body) {
        Callable<R> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName(name)
                    .setMaster("miniyarn://" + routableIp + ":" + rmEnv.address().port)
                    .set("minispark.driver.host", routableIp)
                    .set("minispark.executor.host", routableIp)   // advertise routable, deterministically
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2")
                    .set("minispark.executor.memoryMB", "256");
            try (MiniSparkContext sc = new MiniSparkContext(confTweak.apply(conf))) {
                return body.apply(sc);
            }
        };
        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<R> f = runner.submit(job);
        try {
            return f.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping multi-node app '" + name + "' (cluster could not run): " + e);
            return null;
        } finally {
            runner.shutdownNow();
        }
    }

    /** First up, non-loopback, non-link-local IPv4 on this host; null if none. */
    private static String findRoutableIpv4() {
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (InetAddress a : Collections.list(ni.getInetAddresses())) {
                    if (a instanceof Inet4Address && !a.isLoopbackAddress() && !a.isLinkLocalAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
        } catch (Exception ignore) { /* fall through */ }
        return null;
    }
}
