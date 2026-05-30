package com.minispark.api;

import com.minispark.executor.SparkEnv;
import com.minispark.rdd.ParallelCollectionRDD;
import com.minispark.rdd.RDD;
import com.minispark.rdd.TextFileRDD;
import com.minispark.rpc.RpcEnv;
import com.minispark.scheduler.DAGScheduler;
import com.minispark.scheduler.ResultTask;
import com.minispark.scheduler.SchedulerBackend;
import com.minispark.scheduler.TaskScheduler;
import com.minispark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import com.minispark.scheduler.cluster.ExecutorLauncher;
import com.minispark.scheduler.cluster.LocalExecutorLauncher;
import com.minispark.scheduler.cluster.ProcessExecutorLauncher;
import com.minispark.serializer.JavaSerializer;
import com.minispark.serializer.Serializer;
import com.minispark.shuffle.HashShuffleManager;
import com.minispark.shuffle.ShuffleManager;
import com.minispark.storage.BlockManager;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.MapOutputTracker;
import com.minispark.storage.NetworkBlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The user-facing entry point. Owns the scheduler stack, shuffle manager,
 * block manager, serializer, and the {@link RpcEnv} the driver listens on.
 *
 * <p><b>Transport selection.</b> {@code minispark.rpc.mode} chooses the seam:
 * <ul>
 *   <li>{@code local} (default) — {@link com.minispark.rpc.LocalRpcEnv} with
 *       in-process executors sharing this JVM's {@link SparkEnv}.</li>
 *   <li>{@code netty} — {@link com.minispark.rpc.NettyRpcEnv}; executors are
 *       spawned as separate JVMs that connect back over TCP. Set
 *       {@code minispark.executor.instances} and {@code minispark.executor.cores}.</li>
 * </ul>
 * The scheduler and DAG code are byte-for-byte identical across both.
 *
 * Real Spark equivalent: org.apache.spark.SparkContext
 */
public final class MiniSparkContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MiniSparkContext.class);
    private static final Pattern LOCAL_MASTER = Pattern.compile("local(?:\\[(\\*|\\d+)])?");

    private final MiniSparkConf conf;
    private final Serializer serializer;
    private final RpcEnv rpcEnv;
    private final BlockManager blockManager;
    private final MapOutputTracker mapOutputTracker;
    private final ShuffleManager shuffleManager;
    private final TaskScheduler taskScheduler;
    private final SchedulerBackend backend;
    private final DAGScheduler dagScheduler;
    private final int defaultParallelism;

    public MiniSparkContext(MiniSparkConf conf) {
        this.conf = conf;
        this.serializer = new JavaSerializer();

        String rpcMode = conf.get("minispark.rpc.mode", "local");
        String driverHost = conf.get("minispark.driver.host", "127.0.0.1");
        int driverPort = conf.getInt("minispark.driver.port", 0);
        this.rpcEnv = RpcEnv.create("driver", driverHost, driverPort, rpcMode, serializer);

        // The driver hosts the authoritative MapOutputTracker; executors (local or
        // remote) register their map outputs here and query it for reduce reads.
        this.mapOutputTracker = MapOutputTracker.master();
        rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, mapOutputTracker);

        // A NetworkBlockManager even in local mode: on LocalRpcEnv all fetches
        // resolve to local lookups, so it behaves like an in-memory store while
        // keeping a single code path with the distributed case.
        ExecutorLocation driverLoc = new ExecutorLocation(rpcEnv.address().host, rpcEnv.address().port);
        this.blockManager = new NetworkBlockManager(driverLoc, rpcEnv);
        this.shuffleManager = new HashShuffleManager(blockManager, mapOutputTracker, serializer);

        // The driver's SparkEnv. In local mode the in-process executors share it.
        SparkEnv.set(new SparkEnv(shuffleManager, blockManager, mapOutputTracker, serializer));

        this.taskScheduler = new TaskScheduler();

        int cores = parseLocalCores(conf.master());
        int executorInstances = conf.getInt("minispark.executor.instances", 1);
        int executorCores = conf.getInt("minispark.executor.cores", cores);

        ExecutorLauncher launcher;
        int totalCores;
        if (rpcMode.equals("netty")) {
            // Real separate-JVM executors.
            launcher = new ProcessExecutorLauncher(executorInstances, executorCores);
            totalCores = executorInstances * executorCores;
        } else {
            // Local mode: one in-process executor with `cores` slots (Spark's model).
            launcher = new LocalExecutorLauncher(rpcEnv, serializer, 1, cores);
            totalCores = cores;
        }
        this.defaultParallelism = totalCores;
        this.backend = new CoarseGrainedSchedulerBackend(
                taskScheduler, rpcEnv, serializer, launcher,
                rpcMode.equals("netty") ? executorInstances : 1, totalCores);

        this.taskScheduler.setBackend(backend);
        this.backend.start();

        this.dagScheduler = new DAGScheduler(taskScheduler, mapOutputTracker);
        LOG.info("MiniSparkContext '{}' ready (master={}, rpc={}, parallelism={})",
                conf.appName(), conf.master(), rpcMode, totalCores);
    }

    private static int parseLocalCores(String master) {
        Matcher m = LOCAL_MASTER.matcher(master);
        if (!m.matches()) {
            // miniyarn:// etc. comes in Phase 5; default to a sensible parallelism.
            return Runtime.getRuntime().availableProcessors();
        }
        String s = m.group(1);
        if (s == null) return 1;
        if (s.equals("*")) return Runtime.getRuntime().availableProcessors();
        return Integer.parseInt(s);
    }

    // ----- accessors used by RDDs / shuffle code -----

    public MiniSparkConf conf() { return conf; }
    public Serializer serializer() { return serializer; }
    public RpcEnv rpcEnv() { return rpcEnv; }
    public BlockManager blockManager() { return blockManager; }
    public MapOutputTracker mapOutputTracker() { return mapOutputTracker; }
    public ShuffleManager shuffleManager() { return shuffleManager; }
    public int defaultParallelism() { return defaultParallelism; }

    // ----- RDD constructors -----

    public <T> RDD<T> parallelize(List<T> data) {
        return parallelize(data, defaultParallelism);
    }

    public <T> RDD<T> parallelize(List<T> data, int numSlices) {
        return new ParallelCollectionRDD<>(this, data, numSlices);
    }

    public RDD<String> textFile(String path) {
        return textFile(path, defaultParallelism);
    }

    public RDD<String> textFile(String path, int numPartitions) {
        return new TextFileRDD(this, path, numPartitions);
    }

    // ----- job entry point used by RDD actions -----

    public <T, U> List<U> runJob(RDD<T> rdd, ResultTask.ResultHandler<T, U> handler) {
        return dagScheduler.runJob(rdd, handler);
    }

    @Override
    public void close() {
        backend.stop();
        LOG.info("MiniSparkContext '{}' stopped", conf.appName());
    }
}
