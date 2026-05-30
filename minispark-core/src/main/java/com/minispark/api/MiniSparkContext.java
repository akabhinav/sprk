package com.minispark.api;

import com.minispark.broadcast.Broadcast;
import com.minispark.broadcast.TorrentBroadcast;
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
import com.minispark.scheduler.cluster.YarnExecutorLauncher;
import com.minispark.serializer.JavaSerializer;
import com.minispark.serializer.Serializer;
import com.minispark.shuffle.ShuffleManager;
import com.minispark.shuffle.ShuffleManagerFactory;
import com.minispark.status.AppStatusStore;
import com.minispark.status.LiveListenerBus;
import com.minispark.ui.MiniSparkUI;
import com.minispark.storage.BlockManager;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.BlockId;
import com.minispark.storage.MapOutputTracker;
import com.minispark.storage.NetworkBlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final Pattern YARN_MASTER  = Pattern.compile("miniyarn://([^:]+):(\\d+)");

    private final MiniSparkConf conf;
    private final Serializer serializer;
    private final RpcEnv rpcEnv;
    private final BlockManager blockManager;
    private final MapOutputTracker mapOutputTracker;
    private final ShuffleManager shuffleManager;
    private final TaskScheduler taskScheduler;
    private final SchedulerBackend backend;
    private final ExecutorLauncher launcher;
    private final DAGScheduler dagScheduler;
    private final LiveListenerBus listenerBus;
    private final AppStatusStore statusStore;
    private final MiniSparkUI ui; // null unless minispark.ui.enabled=true
    private final int defaultParallelism;
    // Static so broadcast ids are unique across all contexts in a JVM. The
    // executor-side TorrentBroadcast cache is keyed by id, so a per-context
    // counter would let two contexts mint id=1 and collide in that cache.
    private static final AtomicLong BROADCAST_ID_GEN = new AtomicLong();

    public MiniSparkContext(MiniSparkConf conf) {
        this.conf = conf;
        this.serializer = new JavaSerializer();

        Matcher yarnMatch = YARN_MASTER.matcher(conf.master());
        boolean yarnMode = yarnMatch.matches();
        // Master URL implies transport: miniyarn:// forces netty (executors live in
        // other JVMs by definition); otherwise honour the explicit flag.
        String rpcMode = yarnMode ? "netty" : conf.get("minispark.rpc.mode", "local");
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
        String shuffleManagerName = conf.get("minispark.shuffle.manager", "hash");
        this.shuffleManager = ShuffleManagerFactory.create(
                shuffleManagerName, blockManager, mapOutputTracker, serializer);

        // The driver's SparkEnv. In local mode the in-process executors share it.
        SparkEnv.set(new SparkEnv(shuffleManager, blockManager, mapOutputTracker, serializer));

        // Event bus + status store feed the (optional) web UI. The scheduler and
        // backend post events; the store accumulates them; the UI renders the store.
        this.listenerBus = new LiveListenerBus();
        this.statusStore = new AppStatusStore();
        listenerBus.addListener(statusStore);

        this.taskScheduler = new TaskScheduler();

        int cores = parseLocalCores(conf.master());
        int executorInstances = conf.getInt("minispark.executor.instances", 1);
        int executorCores = conf.getInt("minispark.executor.cores", cores);
        // System properties every executor JVM must inherit to be compatible
        // with the driver. The shuffle wire format depends on this matching.
        java.util.Map<String, String> executorProps = new java.util.HashMap<>();
        executorProps.put("minispark.shuffle.manager", shuffleManagerName);

        ExecutorLauncher launcher0;
        int totalCores;
        int expectedExecutors;
        if (yarnMode) {
            // Executors come from a MiniYarn cluster: AM asks RM for containers,
            // NMs spawn the executor JVMs which dial back to the driver.
            int execMemoryMB = conf.getInt("minispark.executor.memoryMB", 512);
            launcher0 = new YarnExecutorLauncher(rpcEnv,
                    yarnMatch.group(1), Integer.parseInt(yarnMatch.group(2)),
                    conf.appName(), executorInstances, executorCores, execMemoryMB, executorProps);
            totalCores = executorInstances * executorCores;
            expectedExecutors = executorInstances;
        } else if (rpcMode.equals("netty")) {
            // Real separate-JVM executors, locally spawned (no cluster manager).
            launcher0 = new ProcessExecutorLauncher(executorInstances, executorCores, executorProps);
            totalCores = executorInstances * executorCores;
            expectedExecutors = executorInstances;
        } else {
            // Local mode: one in-process executor with `cores` slots (Spark's model).
            launcher0 = new LocalExecutorLauncher(rpcEnv, serializer, 1, cores);
            totalCores = cores;
            expectedExecutors = 1;
        }
        this.defaultParallelism = totalCores;
        this.launcher = launcher0;
        long heartbeatTimeoutMs = conf.getInt("minispark.executor.heartbeatTimeoutMs", 5000);
        this.backend = new CoarseGrainedSchedulerBackend(
                taskScheduler, rpcEnv, serializer, launcher, expectedExecutors, totalCores,
                heartbeatTimeoutMs, listenerBus);

        this.taskScheduler.setBackend(backend);
        this.backend.start();

        this.dagScheduler = new DAGScheduler(taskScheduler, mapOutputTracker, listenerBus);

        // Web UI: off by default (so tests don't bind ports); port 0 = ephemeral.
        if (conf.get("minispark.ui.enabled", "false").equalsIgnoreCase("true")) {
            String uiHost = conf.get("minispark.ui.host", "127.0.0.1");
            int uiPort = conf.getInt("minispark.ui.port", 4040);
            this.ui = new MiniSparkUI(conf.appName(), statusStore, uiHost, uiPort);
        } else {
            this.ui = null;
        }

        LOG.info("MiniSparkContext '{}' ready (master={}, rpc={}, parallelism={}, executors={})",
                conf.appName(), conf.master(), rpcMode, totalCores, expectedExecutors);
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
    public SchedulerBackend backend() { return backend; }
    public ExecutorLauncher launcher() { return launcher; }
    public AppStatusStore statusStore() { return statusStore; }
    /** The web UI's bound port, or -1 if the UI is disabled. */
    public int uiPort() { return ui == null ? -1 : ui.boundPort(); }
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

    /**
     * Publish {@code value} as a broadcast variable. The value is serialized
     * into the driver's BlockManager once; the returned handle is tiny and safe
     * to capture in task closures. Executors fetch-and-cache the value on first
     * access. {@code value} must be {@link Serializable}.
     */
    public <T extends Serializable> Broadcast<T> broadcast(T value) {
        long bid = BROADCAST_ID_GEN.incrementAndGet();
        blockManager.putBlock(new BlockId.BroadcastBlock(bid), serializer.serialize(value));
        LOG.info("Broadcast {} created ({} bytes on driver)", bid,
                serializer.serialize(value).length);
        return new TorrentBroadcast<>(bid, blockManager.location());
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
        if (ui != null) ui.stop();
        listenerBus.stop();
        LOG.info("MiniSparkContext '{}' stopped", conf.appName());
    }
}
