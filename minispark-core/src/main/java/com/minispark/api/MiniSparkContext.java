package com.minispark.api;

import com.minispark.accumulator.Accumulator;
import com.minispark.accumulator.AccumulatorContext;
import com.minispark.accumulator.AccumulatorParam;
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
    private final com.minispark.scheduler.cluster.ExecutorAllocationManager allocationManager; // null unless enabled
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
        long maxMem = parseBytes(conf.get("minispark.memory.store.maxBytes", "536870912")); // 512 MB
        String localDir = conf.get("minispark.local.dir", null);
        this.blockManager = new NetworkBlockManager(driverLoc, rpcEnv, maxMem, localDir);
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

        // Scheduling mode across pools: FIFO (default) or FAIR.
        com.minispark.scheduler.pool.SchedulingMode schedMode =
                conf.get("minispark.scheduler.mode", "FIFO").equalsIgnoreCase("FAIR")
                        ? com.minispark.scheduler.pool.SchedulingMode.FAIR
                        : com.minispark.scheduler.pool.SchedulingMode.FIFO;
        this.taskScheduler = new TaskScheduler(4, schedMode);
        // Speculation: off by default (cheap to leave on, but adds extra
        // duplicate-task launches for slow tasks). When on, polls every
        // `intervalMs` and launches a duplicate of any task that has been
        // running > `multiplier` × the per-task median.
        if (conf.get("minispark.speculation", "false").equalsIgnoreCase("true")) {
            taskScheduler.setSpeculation(true,
                    conf.getInt("minispark.speculation.intervalMs", 500),
                    Double.parseDouble(conf.get("minispark.speculation.quantile", "0.75")),
                    Double.parseDouble(conf.get("minispark.speculation.multiplier", "1.5")));
        }

        int cores = parseLocalCores(conf.master());
        int executorInstances = conf.getInt("minispark.executor.instances", 1);
        int executorCores = conf.getInt("minispark.executor.cores", cores);
        // System properties every executor JVM must inherit to be compatible
        // with the driver. The shuffle wire format depends on this matching.
        java.util.Map<String, String> executorProps = new java.util.HashMap<>();
        executorProps.put("minispark.shuffle.manager", shuffleManagerName);
        executorProps.put("minispark.memory.store.maxBytes", String.valueOf(maxMem));
        if (localDir != null) executorProps.put("minispark.local.dir", localDir);

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

        this.dagScheduler = new DAGScheduler(taskScheduler, mapOutputTracker, listenerBus, conf);

        // Dynamic allocation: opt-in, and only meaningful when the launcher can
        // add/remove executors at runtime (netty/process or yarn modes).
        if (conf.get("minispark.dynamicAllocation.enabled", "false").equalsIgnoreCase("true")
                && backend instanceof CoarseGrainedSchedulerBackend cg
                && cg.launcher().supportsDynamicAllocation()) {
            int minE = conf.getInt("minispark.dynamicAllocation.minExecutors", 1);
            int maxE = conf.getInt("minispark.dynamicAllocation.maxExecutors", 10);
            long idleMs = conf.getInt("minispark.dynamicAllocation.executorIdleTimeoutMs", 60000);
            long pollMs = conf.getInt("minispark.dynamicAllocation.intervalMs", 1000);
            var policy = new com.minispark.scheduler.cluster.ExecutorAllocationPolicy(
                    minE, maxE, executorCores, idleMs);
            this.allocationManager =
                    new com.minispark.scheduler.cluster.ExecutorAllocationManager(cg, policy, pollMs);
            this.allocationManager.start();
        } else {
            this.allocationManager = null;
        }

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

    /** Accepts a raw byte count or a suffixed value like "256m", "1g", "512k". */
    static long parseBytes(String s) {
        s = s.trim().toLowerCase();
        long mult = 1;
        if (s.endsWith("k")) { mult = 1024; s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("m")) { mult = 1024L * 1024; s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("g")) { mult = 1024L * 1024 * 1024; s = s.substring(0, s.length() - 1); }
        return Long.parseLong(s.trim()) * mult;
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

    /**
     * Register a write-only accumulator. Tasks add to it; the driver reads via
     * {@link Accumulator#value()} after the action returns.
     */
    public <T> Accumulator<T> accumulator(T initialValue, String name, AccumulatorParam<T> param) {
        Accumulator<T> acc = new Accumulator<>(AccumulatorContext.nextId(), name, param);
        acc.mergeDelta(initialValue);
        AccumulatorContext.register(acc);
        return acc;
    }

    public Accumulator<Long> longAccumulator(String name) {
        return accumulator(0L, name, AccumulatorParam.LONG);
    }

    public Accumulator<Double> doubleAccumulator(String name) {
        return accumulator(0.0, name, AccumulatorParam.DOUBLE);
    }

    public RDD<String> textFile(String path) {
        return textFile(path, defaultParallelism);
    }

    public RDD<String> textFile(String path, int numPartitions) {
        return new TextFileRDD(this, path, numPartitions);
    }

    // ----- checkpointing -----

    private volatile String checkpointDir;
    private final java.util.List<RDD<?>> pendingCheckpoints =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Set the reliable-storage directory for {@code rdd.checkpoint()}. */
    public void setCheckpointDir(String dir) {
        this.checkpointDir = dir;
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of(dir));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Cannot create checkpoint dir " + dir, e);
        }
    }

    public String checkpointDir() { return checkpointDir; }

    public void registerForCheckpoint(RDD<?> rdd) { pendingCheckpoints.add(rdd); }

    /** After a job, flip RDDs whose partition files now all exist to "checkpointed". */
    private void finalizePendingCheckpoints() {
        for (RDD<?> rdd : pendingCheckpoints) {
            if (rdd.isCheckpointed()) { pendingCheckpoints.remove(rdd); continue; }
            String path = rdd.checkpointPath();
            if (path == null) { pendingCheckpoints.remove(rdd); continue; }
            int n = rdd.getPartitions().size();
            boolean allPresent = true;
            for (int i = 0; i < n; i++) {
                if (!java.nio.file.Files.exists(
                        java.nio.file.Path.of(path, String.format("part-%05d", i)))) {
                    allPresent = false; break;
                }
            }
            if (allPresent) {
                rdd.markCheckpointed();          // truncates lineage for future jobs
                pendingCheckpoints.remove(rdd);
                LOG.info("RDD {} checkpointed to {}", rdd.id(), path);
            }
        }
    }

    // ----- job entry point used by RDD actions -----

    public <T, U> List<U> runJob(RDD<T> rdd, ResultTask.ResultHandler<T, U> handler) {
        List<U> result = dagScheduler.runJob(rdd, handler, jobGroup.get());
        if (!pendingCheckpoints.isEmpty()) finalizePendingCheckpoints();
        return result;
    }

    // ----- job groups & cancellation -----

    // Group tag applied to jobs launched from the calling thread, so a UI button
    // or a timeout watcher can cancel a logical group of jobs at once.
    private final ThreadLocal<String> jobGroup = new ThreadLocal<>();

    /** Tag all jobs launched from this thread with {@code groupId}. */
    public void setJobGroup(String groupId) { jobGroup.set(groupId); }
    public void clearJobGroup() { jobGroup.remove(); }

    // ----- fair scheduler pools -----

    /**
     * Route jobs launched from this thread into the named scheduler pool. Under
     * FAIR mode, pools share resources by weight/minShare; a short interactive
     * job in its own pool isn't blocked behind a long batch job in another.
     * Mirrors Spark's {@code sc.setLocalProperty("spark.scheduler.pool", name)}.
     */
    public void setSchedulerPool(String poolName) { taskScheduler.setCurrentPool(poolName); }
    public void clearSchedulerPool() { taskScheduler.clearCurrentPool(); }

    /** Configure a pool's relative weight and guaranteed minimum slots. */
    public void configurePool(String poolName, int weight, int minShare) {
        taskScheduler.poolScheduler().configurePool(poolName, weight, minShare);
    }

    /** Cancel all in-flight jobs tagged with {@code groupId}. */
    public void cancelJobGroup(String groupId) { dagScheduler.cancelJobs(groupId); }

    /** Cancel every in-flight job. */
    public void cancelAllJobs() { dagScheduler.cancelJobs(null); }

    @Override
    public void close() {
        if (allocationManager != null) allocationManager.stop();
        backend.stop();
        taskScheduler.stop();
        if (ui != null) ui.stop();
        listenerBus.stop();
        LOG.info("MiniSparkContext '{}' stopped", conf.appName());
    }
}
