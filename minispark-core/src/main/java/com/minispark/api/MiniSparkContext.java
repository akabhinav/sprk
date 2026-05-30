package com.minispark.api;

import com.minispark.executor.SparkEnv;
import com.minispark.rdd.ParallelCollectionRDD;
import com.minispark.rdd.RDD;
import com.minispark.rdd.TextFileRDD;
import com.minispark.scheduler.DAGScheduler;
import com.minispark.scheduler.LocalSchedulerBackend;
import com.minispark.scheduler.ResultTask;
import com.minispark.scheduler.SchedulerBackend;
import com.minispark.scheduler.TaskScheduler;
import com.minispark.serializer.JavaSerializer;
import com.minispark.serializer.Serializer;
import com.minispark.shuffle.HashShuffleManager;
import com.minispark.shuffle.ShuffleManager;
import com.minispark.storage.BlockManager;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.InMemoryBlockManager;
import com.minispark.storage.MapOutputTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The user-facing entry point. Owns the scheduler stack, shuffle manager,
 * block manager, and serializer; hands out RDDs.
 *
 * Real Spark equivalent: org.apache.spark.SparkContext
 */
public final class MiniSparkContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MiniSparkContext.class);
    private static final Pattern LOCAL_MASTER = Pattern.compile("local(?:\\[(\\*|\\d+)])?");

    private final MiniSparkConf conf;
    private final Serializer serializer;
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
        this.blockManager = new InMemoryBlockManager(ExecutorLocation.LOCAL);
        this.mapOutputTracker = new MapOutputTracker();
        this.shuffleManager = new HashShuffleManager(blockManager, mapOutputTracker, serializer);

        // Phase 2: one JVM, so the driver and the (single) executor share a
        // SparkEnv. In Phase 5 the executor JVMs build their own.
        SparkEnv.set(new SparkEnv(shuffleManager, blockManager, mapOutputTracker, serializer));

        this.taskScheduler = new TaskScheduler();

        int cores = parseLocalCores(conf.master());
        this.defaultParallelism = cores;
        this.backend = new LocalSchedulerBackend(taskScheduler, serializer, cores);

        this.taskScheduler.setBackend(backend);
        this.backend.start();

        this.dagScheduler = new DAGScheduler(taskScheduler);
        LOG.info("MiniSparkContext '{}' ready (master={}, parallelism={})",
                conf.appName(), conf.master(), cores);
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
