package com.minispark.executor;

import com.minispark.memory.UnifiedMemoryManager;
import com.minispark.serializer.Serializer;
import com.minispark.shuffle.ShuffleManager;
import com.minispark.storage.BlockManager;
import com.minispark.storage.MapOutputTracker;

/**
 * The bundle of "things every executor needs" — shuffle manager, block
 * manager, map-output tracker, serializer.
 *
 * <p><b>Why a singleton.</b> Tasks must not carry these inside their
 * serialized payload, because (a) they are not portable across JVMs and (b)
 * each executor has its own concrete instances. Instead, when a task lands on
 * an executor it asks {@code SparkEnv.get()} for the local versions. The
 * driver sets up <i>its own</i> SparkEnv too, so driver-side scheduling code
 * can reach the same APIs.
 *
 * <p>In Phase 2 there is one JVM, so we use a plain {@code static} reference
 * (initialized when {@link com.minispark.api.MiniSparkContext} starts).
 * Phase 5 will spawn executor JVMs that initialize their own SparkEnv on
 * registration with the driver. The call sites do not change.
 *
 * Real Spark equivalent: org.apache.spark.SparkEnv
 */
public final class SparkEnv {

    private static volatile SparkEnv INSTANCE;

    public static void set(SparkEnv env) { INSTANCE = env; }
    public static SparkEnv get() {
        SparkEnv e = INSTANCE;
        if (e == null) throw new IllegalStateException("SparkEnv not initialized");
        return e;
    }

    private final ShuffleManager shuffleManager;
    private final BlockManager blockManager;
    private final MapOutputTracker mapOutputTracker;
    private final Serializer serializer;
    private final UnifiedMemoryManager memoryManager;

    public SparkEnv(ShuffleManager shuffleManager,
                    BlockManager blockManager,
                    MapOutputTracker mapOutputTracker,
                    Serializer serializer) {
        this(shuffleManager, blockManager, mapOutputTracker, serializer, null);
    }

    public SparkEnv(ShuffleManager shuffleManager,
                    BlockManager blockManager,
                    MapOutputTracker mapOutputTracker,
                    Serializer serializer,
                    UnifiedMemoryManager memoryManager) {
        this.shuffleManager = shuffleManager;
        this.blockManager = blockManager;
        this.mapOutputTracker = mapOutputTracker;
        this.serializer = serializer;
        this.memoryManager = memoryManager;
    }

    public ShuffleManager shuffleManager() { return shuffleManager; }
    public BlockManager blockManager() { return blockManager; }
    public MapOutputTracker mapOutputTracker() { return mapOutputTracker; }
    public Serializer serializer() { return serializer; }
    /** May be {@code null} in legacy / test setups that didn't wire one. */
    public UnifiedMemoryManager memoryManager() { return memoryManager; }
}
