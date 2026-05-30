package com.minispark.scheduler;

import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffleDependency;

import java.util.List;

/**
 * An intermediate stage whose tasks write shuffle output. The downstream
 * stage(s) consume that output via the {@link com.minispark.storage.MapOutputTracker}.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ShuffleMapStage
 */
public final class ShuffleMapStage extends Stage {
    private final ShuffleDependency<?, ?> shuffleDep;

    public ShuffleMapStage(int id, RDD<?> rdd, List<Stage> parents,
                           ShuffleDependency<?, ?> shuffleDep) {
        super(id, rdd, parents);
        this.shuffleDep = shuffleDep;
    }

    public ShuffleDependency<?, ?> shuffleDep() { return shuffleDep; }
}
