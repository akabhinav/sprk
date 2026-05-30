package com.minispark.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stable identifier for a block stored in a {@link BlockManager}.
 *
 * <ul>
 *   <li>{@link ShuffleBlock} — one block per (shuffle, map task, reduce partition).</li>
 *   <li>{@link RDDBlock} — a cached RDD partition (one per (rddId, partitionIndex)).</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockId
 */
public abstract sealed class BlockId implements Serializable
        permits BlockId.ShuffleBlock, BlockId.RDDBlock {

    public abstract String name();

    @Override public final String toString() { return name(); }

    /** {@code shuffle_<shuffleId>_<mapId>_<reduceId>}. */
    public static final class ShuffleBlock extends BlockId {
        public final int shuffleId;
        public final int mapId;
        public final int reduceId;

        public ShuffleBlock(int shuffleId, int mapId, int reduceId) {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.reduceId = reduceId;
        }

        @Override public String name() {
            return "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId;
        }

        @Override public boolean equals(Object o) {
            return o instanceof ShuffleBlock b
                    && b.shuffleId == shuffleId && b.mapId == mapId && b.reduceId == reduceId;
        }
        @Override public int hashCode() { return Objects.hash(shuffleId, mapId, reduceId); }
    }

    /** {@code rdd_<rddId>_<partitionIndex>}. */
    public static final class RDDBlock extends BlockId {
        public final int rddId;
        public final int partitionIndex;

        public RDDBlock(int rddId, int partitionIndex) {
            this.rddId = rddId;
            this.partitionIndex = partitionIndex;
        }

        @Override public String name() { return "rdd_" + rddId + "_" + partitionIndex; }

        @Override public boolean equals(Object o) {
            return o instanceof RDDBlock b && b.rddId == rddId && b.partitionIndex == partitionIndex;
        }
        @Override public int hashCode() { return Objects.hash(rddId, partitionIndex); }
    }
}
