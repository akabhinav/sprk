package com.minispark.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stable identifier for a block stored in a {@link BlockManager}.
 *
 * <p>For Phase 2 the only kind we need is {@link ShuffleBlock} — one block
 * per (shuffle, map task, reduce partition) triple. Later phases add
 * RDDBlock (for {@code rdd.cache()}), BroadcastBlock, etc.
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockId
 */
public abstract sealed class BlockId implements Serializable permits BlockId.ShuffleBlock {

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
}
