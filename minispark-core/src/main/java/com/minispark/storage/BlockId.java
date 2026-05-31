package com.minispark.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stable identifier for a block stored in a {@link BlockManager}.
 *
 * <ul>
 *   <li>{@link ShuffleBlock} — hash-shuffle bucket: one per (shuffle, map, reduce).</li>
 *   <li>{@link ShuffleDataBlock} — sort-shuffle map output: one per (shuffle, map),
 *       holding every reduce partition's records (the reducer slices out its own).</li>
 *   <li>{@link RDDBlock} — a cached RDD partition (one per (rddId, partitionIndex)).</li>
 *   <li>{@link BroadcastBlock} — a broadcast variable's serialized value.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockId
 */
public abstract sealed class BlockId implements Serializable
        permits BlockId.ShuffleBlock, BlockId.ShuffleDataBlock, BlockId.RDDBlock, BlockId.BroadcastBlock, BlockId.SpillBlock {

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

    /** {@code shuffledata_<shuffleId>_<mapId>} — one consolidated file per map task. */
    public static final class ShuffleDataBlock extends BlockId {
        public final int shuffleId;
        public final int mapId;

        public ShuffleDataBlock(int shuffleId, int mapId) {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
        }

        @Override public String name() { return "shuffledata_" + shuffleId + "_" + mapId; }

        @Override public boolean equals(Object o) {
            return o instanceof ShuffleDataBlock b && b.shuffleId == shuffleId && b.mapId == mapId;
        }
        @Override public int hashCode() { return Objects.hash(shuffleId, mapId); }
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

    /**
     * {@code spill_<taskId>_<seq>} — a chunk dumped from an
     * {@link com.minispark.memory.ExternalAppendOnlyMap} (or similar
     * spillable consumer) when execution memory runs out. Always stored on
     * disk; never replicated; the consumer reads it back during merge.
     */
    public static final class SpillBlock extends BlockId {
        public final long taskAttemptId;
        public final int sequence;

        public SpillBlock(long taskAttemptId, int sequence) {
            this.taskAttemptId = taskAttemptId;
            this.sequence = sequence;
        }

        @Override public String name() { return "spill_" + taskAttemptId + "_" + sequence; }

        @Override public boolean equals(Object o) {
            return o instanceof SpillBlock b && b.taskAttemptId == taskAttemptId && b.sequence == sequence;
        }
        @Override public int hashCode() { return Objects.hash(taskAttemptId, sequence); }
    }

    /** {@code broadcast_<broadcastId>}. */
    public static final class BroadcastBlock extends BlockId {
        public final long broadcastId;

        public BroadcastBlock(long broadcastId) { this.broadcastId = broadcastId; }

        @Override public String name() { return "broadcast_" + broadcastId; }

        @Override public boolean equals(Object o) {
            return o instanceof BroadcastBlock b && b.broadcastId == broadcastId;
        }
        @Override public int hashCode() { return Long.hashCode(broadcastId); }
    }
}
