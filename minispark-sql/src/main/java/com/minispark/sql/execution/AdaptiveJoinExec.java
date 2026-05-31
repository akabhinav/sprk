package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Runtime AQE wrapper for a non-broadcast join. Materialises both children to
 * cached RDDs first, learns each side's actual row count, and then — if a
 * side comes in below the demote threshold and the join type allows building
 * from that side — swaps the planned {@link ShuffledHashJoinExec} /
 * {@link SortMergeJoinExec} to {@link BroadcastHashJoinExec}.
 *
 * <p><b>Why this is "the AQE shape".</b> The compile-time planner sees only
 * static row counts on {@code LocalRelation}s. Anything that flows through a
 * filter, an aggregate, or a file source has no compile-time size — so
 * compile-time {@code df.broadcast()} either wastefully broadcasts a large
 * intermediate or misses an opportunity to demote a join whose intermediate
 * turns out small. This exec defers the decision to the moment the inputs
 * are actually materialised, exactly like real Spark's
 * {@code AdaptiveSparkPlanExec} → {@code DemoteBroadcastHashJoin} rule chain.
 *
 * <p><b>The cost.</b> Materialising both sides up-front means the work
 * happens before the join decision is taken; with caching the join itself
 * doesn't re-execute it. For a sub-tree the original plan would have
 * materialised anyway (a shuffled side), the only extra cost is the
 * {@code .count()} round-trip. For the broadcast-side win we save the entire
 * shuffle on the streaming side.
 *
 * <p><b>Gated by</b> {@code minispark.sql.adaptive.enabled=true}. Threshold:
 * {@code minispark.sql.adaptive.autoBroadcastJoinThreshold.rows} (default 1000).
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, DemoteBroadcastHashJoin}.
 */
public final class AdaptiveJoinExec implements PhysicalPlan {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJoinExec.class);

    /** Which non-broadcast strategy the compile-time planner picked. */
    public enum Fallback { SHUFFLED_HASH, SORT_MERGE }

    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;
    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Fallback fallback;
    private final long demoteThresholdRows;
    private final MiniSparkContext sc;
    private final int numPartitions; // used only by the SORT_MERGE fallback path

    public AdaptiveJoinExec(List<Expression> leftKeys, List<Expression> rightKeys,
                            JoinType joinType, StructType schema,
                            PhysicalPlan left, PhysicalPlan right,
                            Fallback fallback,
                            long demoteThresholdRows,
                            MiniSparkContext sc, int numPartitions) {
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinType = joinType;
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.fallback = fallback;
        this.demoteThresholdRows = demoteThresholdRows;
        this.sc = sc;
        this.numPartitions = numPartitions;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() {
        return "AdaptiveJoinExec(fallback=" + fallback + ", demoteThreshold=" + demoteThresholdRows + ")";
    }

    @Override
    public RDD<Row> execute() {
        // Materialise + cache so the join body re-reads from BlockManager,
        // not from the lineage. count() is the cheapest action that forces
        // materialisation and gives us the size signal AQE needs.
        RDD<Row> leftRdd = left.execute().cache();
        RDD<Row> rightRdd = right.execute().cache();
        long leftRows = leftRdd.count();
        long rightRows = rightRdd.count();

        PhysicalPlan leftMat = new MaterializedRDDScanExec(leftRdd, left.schema());
        PhysicalPlan rightMat = new MaterializedRDDScanExec(rightRdd, right.schema());

        // Demote if a side fits under the threshold AND the join type permits
        // building from that side. Prefer building from the right when both
        // sides are eligible (matches the planner's compile-time tiebreak).
        boolean canRight = BroadcastHashJoinExec.canBuildRight(joinType);
        boolean canLeft  = BroadcastHashJoinExec.canBuildLeft(joinType);

        if (canRight && rightRows <= demoteThresholdRows) {
            LOG.info("AQE: demoting {} join to BroadcastHashJoin (right={} rows ≤ {})",
                    fallback, rightRows, demoteThresholdRows);
            return new BroadcastHashJoinExec(leftKeys, rightKeys, joinType, schema,
                    leftMat, rightMat, /*buildIsLeft=*/false, sc).execute();
        }
        if (canLeft && leftRows <= demoteThresholdRows) {
            LOG.info("AQE: demoting {} join to BroadcastHashJoin (left={} rows ≤ {})",
                    fallback, leftRows, demoteThresholdRows);
            return new BroadcastHashJoinExec(leftKeys, rightKeys, joinType, schema,
                    leftMat, rightMat, /*buildIsLeft=*/true, sc).execute();
        }

        LOG.debug("AQE: keeping {} join (left={} rows, right={} rows, both > {})",
                fallback, leftRows, rightRows, demoteThresholdRows);
        return switch (fallback) {
            case SHUFFLED_HASH -> new ShuffledHashJoinExec(
                    leftKeys, rightKeys, joinType, schema, leftMat, rightMat).execute();
            case SORT_MERGE -> new SortMergeJoinExec(
                    leftKeys, rightKeys, joinType, schema, leftMat, rightMat, sc, numPartitions).execute();
        };
    }
}
