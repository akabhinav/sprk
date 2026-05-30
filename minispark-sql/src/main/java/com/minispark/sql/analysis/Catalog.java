package com.minispark.sql.analysis;

import com.minispark.sql.plan.LogicalPlan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Name → logical plan registry for temporary views. {@code df.createOrReplace
 * TempView("t")} puts a plan here; the analyzer swaps an {@link
 * com.minispark.sql.plan.UnresolvedRelation} for the registered plan.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.catalog.SessionCatalog
 *                        (the temp-view portion).
 */
public final class Catalog {

    private final Map<String, LogicalPlan> views = new ConcurrentHashMap<>();

    public void registerView(String name, LogicalPlan plan) {
        views.put(name.toLowerCase(), plan);
    }

    public LogicalPlan lookup(String name) {
        LogicalPlan p = views.get(name.toLowerCase());
        if (p == null) throw new Analyzer.AnalysisException("no such table or view: " + name);
        return p;
    }

    public boolean exists(String name) { return views.containsKey(name.toLowerCase()); }
}
