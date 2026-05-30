package com.minispark.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Untyped string-keyed configuration. Mirrors SparkConf so that later phases
 * can introduce keys like {@code spark.shuffle.manager} or
 * {@code spark.executor.cores} without rewriting call sites.
 *
 * Real Spark equivalent: org.apache.spark.SparkConf
 */
public final class MiniSparkConf {
    private final Map<String, String> entries = new HashMap<>();

    public MiniSparkConf set(String key, String value) {
        entries.put(key, value);
        return this;
    }

    public MiniSparkConf setAppName(String name) { return set("minispark.app.name", name); }
    public MiniSparkConf setMaster(String master) { return set("minispark.master", master); }

    public String appName() { return get("minispark.app.name", "minispark-app"); }
    public String master()  { return get("minispark.master", "local[*]"); }

    /**
     * Lookup order: explicit {@code set(...)} entries first, then JVM system
     * properties (so {@code -Dminispark.rpc.mode=netty} works without code
     * changes, mirroring how Spark reads {@code spark.*} system properties).
     */
    public Optional<String> getOption(String key) {
        String v = entries.get(key);
        if (v == null) v = System.getProperty(key);
        return Optional.ofNullable(v);
    }

    public String get(String key, String defaultValue) {
        return getOption(key).orElse(defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return getOption(key).map(Integer::parseInt).orElse(defaultValue);
    }
}
