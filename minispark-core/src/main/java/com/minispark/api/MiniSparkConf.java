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

    public String appName() { return entries.getOrDefault("minispark.app.name", "minispark-app"); }
    public String master()  { return entries.getOrDefault("minispark.master", "local[*]"); }

    public Optional<String> getOption(String key) { return Optional.ofNullable(entries.get(key)); }
    public String get(String key, String defaultValue) { return entries.getOrDefault(key, defaultValue); }
    public int getInt(String key, int defaultValue) {
        String v = entries.get(key);
        return v == null ? defaultValue : Integer.parseInt(v);
    }
}
