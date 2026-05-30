package com.miniyarn.common;

import java.io.Serializable;

/**
 * The two-dimensional resource model YARN exposes: CPU cores and memory in MB.
 *
 * Real Spark/YARN equivalent: org.apache.hadoop.yarn.api.records.Resource
 */
public record Resource(int cores, int memoryMB) implements Serializable {
    public boolean fitsIn(Resource available) {
        return cores <= available.cores && memoryMB <= available.memoryMB;
    }
    public Resource minus(Resource other) {
        return new Resource(cores - other.cores, memoryMB - other.memoryMB);
    }
    public Resource plus(Resource other) {
        return new Resource(cores + other.cores, memoryMB + other.memoryMB);
    }
    public static Resource ZERO = new Resource(0, 0);
}
