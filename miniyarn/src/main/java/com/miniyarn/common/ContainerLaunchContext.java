package com.miniyarn.common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Everything a NodeManager needs to spawn a container's process: the command
 * line and environment. The classpath is part of {@code command} so that
 * heterogeneous AMs (Spark vs other) can supply their own.
 *
 * Real YARN equivalent: org.apache.hadoop.yarn.api.records.ContainerLaunchContext
 */
public record ContainerLaunchContext(List<String> command, Map<String, String> environment)
        implements Serializable {}
