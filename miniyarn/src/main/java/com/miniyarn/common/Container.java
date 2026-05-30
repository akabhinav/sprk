package com.miniyarn.common;

import java.io.Serializable;

/**
 * An RM-allocated container: a slice of one node's resources, reserved for one
 * application. The AM still has to send {@link YarnMessages.LaunchContainer}
 * to the owning NM to actually start the process — matching real YARN, where
 * allocation and launch are separate steps.
 *
 * Real YARN equivalent: org.apache.hadoop.yarn.api.records.Container
 */
public record Container(ContainerId id, NodeId nodeId, String nodeHost, int nodePort,
                        Resource resource) implements Serializable {}
