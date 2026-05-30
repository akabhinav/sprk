package com.miniyarn.common;

import java.io.Serializable;

/** A container's identity. Scoped to an application + monotonic id within it. */
public record ContainerId(ApplicationId appId, int id) implements Serializable {
    @Override public String toString() { return appId + "_container_" + id; }
}
