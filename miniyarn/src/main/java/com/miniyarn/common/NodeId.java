package com.miniyarn.common;

import java.io.Serializable;

/** A NodeManager's identity. Real YARN: NodeId(host, port). */
public record NodeId(String name) implements Serializable {}
