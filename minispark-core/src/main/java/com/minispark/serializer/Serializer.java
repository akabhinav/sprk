package com.minispark.serializer;

/**
 * Object &lt;-&gt; bytes seam. Phase 1 uses {@link JavaSerializer}; later phases
 * may swap in Kryo or a custom wire format. All code that crosses the
 * driver/executor boundary should route through this interface.
 *
 * Real Spark equivalent: org.apache.spark.serializer.Serializer
 */
public interface Serializer {
    byte[] serialize(Object o);
    <T> T deserialize(byte[] bytes);
}
