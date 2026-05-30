package com.minispark.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Default {@link Serializer}: plain Java serialization. Slow but easy and
 * works on any {@link java.io.Serializable} closure. Phase 4 may add a
 * KryoSerializer.
 *
 * Real Spark equivalent: org.apache.spark.serializer.JavaSerializer
 */
public final class JavaSerializer implements Serializer {

    @Override
    public byte[] serialize(Object o) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("serialize failed", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("deserialize failed", e);
        }
    }
}
