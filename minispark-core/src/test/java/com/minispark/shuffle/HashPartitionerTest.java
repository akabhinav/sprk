package com.minispark.shuffle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class HashPartitionerTest {

    @Test
    void distributes_into_range() {
        HashPartitioner p = new HashPartitioner(7);
        for (int i = -1000; i < 1000; i++) {
            int part = p.getPartition(i);
            assertThat(part).isBetween(0, 6);
        }
    }

    @Test
    void null_key_goes_to_zero() {
        assertThat(new HashPartitioner(4).getPartition(null)).isZero();
    }

    @Test
    void identical_keys_same_bucket() {
        HashPartitioner p = new HashPartitioner(16);
        assertThat(p.getPartition("hello")).isEqualTo(p.getPartition("hello"));
    }

    @Test
    void zero_partitions_rejected() {
        assertThatThrownBy(() -> new HashPartitioner(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
