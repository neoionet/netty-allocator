package io.github.neoionet.netty.microbenchmark.data;

import java.util.SplittableRandom;

/**
 * Buffer sizes drawn from a uniform distribution over [1, 128 KiB): every size is equally likely, so most
 * requests are large. Median and mean are both about 64 KiB, and fewer than 1% of sizes are at most 1 KiB.
 */
public class UniformPattern {

    public static final int[] FLATTENED_SIZE_ARRAY = new int[1 << 17];

    private static final SplittableRandom rand = new SplittableRandom(42L);

    static {
        for (int i = 0; i < FLATTENED_SIZE_ARRAY.length; i++) {
            FLATTENED_SIZE_ARRAY[i] = rand.nextInt(1, 128 * 1024);
        }
    }
}
