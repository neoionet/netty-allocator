package io.github.neoionet.netty.microbenchmark.data;

import java.util.SplittableRandom;

/**
 * Buffer sizes drawn from a log-uniform distribution over [1, 128 KiB): the power-of-two ranges
 * [1, 2), [2, 4), ..., [64 KiB, 128 KiB) are equally likely, so small sizes are far more frequent
 * than large ones. Median about 362 bytes, mean about 11 KiB.
 */
public class LogUniformSizePattern {

    public static final int[] FLATTENED_SIZE_ARRAY = new int[1 << 17];

    private static final SplittableRandom rand = new SplittableRandom(42L);

    static {
        double lo = Math.log(1), hi = Math.log(128 * 1024);
        for (int i = 0; i < FLATTENED_SIZE_ARRAY.length; i++) {
            FLATTENED_SIZE_ARRAY[i] = (int) Math.exp(lo + rand.nextDouble() * (hi - lo));
        }
    }
}
