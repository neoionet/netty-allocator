package io.github.neoionet.netty.microbenchmark.data;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.random.Well19937c;

/**
 * The class generates an array of sizes based on a log-normal distribution,
 * which is commonly used to model network traffic patterns.
 */
public class ApiGatewayPattern {
    public static final int[] FLATTENED_SIZE_ARRAY = new int[1 << 17];
    // Median ≈ 2 KiB; Mean ≈ 3.3 KiB; P95 ≈ 10KiB; P99 ≈ 20 KiB.
    private static final double scale = 7.6, shape = 1.0;
    static {
        LogNormalDistribution sizeDistribution = new LogNormalDistribution(new Well19937c(42L),
                scale, shape, LogNormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        for (int i = 0; i < FLATTENED_SIZE_ARRAY.length; i++) {
            long sampleSize = (long) sizeDistribution.sample();
            // The sizes are constrained to be between 8 bytes and 1 MiB.
            int size = (int) Math.max(8, Math.min(sampleSize, 1024 * 1024));
            FLATTENED_SIZE_ARRAY[i] = size;
        }
    }
}
