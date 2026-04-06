package io.github.neoionet.netty.microbenchmark.data;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.random.Well19937c;

/**
 * This class generates an array of sizes based on a log-normal distribution,
 * which is commonly used to model network traffic patterns.
 * The sizes are constrained to be between 8 bytes and 1 MiB.
 */
public class LogNormalDistributionPattern {

    public static final int[] FLATTENED_SIZE_ARRAY = new int[1 << 17];
    static {
        // TODO: consider using (7.0, 0.85), (6.9, 0.9)?
        LogNormalDistribution sizeDistribution = new LogNormalDistribution(new Well19937c(42L),
                7.5, 1.2, LogNormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        for (int i = 0; i < FLATTENED_SIZE_ARRAY.length; i++) {
            long sampleSize = (long) sizeDistribution.sample();
            // Range: 8 Byte - 1 MiB.
            int size = (int) Math.max(8, Math.min(sampleSize, 1024 * 1024));
            FLATTENED_SIZE_ARRAY[i] =size;
        }
    }
}
