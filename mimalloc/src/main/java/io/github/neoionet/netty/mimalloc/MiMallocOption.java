package io.github.neoionet.netty.mimalloc;

import io.netty.util.NettyRuntime;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Locale;
import java.util.function.Supplier;

import static io.github.neoionet.netty.mimalloc.MiByteBufUtil.MiB;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.PageSearchStrategy.BEST;

public final class MiMallocOption {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MiMallocOption.class);

    private MiMallocOption() {}

    private static final String HEAP_STRATEGY_PROP_KEY = "io.github.neoionet.allocator.mimalloc.heap.strategy";

    private static final String MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.maxSharedHeaps";

    private static final String PAGE_SEARCH_STRATEGY_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.page.search.strategy";

    private static final String SEGMENT_SIZE_PROP_KEY = "io.github.neoionet.allocator.mimalloc.segment.size.mib";

    // Default segment size in MiB.
    private static final int DEFAULT_SEGMENT_SIZE_IN_MIB = 4;

    private static final int MAX_SEGMENT_SIZE_IN_MIB = 32;

    // Free segments kept per heap: rounded up to a power of two, at most 1024; at least one segment
    // is kept whatever the value. Raise it for live sets that swing by more than this per heap.
    private static final String SEGMENT_CACHE_SIZE_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.segment.cache.size.mib";

    private static final int DEFAULT_SEGMENT_CACHE_SIZE_IN_MIB = 128;

    private static final int MAX_SEGMENT_CACHE_SIZE_IN_MIB = 1024;

    // Allowed segment size: {1, 2, 4, 8, 16, 32} MiB.
    static int calculateSegmentSizeInBytes(int segmentSizeInMiB) {
        ObjectUtil.checkPositive(segmentSizeInMiB, "segmentSizeInMiB");
        int segmentMibNextPower2 = MathUtil.safeFindNextPositivePowerOfTwo(segmentSizeInMiB);
        if (segmentMibNextPower2 > MAX_SEGMENT_SIZE_IN_MIB) {
            logger.warn("segmentSizeInMiB {} exceeds the maximum, using {}",
                    segmentSizeInMiB, MAX_SEGMENT_SIZE_IN_MIB);
            segmentMibNextPower2 = MAX_SEGMENT_SIZE_IN_MIB;
        }
        return segmentMibNextPower2 * MiB;
    }

    private static int parseSegmentSizeInBytes(String raw) {
        return calculateSegmentSizeInBytes(Integer.parseInt(raw));
    }

    // Free segments kept for reuse per heap, in MiB: at most 1024 MiB;
    // at least one segment is kept whatever the value.
    static int calculateSegmentCacheSizeInBytes(int segmentCacheSizeInMiB) {
        ObjectUtil.checkPositive(segmentCacheSizeInMiB, "segmentCacheSizeInMiB");
        if (segmentCacheSizeInMiB > MAX_SEGMENT_CACHE_SIZE_IN_MIB) {
            logger.warn("segmentCacheSizeInMiB {} exceeds the maximum, using {}",
                    segmentCacheSizeInMiB, MAX_SEGMENT_CACHE_SIZE_IN_MIB);
            segmentCacheSizeInMiB = MAX_SEGMENT_CACHE_SIZE_IN_MIB;
        }
        return segmentCacheSizeInMiB * MiB;
    }

    private static int parseSegmentCacheSizeInBytes(String raw) {
        return calculateSegmentCacheSizeInBytes(Integer.parseInt(raw));
    }

    static int calculateMaxHeapWrapsLength(int maxSharedHeapWrapsLength) {
        ObjectUtil.checkPositive(maxSharedHeapWrapsLength, "maxSharedHeapWrapsLength");
        return MathUtil.safeFindNextPositivePowerOfTwo(maxSharedHeapWrapsLength);
    }

    private static int parseMaxHeapWrapsLength(String raw) {
        return calculateMaxHeapWrapsLength(Integer.parseInt(raw));
    }

    static HeapStrategy getDefaultHeapStrategy() {
        String value = SystemPropertyUtil.get(HEAP_STRATEGY_PROP_KEY, HeapStrategy.AUTO.name());
        return withPropertyContext(HEAP_STRATEGY_PROP_KEY, value,
                () -> HeapStrategy.valueOf(value.toUpperCase(Locale.ROOT)));
    }

    static PageSearchStrategy getDefaultPageSearchStrategy() {
        String value = SystemPropertyUtil.get(PAGE_SEARCH_STRATEGY_PROP_KEY, BEST.name());
        return withPropertyContext(PAGE_SEARCH_STRATEGY_PROP_KEY, value,
                () -> PageSearchStrategy.valueOf(value.toUpperCase(Locale.ROOT)));
    }

    static int getDefaultSegmentSizeInBytes() {
        String value = SystemPropertyUtil.get(SEGMENT_SIZE_PROP_KEY,
                String.valueOf(DEFAULT_SEGMENT_SIZE_IN_MIB)).trim();
        return withPropertyContext(SEGMENT_SIZE_PROP_KEY, value, () -> parseSegmentSizeInBytes(value));
    }

    static int getDefaultMaxSharedHeapWrapsLength() {
        // Use `NettyRuntime.availableProcessors() * 4` as the default max shared heaps length,
        // which exceed the common thread pool size (cores * 2), to leave reasonable capacity to expand.
        int maxLength = NettyRuntime.availableProcessors() * 4;
        String value = SystemPropertyUtil.get(MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY, String.valueOf(maxLength)).trim();
        return withPropertyContext(MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY, value, () -> parseMaxHeapWrapsLength(value));
    }

    static int getDefaultSegmentCacheSizeInBytes() {
        String value = SystemPropertyUtil.get(SEGMENT_CACHE_SIZE_PROP_KEY,
                String.valueOf(DEFAULT_SEGMENT_CACHE_SIZE_IN_MIB)).trim();
        return withPropertyContext(SEGMENT_CACHE_SIZE_PROP_KEY, value, () -> parseSegmentCacheSizeInBytes(value));
    }

    /**
     * Runs {@code supplier}, wrapping any {@link IllegalArgumentException} it throws with the
     * offending system property key/value,
     * including {@link NumberFormatException} thrown while parsing numeric property values,
     * since it is a subclass of {@link IllegalArgumentException}.
     * So the failure is traceable back to a specific -D flag.
     */
    private static <T> T withPropertyContext(String propKey, Object rawValue, Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for -D" + propKey + ": " + rawValue, e);
        }
    }

    public enum PageSearchStrategy {
        BEST, // best-fit
        FIRST // first-fit
    }

    public enum HeapStrategy {
        /**
         * Default: threads that clean up their FastThreadLocals when they exit (FastThreadLocalThreads, such as
         * the event loop threads) use thread-local heaps; other threads use shared heaps.
         */
        AUTO,
        /**
         * Force all threads to use thread-local heaps.
         * Note: If you forcefully use `tl` for non-eventLoop thread,
         *       it will not do automatically cleanup when the thread exits.
         */
        TL,
        /**
         * Force all threads to use shared heaps.
         */
        SHARED
    }

    enum AllocType {
        DIRECT,
        HEAP
    }
}
