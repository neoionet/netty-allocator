package io.github.neoionet.netty.mimalloc;

import io.netty.util.NettyRuntime;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
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

    private static final String HEAP_IDLE_TRIM_PROP_KEY = "io.github.neoionet.allocator.mimalloc.heap.idle.trim";

    private static final String MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.maxSharedHeaps";

    private static final String PAGE_SEARCH_STRATEGY_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.page.search.strategy";

    private static final String SEGMENT_SIZE_PROP_KEY = "io.github.neoionet.allocator.mimalloc.segment.size.mib";

    // Default segment size in MiB.
    private static final int DEFAULT_SEGMENT_SIZE_IN_MIB = 2;

    private static final int MAX_SEGMENT_SIZE_IN_MIB = 32;

    private static final String SEGMENT_CACHE_SIZE_PROP_KEY =
            "io.github.neoionet.allocator.mimalloc.segment.cache.size.mib";

    private static final long MAX_DEFAULT_SEGMENT_CACHE_SIZE_IN_BYTES = 1024L * MiB;

    // A quarter of the maximum direct memory, at most 1 GiB.
    private static final long DEFAULT_DIRECT_SEGMENT_CACHE_SIZE_IN_BYTES =
            defaultSegmentCacheSizeInBytes(PlatformDependent.maxDirectMemory(), 4);

    // An eighth of the maximum heap, at most 1 GiB: the cached segments are byte arrays that the GC cannot reclaim.
    private static final long DEFAULT_HEAP_SEGMENT_CACHE_SIZE_IN_BYTES =
            defaultSegmentCacheSizeInBytes(Runtime.getRuntime().maxMemory(), 8);

    // No lower bound: a floor would take a larger share of a small heap or direct memory than the fraction.
    // In bytes, not MiB, so that it is not rounded twice; the allocator rounds it down to whole segments.
    private static long defaultSegmentCacheSizeInBytes(long maxBytes, int divisor) {
        return maxBytes <= 0 ? 0 : Math.min(maxBytes / divisor, MAX_DEFAULT_SEGMENT_CACHE_SIZE_IN_BYTES);
    }

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

    // 0 keeps none: every segment that becomes empty is freed, and a new one is allocated (and zeroed) when needed.
    static long calculateSegmentCacheSizeInBytes(int segmentCacheSizeInMiB) {
        ObjectUtil.checkPositiveOrZero(segmentCacheSizeInMiB, "segmentCacheSizeInMiB");
        return ((long) segmentCacheSizeInMiB) * MiB;
    }

    private static long parseSegmentCacheSizeInBytes(String raw) {
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

    static long getDefaultSegmentCacheSizeInBytes(boolean isDirect) {
        String value = SystemPropertyUtil.get(SEGMENT_CACHE_SIZE_PROP_KEY);
        if (value == null) {
            return isDirect ? DEFAULT_DIRECT_SEGMENT_CACHE_SIZE_IN_BYTES : DEFAULT_HEAP_SEGMENT_CACHE_SIZE_IN_BYTES;
        }
        String trimmed = value.trim();
        return withPropertyContext(SEGMENT_CACHE_SIZE_PROP_KEY, trimmed, () -> parseSegmentCacheSizeInBytes(trimmed));
    }

    static boolean getDefaultHeapIdleTrim() {
        String value = SystemPropertyUtil.get(HEAP_IDLE_TRIM_PROP_KEY, String.valueOf(true)).trim();
        return withPropertyContext(HEAP_IDLE_TRIM_PROP_KEY, value, () -> parseBoolean(value));
    }

    // Only "true" or "false" (any case): `Boolean.parseBoolean` takes any other value as false, so a typo
    // would silently turn the option off.
    private static boolean parseBoolean(String raw) {
        if (String.valueOf(true).equalsIgnoreCase(raw)) {
            return true;
        }
        if (String.valueOf(false).equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("expected true or false");
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
         * Note: the heaps of threads that do not clean up their FastThreadLocals when they exit (threads other than
         *       FastThreadLocalThreads) are not cleaned up automatically when the threads exit; and the heaps of
         *       non-EventLoop threads are not trimmed while idle either (there is no EventLoop to run the idle
         *       trim on): their cached memory is only released by later allocations on the same thread.
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
