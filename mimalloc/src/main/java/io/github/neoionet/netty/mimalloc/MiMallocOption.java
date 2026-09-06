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

    // Allowed segment size: {4, 8, 16, 32} MiB.
    static int calculateSegmentSizeInBytes(String segmentSizeInMiBVal) {
        int segmentSizeInMiB = Integer.parseInt(segmentSizeInMiBVal);
        ObjectUtil.checkPositive(segmentSizeInMiB, "segmentSizeInMiB");
        int segmentMibNextPower2 = MathUtil.safeFindNextPositivePowerOfTwo(segmentSizeInMiB);
        if (segmentMibNextPower2 < 4) {
            segmentMibNextPower2 = 4;
        }
        if (segmentMibNextPower2 > 32) {
            segmentMibNextPower2 = 32;
        }
        return 1 << (Integer.numberOfTrailingZeros(segmentMibNextPower2) + Integer.numberOfTrailingZeros(MiB));
    }

    static int calculateMaxHeapWrapsLength(String maxSharedHeapWrapsLengthVal) {
        int maxSharedHeapWrapsLength = Integer.parseInt(maxSharedHeapWrapsLengthVal);
        ObjectUtil.checkPositive(maxSharedHeapWrapsLength, "maxSharedHeapWrapsLength");
        return MathUtil.safeFindNextPositivePowerOfTwo(maxSharedHeapWrapsLength);
    }

    static HeapStrategy getDefaultHeapStrategy() {
        String value = SystemPropertyUtil.get(HEAP_STRATEGY_PROP_KEY, HeapStrategy.AUTO.name())
                .toUpperCase(Locale.ROOT);
        return withPropertyContext(HEAP_STRATEGY_PROP_KEY, value, () -> HeapStrategy.valueOf(value));
    }

    static PageSearchStrategy getDefaultPageSearchStrategy() {
        String value = SystemPropertyUtil.get(PAGE_SEARCH_STRATEGY_PROP_KEY, BEST.name())
                .toUpperCase(Locale.ROOT);
        return withPropertyContext(PAGE_SEARCH_STRATEGY_PROP_KEY, value, () -> PageSearchStrategy.valueOf(value));
    }

    static int getDefaultSegmentSizeInBytes() {
        // Default segment size: 4 MiB.
        String value = SystemPropertyUtil.get(SEGMENT_SIZE_PROP_KEY, "4").trim();
        return withPropertyContext(SEGMENT_SIZE_PROP_KEY, value, () -> calculateSegmentSizeInBytes(value));
    }

    static int getDefaultMaxSharedHeapWrapsLength() {
        // Use `NettyRuntime.availableProcessors() * 4` as the default max shared heaps length,
        // which exceed the common thread pool size (cores * 2), to leave reasonable capacity to expand.
        int maxLength = NettyRuntime.availableProcessors() * 4;
        String value = SystemPropertyUtil.get(MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY, String.valueOf(maxLength)).trim();
        return withPropertyContext(MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_KEY, value,
                () -> calculateMaxHeapWrapsLength(value));
    }

    /**
     * Runs {@code supplier}, wrapping any {@link IllegalArgumentException} it throws with the
     * offending system property key/value, so the failure is traceable back to a specific -D flag.
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
        AUTO,       // Default: EventLoop threads use thread-local heaps, non-EventLoop threads use shared heaps.
        TL,         // Force all threads to use thread-local heaps.
        SHARED      // Force all threads to use shared heaps.
    }

    enum AllocType {
        DIRECT,
        HEAP
    }
}
