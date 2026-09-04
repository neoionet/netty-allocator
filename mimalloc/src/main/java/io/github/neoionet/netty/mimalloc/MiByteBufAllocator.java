package io.github.neoionet.netty.mimalloc;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.UnstableApi;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.HEAP_STRATEGY_PROP_VALUE;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_VALUE;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.PAGE_SEARCH_STRATEGY_PROP_VALUE;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.SEGMENT_SIZE_PROP_VALUE_IN_BYTES;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.calculateMaxHeapWrapsLength;
import static io.github.neoionet.netty.mimalloc.MiMallocByteBufAllocator.calculateSegmentSizeInBytes;

/**
 * A Free-List {@link ByteBufAllocator} derived from `mimalloc`:
 * <a href="https://www.microsoft.com/en-us/research/wp-content/uploads/2019/06/mimalloc-tr-v1.pdf">mimalloc-paper</a>.
 * <a href="https://github.com/microsoft/mimalloc">mimalloc-code</a>
 * <p>
 *
 * <strong>Note:</strong> This allocator is <strong>experimental</strong>.
 * It is recommended to roll out usage slowly, and to carefully monitor application performance in the process.
 * <p>
 *
 * See the {@link MiMallocByteBufAllocator} class documentation for implementation details.
 */
@UnstableApi
public final class MiByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {

    private final MiMallocByteBufAllocator direct;
    private final MiMallocByteBufAllocator heap;

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the default settings.
     */
    public MiByteBufAllocator() {
        this(!PlatformDependent.isExplicitNoPreferDirect());
    }

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the specified preference for direct buffers.
     */
    public MiByteBufAllocator(boolean preferDirect) {
        super(preferDirect);
        final Builder builder = builder();
        direct = new MiMallocByteBufAllocator(new DirectChunkAllocator(this), builder);
        heap = new MiMallocByteBufAllocator(new HeapChunkAllocator(this), builder);
    }

    private MiByteBufAllocator(Builder builder) {
        super(builder.preferDirect);
        direct = new MiMallocByteBufAllocator(new DirectChunkAllocator(this), builder);
        heap = new MiMallocByteBufAllocator(new HeapChunkAllocator(this), builder);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(heap.allocate(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(direct.allocate(initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public long usedHeapMemory() {
        return heap.usedMemory();
    }

    @Override
    public long usedDirectMemory() {
        return direct.usedMemory();
    }

    @Override
    public ByteBufAllocatorMetric metric() {
        return this;
    }

    static final class HeapChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private HeapChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public UnpooledHeapByteBuf allocate(int initialCapacity, int maxCapacity) {
            return PlatformDependent.hasUnsafe() ?
                    new MiUnpooledUnsafeHeapByteBuf(allocator, initialCapacity, maxCapacity) :
                    new MiUnpooledHeapByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }

    static final class DirectChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private DirectChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public UnpooledDirectByteBuf allocate(int initialCapacity, int maxCapacity) {
            return MiByteBufUtil.newDirectByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }

    public static final class Builder {
        private boolean preferDirect = !PlatformDependent.isExplicitNoPreferDirect();
        int segmentSizeInBytes = SEGMENT_SIZE_PROP_VALUE_IN_BYTES;
        PageSearchStrategy pageSearchStrategy = PAGE_SEARCH_STRATEGY_PROP_VALUE;
        int maxSharedHeapWrapsLength = MAX_SHARED_HEAP_WRAPS_LENGTH_PROP_VALUE;
        HeapStrategy heapStrategy = HEAP_STRATEGY_PROP_VALUE;

        private Builder() {}

        public Builder preferDirect(boolean preferDirect) {
            this.preferDirect = preferDirect;
            return this;
        }

        public Builder segmentSizeInMiB(int segmentSizeInMiB) {
            this.segmentSizeInBytes = calculateSegmentSizeInBytes(segmentSizeInMiB);
            return this;
        }

        public Builder pageSearchStrategy(PageSearchStrategy pageSearchStrategy) {
            ObjectUtil.checkNotNull(pageSearchStrategy, "pageSearchStrategy");
            this.pageSearchStrategy = pageSearchStrategy;
            return this;
        }

        public Builder maxSharedHeapWrapsLength(int maxSharedHeapWrapsLength) {
            this.maxSharedHeapWrapsLength = calculateMaxHeapWrapsLength(maxSharedHeapWrapsLength);
            return this;
        }

        public Builder heapStrategy(HeapStrategy heapStrategy) {
            ObjectUtil.checkNotNull(heapStrategy, "heapStrategy");
            this.heapStrategy = heapStrategy;
            return this;
        }

        /**
         * @return A {@code MiByteBufAllocator} instance.
         */
        public MiByteBufAllocator build() {
            return new MiByteBufAllocator(this);
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

    /**
     * @return A default {@code Builder} instance.
     */
    public static Builder builder() {
        return new Builder();
    }
}
