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
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Locale;

import static io.github.neoionet.netty.mimalloc.MiMallocOption.AllocType;
import static io.github.neoionet.netty.mimalloc.MiByteBufUtil.MiB;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.calculateMaxHeapWrapsLength;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.calculateSegmentSizeInBytes;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.PageSearchStrategy;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.HeapStrategy;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.getDefaultHeapStrategy;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.getDefaultMaxSharedHeapWrapsLength;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.getDefaultPageSearchStrategy;
import static io.github.neoionet.netty.mimalloc.MiMallocOption.getDefaultSegmentSizeInBytes;

/**
 * A Free-List {@link ByteBufAllocator} derived from `mimalloc`:
 * <a href="https://www.microsoft.com/en-us/research/wp-content/uploads/2019/06/mimalloc-tr-v1.pdf">mimalloc-paper</a>.
 * <a href="https://github.com/microsoft/mimalloc">mimalloc-code</a>
 * <p>
 *
 * <strong>Note:</strong> This allocator is <strong>experimental</strong>.
 * It is recommended to roll out usage slowly, and to carefully monitor application performance in the process.
 * <p>
 * See the {@link MiMallocByteBufAllocator} class documentation for implementation details.
 */
@UnstableApi
public final class MiByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MiByteBufAllocator.class);

    private final MiMallocByteBufAllocator direct;
    private final MiMallocByteBufAllocator heap;

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the default settings.
     */
    public MiByteBufAllocator() {
        this(defaultPreferDirect());
    }

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the specified preference for direct buffers.
     */
    public MiByteBufAllocator(boolean preferDirect) {
        super(preferDirect);
        final Builder builder = builder();
        direct = new MiMallocByteBufAllocator(new DirectChunkAllocator(this), builder, AllocType.DIRECT);
        heap = new MiMallocByteBufAllocator(new HeapChunkAllocator(this), builder, AllocType.HEAP);
    }

    private MiByteBufAllocator(boolean preferDirect, Builder builder) {
        super(preferDirect);
        direct = new MiMallocByteBufAllocator(new DirectChunkAllocator(this), builder, AllocType.DIRECT);
        heap = new MiMallocByteBufAllocator(new HeapChunkAllocator(this), builder, AllocType.HEAP);
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

    private static boolean defaultPreferDirect() {
        return !PlatformDependent.isExplicitNoPreferDirect();
    }

    private static final class HeapChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
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

    private static final class DirectChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
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
        int segmentSizeInBytes = getDefaultSegmentSizeInBytes();
        PageSearchStrategy pageSearchStrategy = getDefaultPageSearchStrategy();
        int maxSharedHeapWrapsLength = getDefaultMaxSharedHeapWrapsLength();
        HeapStrategy heapStrategy = getDefaultHeapStrategy();

        private Builder() {
            if (logger.isDebugEnabled()) {
                logger.debug("Default segmentSizeInBytes in MiB: {}, instance: {}",
                        segmentSizeInBytes / MiB, this);
                logger.debug("Default pageSearchStrategy: {}, instance: {}",
                        pageSearchStrategy.name().toLowerCase(Locale.ROOT), this);
                logger.debug("Default maxSharedHeapWrapsLength: {}, instance: {}",
                        maxSharedHeapWrapsLength, this);
                logger.debug("Default heapStrategy: {}, instance: {}",
                        heapStrategy.name().toLowerCase(Locale.ROOT), this);
            }
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
         * @return A {@code MiByteBufAllocator} instance, with specified {@code preferDirect}
         */
        public MiByteBufAllocator build(boolean preferDirect) {
            return new MiByteBufAllocator(preferDirect, this);
        }

        /**
         * @return A {@code MiByteBufAllocator} instance, with default {@code preferDirect}
         */
        public MiByteBufAllocator build() {
            return new MiByteBufAllocator(defaultPreferDirect(), this);
        }
    }

    /**
     * @return A default {@code Builder} instance.
     */
    public static Builder builder() {
        return new Builder();
    }
}
