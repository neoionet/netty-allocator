# Netty mimalloc allocator
<a href="#"><img src="https://img.shields.io/badge/License-Apache%202.0-brightgreen.svg" alt="License"></a>
<a href="#"><img src="https://img.shields.io/badge/Java-1.8%2B-brightgreen.svg" alt="Java Version"></a>
<a href="#"><img src="https://img.shields.io/badge/Netty-4.2.10.Final%2B-brightgreen.svg" alt="Netty Version"></a>
<a href="#"><img src="https://img.shields.io/badge/VirtualThread-Friendly-brightgreen.svg" alt="Virtual Thread Friendly"></a>

A high-performance Java port of the **mimalloc** allocator, tailored for **Netty**.

---

## 🎯 Key Features

* **Mimalloc Powered**: Leverages mimalloc's advanced allocation strategies (free lists, local shards).
* **Tailored for Netty**: Specifically designed to handle Netty's `ByteBuf` allocation with minimal overhead.
* **High Throughput**: Optimized for multi-threaded network environments to ensure high and stable performance.
* **Virtual-Threads Friendly**: Optimized for Project Loom, preventing the memory explosion typically caused by massive `ThreadLocal` usage in virtual thread scenarios.

---

## 📮 Requirements

| Requirement | Minimum Version |
| :--- | :--- |
| **Java (JDK)** | **1.8** or higher |
| **Netty** | **4.2.10.Final** or newer |

---

## 📚 How to use

### 1. Maven dependencies:
Add the following dependencies to your `pom.xml`:

```xml
    <dependencies>
        <dependency>
            <groupId>io.github.neoionet</groupId>
            <artifactId>mimalloc</artifactId>
            <version>1.2.17.Final</version>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-common</artifactId>
            <version>4.2.19.Final</version>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-buffer</artifactId>
            <version>4.2.19.Final</version>
        </dependency>
    </dependencies>
```

If you want to use the mimalloc allocator within your server or client transport, ensure the `netty-transport` dependency is included in your project as well:
```xml
    <dependencies>
       <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-transport</artifactId>
            <version>4.2.19.Final</version>
        </dependency>
    </dependencies>
```

### 2. Quick start
#### 2.1. Initialize Allocator:
```java
// Create an instance of the mimalloc-based allocator.
ByteBufAllocator miMallocAllocator = new MiByteBufAllocator();
```

#### 2.2. Simple allocation:
```java
ByteBuf buf = miMallocAllocator.directBuffer();
buf.writeLong(123);
System.out.println(buf.readLong()); // print 123.
buf.release();
```

#### 2.3. Apply to Server:
```java
ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(NioServerSocketChannel.class)
 .option(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator.
 .childOption(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator for child.
 ...
```

#### 2.4. Apply to Client:
```java
Bootstrap b = new Bootstrap();
b.group(group)
 .channel(NioSocketChannel.class)
 .option(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator.
 ...
```

---

## 🚀 Benchmark

* **[Report](https://neoionet.github.io/netty-allocator-benchmark-report/1.2.0/benchmark.html)**
* **[Specification](https://neoionet.github.io/netty-allocator-benchmark-report/1.2.0/specification.html)**

---

## 🔧 Configuration
### 1. Global level configuration:
*   **`-Dio.github.neoionet.allocator.mimalloc.heap.strategy`**
    *   **Allowed Values(case-insensitive):** `auto`, `tl`, `shared`
    *   **Description:** Determines the heap strategy. 
    * * `auto`: **EventLoop threads use thread-local heaps, non-EventLoop threads use shared heaps**.
    * * `tl`: **Force all threads to use thread-local heaps. NOTE: If you forcefully use `tl` for non-EventLoop threads, their heaps will not be cleaned up automatically when the threads exit, and will not be trimmed while idle either (there is no EventLoop to run the idle trim on): their cached memory is only released by later allocations on the same thread**.
    * * `shared`: **Force all threads to use shared heaps**.
    *   **Default value:** `auto`

*   **`-Dio.github.neoionet.allocator.mimalloc.page.search.strategy`**
    *   **Allowed Values(case-insensitive):** `best`, `first`
    *   **Description:** Determines the page search strategy. 
    * * `best` stands for **best-fit**.
    * * `first` stands for **first-fit**.
    *   **Default value:** `best`

*   **`-Dio.github.neoionet.allocator.mimalloc.segment.size.mib`**
    *   **Allowed Values:** `1`, `2`, `4`, `8`, `16`, `32` (other values are rounded up to a power of two; values above `32` are clamped to `32` with a warning)
    *   **Description:** Sets the segment size in MiB.
    *   **Default value:** `2`

*   **`-Dio.github.neoionet.allocator.mimalloc.segment.cache.size.mib`**
    *   **Allowed Values:** `0` or more
    *   **Description:** Sets how much memory of free segments is kept for reuse, in MiB. The cache is **shared by all the heaps** of an allocator: a segment freed by one thread can be reused by any other. The direct and the heap allocator each have a cache of this size, rounded down to whole segments. A segment that becomes empty while the cache is full, or in the heap of a thread that exits, is freed instead. Every decay interval (about 10 seconds), up to half of the cached segments are freed, the oldest first, among those that stayed in the cache for a whole interval. When a new segment cannot be allocated for lack of memory, cached segments are freed first and the allocation is tried once more. Who runs the decay depends on `heap.idle.trim` (see below). `0` keeps none: every segment that becomes empty is freed at once, and a new one is allocated (and zeroed) when needed; use it only when memory matters more than allocation speed.
    *   **Default value:** a quarter of the maximum direct memory (`-XX:MaxDirectMemorySize`, or `-Dio.netty.maxDirectMemory` if set) for direct buffers, and an eighth of the maximum heap (`-Xmx`) for heap buffers, at most `1024` (1 GiB). Setting this property applies the same value to both.

* **`-Dio.github.neoionet.allocator.mimalloc.maxSharedHeaps`**
    *   **Description:** Sets the max shared heaps count (rounded up to a power of two).
    *   **Default value:** `NettyRuntime.availableProcessors() * 4`

* **`-Dio.github.neoionet.allocator.mimalloc.heap.idle.trim`**
    *   **Allowed Values(case-insensitive):** `true`, `false` (any other value fails with an `IllegalArgumentException`)
    *   **Description:** Sets whether cached memory is released while the allocator is idle. When enabled, about every 10 seconds the memory an idle heap caches is trimmed, and up to half of the free segments that stayed in the shared segment cache for a whole interval are freed, the oldest first:
    * * thread-local heaps of EventLoop threads are trimmed by a task on their own EventLoop, which stops once the heap has nothing more to release, so the EventLoop can still suspend.
    * * shared heaps are trimmed by a single JVM-wide daemon thread, `mi-shared-heaps-trim`, which is only started once the allocator allocates its first segment, skips the heaps that are busy, and exits about a minute after no allocator is left.
    * * the shared segment cache is trimmed by whichever comes first: an EventLoop's trim task, the daemon thread, or an allocation (a few segments at a time); one pass per interval for the whole allocator. The daemon thread makes sure it is trimmed even when no EventLoop is idle and nothing allocates.
    * * When disabled, no task or thread is started: the memory a heap caches is only released by later allocations of that heap; the shared segment cache by later allocations of any heap, a few segments at a time.
    *   **Default value:** `true`

### 2. Instance level configuration, which can override the global configuration:
```java
        ByteBufAllocator allocator = MiByteBufAllocator
                                         .builder()
                                         .heapStrategy(MiMallocOption.HeapStrategy.AUTO)
                                         .pageSearchStrategy(MiMallocOption.PageSearchStrategy.BEST)
                                         .segmentSizeInMiB(2)
                                         .segmentCacheSizeInMiB(1024)
                                         .maxSharedHeaps(16)
                                         .heapIdleTrim(true)
                                         .build();
```

## ⭐ Acknowledgments

This project would not be possible without the following open-source works:

* **[mimalloc](https://github.com/microsoft/mimalloc)** - A compact general purpose allocator with excellent performance.
* **[Netty](https://github.com/netty/netty)** - An event-driven asynchronous network application framework.

---

