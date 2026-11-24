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
            <version>1.2.16.Final</version>
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
    *   **Allowed Values:** `0` to `1024` (values above `1024` are clamped to `1024` with a warning)
    *   **Description:** Sets how much memory of free segments each heap keeps for reuse, in MiB. The limit applies to **each heap**, so the memory kept can reach this value times the number of heaps. Any value above `0` keeps at least one segment; `0` keeps none.
    *   **Default value:** `1024`

* **`-Dio.github.neoionet.allocator.mimalloc.maxSharedHeaps`**
    *   **Description:** Sets the max shared heaps count (rounded up to a power of two).
    *   **Default value:** `NettyRuntime.availableProcessors() * 4`

* **`-Dio.github.neoionet.allocator.mimalloc.heap.idle.trim`**
    *   **Allowed Values(case-insensitive):** `true`, `false` (any other value fails with an `IllegalArgumentException`)
    *   **Description:** Sets whether cached memory is released while the heaps are idle. When enabled, about every 10 seconds the free segments cached by an idle heap are halved, until none are left:
    * * thread-local heaps are trimmed by a task on their own EventLoop, which stops once the heap has nothing more to release, so the EventLoop can still suspend.
    * * shared heaps are trimmed by a single JVM-wide daemon thread, `mi-shared-heaps-trim`, which is only started once a shared heap is used, skips the heaps that are busy, and exits about a minute after no allocator is left.
    * * When disabled, no task or thread is started, and cached memory is only released by later allocations of the same heap.
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

