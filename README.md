# Netty mimalloc allocator

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-1.8%2B-orange.svg)](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html)
[![Netty Version](https://img.shields.io/badge/Netty-4.2.10.Final%2B-green.svg)](https://netty.io/)

A high-performance Java port of the **mimalloc** allocator, tailored for **Netty**.

---

## 🚀 Key Features

* **Mimalloc Powered**: Leverages mimalloc's advanced allocation strategies (free lists, local shards).
* **Tailored for Netty**: Specifically designed to handle Netty's `ByteBuf` allocation with minimal overhead.
* **High Throughput**: Optimized for multi-threaded network environments to ensure high and stable performance.

---

## 🛠 Requirements

| Requirement | Minimum Version |
| :--- | :--- |
| **Java (JDK)** | **1.8** or higher |
| **Netty** | **4.2.10.Final** or newer |

---

## 📖 How to use

### 1. Maven dependencies
Add the following dependencies to your `pom.xml`:

```xml
    <dependencies>
        <dependency>
            <groupId>io.github.neoionet</groupId>
            <artifactId>netty-allocator</artifactId>
            <version>1.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-common</artifactId>
            <scope>4.2.10.Final</scope>
        </dependency>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-buffer</artifactId>
            <scope>4.2.10.Final</scope>
        </dependency>
    </dependencies>
```

If you want to use the mimalloc allocator within your server or client transport, ensure the `netty-transport` dependency is included in your project as well:
```xml
    <dependencies>
       <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-transport</artifactId>
            <version>4.2.10.Final</version>
        </dependency>
    </dependencies>
```

### 2. Quick start
#### 2.1. Initialize Allocator
```java
// Create an instance of the mimalloc-based allocator.
ByteBufAllocator miMallocAllocator = new MiByteBufAllocator();
```

#### 2.2. Apply to Server
```java
ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(NioServerSocketChannel.class)
 .option(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator.
 .childOption(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator for child.
 ...
```

#### 2.3. Apply to Client
```java
Bootstrap b = new Bootstrap();
b.group(group)
 .channel(NioSocketChannel.class)
 .option(ChannelOption.ALLOCATOR, miMallocAllocator) // Set the mimalloc allocator.
 ...
```

---

## 🌟 Acknowledgments

This project would not be possible without the following open-source works:

* **[mimalloc](https://github.com/microsoft/mimalloc)** - A compact general purpose allocator with excellent performance.
* **[Netty](https://github.com/netty/netty)** - An event-driven asynchronous network application framework.

---

