/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.github.neoionet.netty.microbenchmark.executor;

import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.ThreadExecutorMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is a modified portion of `io.netty.microbench.util.AbstractMicrobenchmark.HarnessExecutor`
 * from the <a href="https://github.com/netty/netty">netty</a> project.
 * <p>
 * A custom JMH executor which uses `FastThreadLocalThread`.
 */
public class FastThreadLocalThreadHarnessExecutor extends ThreadPoolExecutor {

    public FastThreadLocalThreadHarnessExecutor(int maxThreads, String prefix) {
        super(maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new DefaultThreadFactory(prefix));
        EventExecutor eventExecutor = new AbstractEventExecutor() {
            @Override
            public void shutdown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean inEventLoop(Thread thread) {
                return thread instanceof FastThreadLocalThread;
            }

            @Override
            public boolean isShuttingDown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<?> terminationFuture() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isShutdown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isTerminated() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(Runnable command) {
                throw new UnsupportedOperationException();
            }
        };
        setThreadFactory(ThreadExecutorMap.apply(getThreadFactory(), eventExecutor));
    }
}

