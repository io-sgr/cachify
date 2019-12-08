/*
 * Copyright 2017-2019 SgrAlpha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.sgr.cachify;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Strings;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public abstract class AbstractBlockingCacheTest {

    protected abstract BlockingCache<String> getBlockingCache();

    protected abstract Logger getLogger();

    @After
    public void tearDown() {
        getLogger().info("Cleaning up ...");
        final Instant start = Instant.now(Clock.systemUTC());
        getBlockingCache().evictAll();
        final Duration duration = Duration.between(start, Instant.now(Clock.systemUTC()));
        getLogger().info("It took {} to clean up.", duration);
    }

    @Test(expected = IOException.class)
    public void testCheckedException() throws IOException {
        final BlockingCache<String> cache = getBlockingCache();
        cache.get("some_key", key -> {
            throw new IOException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

    @Test(expected = RuntimeException.class)
    public void testUncheckedException() {
        final BlockingCache<String> cache = getBlockingCache();
        cache.get("some_key", key -> {
            throw new RuntimeException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

    @Test
    public void testGetWithoutGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        cache.evict("k2");
        Optional<String> result = cache.get("k2");
        assertFalse(result.isPresent());

        cache.put("k2", "v2");
        result = cache.get("k2");
        assertEquals("v2", result.orElse(null));
    }

    @Test
    public void testGetWithGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        Optional<String> result = cache.get("k3", key -> Optional.of("v3"));
        assertEquals("v3", result.orElse(null));
        result = cache.get("k3");
        assertEquals("v3", result.orElse(null));
    }

    @Test
    public void testUncheckedGetWithGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        Optional<String> result = cache.uncheckedGet("k4", key -> Optional.of("v4"));
        assertEquals("v4", result.orElse(null));
        result = cache.get("k4");
        assertEquals("v4", result.orElse(null));
    }

    @Test
    public void testMassiveBulkGet() {
        final String keyPrefix = "bulk_key_";
        final int total = Optional.ofNullable(System.getenv("IT_REDIS_BULK_TOTAL")).map(Strings::emptyToNull).map(Integer::parseInt).orElse(100000);
        final int nThreads = Optional.ofNullable(System.getenv("IT_REDIS_BULK_THREAD")).map(Strings::emptyToNull).map(Integer::parseInt).orElse(100);

        final ExecutorService es = Executors.newFixedThreadPool(nThreads);
        getLogger().info("Generating keys ...");
        final Instant start = Instant.now(Clock.systemUTC());
        final List<CompletableFuture<?>> futureList = new LinkedList<>();
        final AtomicInteger generated = new AtomicInteger(0);
        for (int i = 1; i <= nThreads; i++) {
            futureList.add(
                    CompletableFuture.runAsync(() -> {
                        int value;
                        for (int j = 1; j <= total / nThreads; j++) {
                            value = generated.incrementAndGet();
                            getBlockingCache().put(keyPrefix + value, String.valueOf(value));
                        }
                    }, es)
            );
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        final Duration duration = Duration.between(start, Instant.now(Clock.systemUTC()));
        getLogger().info("It took {} to generate {} records.", duration, generated.get());
        futureList.clear();

        for (int i = 1; i <= 10; i++) {
            futureList.add(
                    CompletableFuture.runAsync(() -> {
                        final Instant readStart = Instant.now(Clock.systemUTC());
                        final AtomicInteger counter = new AtomicInteger(0);
                        try (
                                Stream<String> stream = getBlockingCache().bulkGet(keyPrefix, 1000)
                        ) {
                            stream.forEach(value -> {
                                assertTrue(Integer.parseInt(value) > 0);
                                counter.getAndIncrement();
                            });
                        }
                        final Duration readDuration = Duration.between(readStart, Instant.now(Clock.systemUTC()));
                        getLogger().info("It took {} to get {} records.", readDuration, counter.get());
                        assertEquals(generated.get(), counter.get());
                    }, es)
            );
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        futureList.clear();
    }

}