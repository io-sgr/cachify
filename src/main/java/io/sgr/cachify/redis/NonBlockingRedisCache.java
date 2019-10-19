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

package io.sgr.cachify.redis;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;

import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.NonBlockingCache;
import io.sgr.cachify.ValueGetter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

public class NonBlockingRedisCache<V> implements NonBlockingCache<V>, AutoCloseable {

    private final BlockingRedisCache<V> delegate;
    private final Executor executor;

    public NonBlockingRedisCache(@Nonnull final BlockingRedisCache<V> delegate, @Nonnull final Executor executor) {
        checkArgument(nonNull(delegate), "Missing delegate!");
        this.delegate = delegate;
        checkArgument(nonNull(executor), "Missing executor!");
        this.executor = executor;
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<V>> get(@Nonnull final String key) {
        return CompletableFuture.supplyAsync(() -> delegate.get(key), executor);
    }

    @Nonnull
    @Override
    public <E extends Throwable> CompletableFuture<Optional<V>> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, V, E> getter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return delegate.get(key, getter);
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<V>> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, V> getter) {
        return CompletableFuture.supplyAsync(() -> delegate.uncheckedGet(key, getter), executor);
    }

    @Override
    public CompletableFuture<Void> put(@Nonnull final String key, @Nonnull final V value, final long expirationInMilli) {
        return CompletableFuture.runAsync(() -> delegate.put(key, value, expirationInMilli), executor);
    }

    @Override
    public CompletableFuture<Void> evict(@Nonnull final String key) {
        return CompletableFuture.runAsync(() -> delegate.evict(key), executor);
    }

    @Override
    public CompletableFuture<Void> bulkEvict(@Nonnull final String keyPattern) {
        return CompletableFuture.runAsync(() -> delegate.bulkEvict(keyPattern), executor);
    }

    @Override
    public void close() {
        delegate.close();
    }

}
