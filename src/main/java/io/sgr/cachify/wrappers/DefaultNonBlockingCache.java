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

package io.sgr.cachify.wrappers;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;

import io.sgr.cachify.BlockingCache;
import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.NonBlockingCache;
import io.sgr.cachify.ValueGetter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

public class DefaultNonBlockingCache implements NonBlockingCache<String> {

    private final BlockingCache<String> delegate;
    private final Executor executor;

    public DefaultNonBlockingCache(@Nonnull final BlockingCache<String> delegate, @Nonnull final Executor executor) {
        checkArgument(nonNull(delegate), "Missing delegate!");
        this.delegate = delegate;
        checkArgument(nonNull(executor), "Missing executor!");
        this.executor = executor;
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<String>> get(@Nonnull final String key) {
        return CompletableFuture.supplyAsync(() -> delegate.get(key), executor);
    }

    @Nonnull
    @Override
    public <E extends Exception> CompletableFuture<Optional<String>> get(
            @Nonnull final String key, @Nonnull final CheckedValueGetter<String, String, E> getter
    ) {
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
    public CompletableFuture<Optional<String>> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, String> getter) {
        return CompletableFuture.supplyAsync(() -> delegate.uncheckedGet(key, getter), executor);
    }

    @Override
    public CompletableFuture<Void> put(@Nonnull final String key, @Nonnull final String value) {
        return CompletableFuture.runAsync(() -> delegate.put(key, value), executor);
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
    public void close() throws Exception {
        delegate.close();
    }

}
