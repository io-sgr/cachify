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
import io.sgr.cachify.ValueGetter;

import java.util.Optional;

import javax.annotation.Nonnull;

public class TieredBlockingCache<V> implements BlockingCache<V> {

    private final BlockingCache<V> primary;
    private final BlockingCache<V> secondary;

    /**
     * Construct a tiered blocking cache. If cache missed in primary cache, then try secondary.
     *
     * @param primary
     *         The primary cache.
     * @param secondary
     *         The secondary cache.
     */
    public TieredBlockingCache(@Nonnull final BlockingCache<V> primary, final BlockingCache<V> secondary) {
        checkArgument(nonNull(primary), "Missing primary cache!");
        this.primary = primary;
        checkArgument(nonNull(primary), "Missing secondary cache!");
        this.secondary = secondary;
    }

    @Nonnull
    @Override
    public Optional<V> get(@Nonnull final String key) {
        return primary.get(key, k -> secondary.get(k).orElse(null));
    }

    @Nonnull
    @Override
    public <E extends Exception> Optional<V> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, V, E> getter) throws E {
        return primary.get(key, k -> secondary.get(k, getter).orElse(null));
    }

    @Nonnull
    @Override
    public Optional<V> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, V> getter) {
        return primary.uncheckedGet(key, k -> secondary.uncheckedGet(k, getter).orElse(null));
    }

    @Override
    public void put(@Nonnull final String key, @Nonnull final V value) {
        primary.put(key, value);
        secondary.put(key, value);
    }

    @Override
    public void evict(@Nonnull final String key) {
        secondary.evict(key);
        primary.evict(key);
    }

    @Override
    public void bulkEvict(@Nonnull final String keyPattern) {
        secondary.bulkEvict(keyPattern);
        primary.bulkEvict(keyPattern);
    }

    @Override
    public void close() throws Exception {
        primary.close();
        secondary.close();
    }

}
