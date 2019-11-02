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

package io.sgr.cachify.guava;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import io.sgr.cachify.BlockingCache;
import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.ValueGetter;
import io.sgr.cachify.serialization.JsonSerializer;
import io.sgr.cachify.serialization.ValueSerializer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

public final class BlockingGuavaCache<V> implements BlockingCache<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingGuavaCache.class);

    private final ValueSerializer<V> serializer;
    private final Cache<String, String> cache;

    private BlockingGuavaCache(
            @Nonnull final ValueSerializer<V> serializer,
            final long maxSize,
            final long duration, @Nonnull final TimeUnit unit) {
        this.serializer = serializer;
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(duration, unit)
                .maximumSize(maxSize)
                .build();
    }

    public static <V> Builder<V> newBuilder() {
        return new Builder<>();
    }

    @Nonnull
    @Override
    public Optional<V> get(@Nonnull final String key) {
        return Optional.ofNullable(cache.getIfPresent(key)).map(serializer::deserialize);
    }

    @Nonnull
    @Override
    public <E extends Exception> Optional<V> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, V, E> getter) throws E {
        final V value = get(key).orElse(getter.get(key));
        final Optional<V> result = Optional.ofNullable(value);
        result.ifPresent(v -> put(key, v));
        return result;
    }

    @Nonnull
    @Override
    public Optional<V> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, V> getter) {
        final V value = get(key).orElse(getter.get(key));
        final Optional<V> result = Optional.ofNullable(value);
        result.ifPresent(v -> put(key, v));
        return result;
    }

    @Override
    public void put(@Nonnull final String key, @Nonnull final V value) {
        cache.put(key, serializer.serialize(value));
    }

    @Override
    public void evict(@Nonnull final String key) {
        cache.invalidate(key);
    }

    @Override
    public void bulkEvict(@Nonnull final String keyPattern) {
        cache.invalidateAll((Iterable<String>) () -> cache.asMap().keySet().stream().filter(matchPattern(keyPattern)).iterator());
    }

    private static Predicate<String> matchPattern(@Nonnull final String keyPattern) {
        return key -> key.startsWith(keyPattern);
    }

    @Override
    public void close() {

    }

    public static class Builder<V> {

        static final int DEFAULT_MAX_TOTAL = Integer.MAX_VALUE;
        static final long DEFAULT_VALUE_EXPIRES_IN_MILLI = TimeUnit.HOURS.toMillis(1);

        private ValueSerializer<V> serializer = null;

        private int maxTotal;
        private Long expirationDuration;
        private TimeUnit expirationTimeUnit;

        private Builder() {
        }

        /**
         * Set a value serializer to help convert Java object to the format that supported by cache implementation.
         *
         * @param serializer
         *         The serializer.
         * @return The builder.
         */
        public Builder<V> serializer(@Nonnull final ValueSerializer<V> serializer) {
            checkArgument(nonNull(serializer), "Wanna use a customized serializer but passing NULL? That does not make sense!");
            this.serializer = serializer;
            return this;
        }

        /**
         * Set the number of maximum elements to keep in memory, default to {@link Builder#DEFAULT_MAX_TOTAL}.
         *
         * @param maxTotal
         *         the maximum elements to keep in memory.
         * @return The builder.
         */
        public Builder<V> setMaxTotal(final int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Set element expiration time, default to {@link Builder#DEFAULT_VALUE_EXPIRES_IN_MILLI}.
         *
         * @param duration
         *         The duration, should be grater than 0.
         * @param unit
         *         The time unit of duration.
         * @return The builder.
         */
        public Builder<V> expiresIn(final long duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Expiration time should be greater than zero!");
            this.expirationDuration = duration;
            checkArgument(nonNull(unit), "Wanna use a customized expiration but passing NULL as the unit of time? That does not make sense!");
            this.expirationTimeUnit = unit;
            return this;
        }

        /**
         * Set element expiration time, default to {@link Builder#DEFAULT_VALUE_EXPIRES_IN_MILLI}.
         *
         * @param milli
         *         expiration time in millisecond, should be greater than 0.
         * @return The builder.
         */
        public Builder<V> expiresIn(final long milli) {
            checkArgument(milli > 0, "Expiration time should be greater than zero!");
            this.expirationDuration = milli;
            this.expirationTimeUnit = TimeUnit.MILLISECONDS;
            return this;
        }

        /**
         * Build cache based on given parameter.
         *
         * @return The cache.
         */
        @Nonnull
        public BlockingGuavaCache<V> build() {
            final ValueSerializer<V> actualSerializer = Optional.ofNullable(serializer)
                    .orElseGet(() -> {
                        LOGGER.warn("No value serializer specified, using default: {}", JsonSerializer.class);
                        return JsonSerializer.getDefault();
                    });
            final int maxSize = maxTotal <= 0 ? DEFAULT_MAX_TOTAL : maxTotal;
            if (isNull(expirationDuration) || isNull(expirationTimeUnit)) {
                return new BlockingGuavaCache<>(actualSerializer, maxSize, DEFAULT_VALUE_EXPIRES_IN_MILLI, TimeUnit.MILLISECONDS);
            }
            return new BlockingGuavaCache<>(actualSerializer, maxSize, expirationDuration, expirationTimeUnit);
        }

    }

}
