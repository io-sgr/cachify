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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

public final class BlockingGuavaCache implements BlockingCache<String> {

    private final Cache<String, String> cache;

    private BlockingGuavaCache(
            final long maxSize,
            final long duration, @Nonnull final TimeUnit unit) {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(duration, unit)
                .maximumSize(maxSize)
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private static Predicate<String> matchPattern(@Nonnull final String keyPattern) {
        return key -> key.startsWith(keyPattern);
    }

    @Nonnull
    @Override
    public Optional<String> get(@Nonnull final String key) {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @Nonnull
    @Override
    public <E extends Exception> Optional<String> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, String, E> getter) throws E {
        final String value = get(key).orElse(getter.get(key));
        final Optional<String> result = Optional.ofNullable(value);
        result.ifPresent(v -> put(key, v));
        return result;
    }

    @Nonnull
    @Override
    public Optional<String> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, String> getter) {
        final String value = get(key).orElse(getter.get(key));
        final Optional<String> result = Optional.ofNullable(value);
        result.ifPresent(v -> put(key, v));
        return result;
    }

    @Override
    public void put(@Nonnull final String key, @Nonnull final String value) {
        cache.put(key, value);
    }

    @Override
    public void evict(@Nonnull final String key) {
        cache.invalidate(key);
    }

    @Override
    public void bulkEvict(@Nonnull final String keyPattern) {
        cache.invalidateAll((Iterable<String>) () -> cache.asMap().keySet().stream().filter(matchPattern(keyPattern)).iterator());
    }

    @Override
    public void close() {

    }

    public static class Builder {

        static final int DEFAULT_MAX_TOTAL = Integer.MAX_VALUE;
        static final long DEFAULT_VALUE_EXPIRES_IN_MILLI = TimeUnit.HOURS.toMillis(1);

        private int maxTotal;
        private Long expirationDuration;
        private TimeUnit expirationTimeUnit;

        private Builder() {
        }

        /**
         * Set the number of maximum elements to keep in memory, default to {@link Builder#DEFAULT_MAX_TOTAL}.
         *
         * @param maxTotal the maximum elements to keep in memory.
         * @return The builder.
         */
        public Builder setMaxTotal(final int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Set element expiration time, default to {@link Builder#DEFAULT_VALUE_EXPIRES_IN_MILLI}.
         *
         * @param duration The duration, should be grater than 0.
         * @param unit The time unit of duration.
         * @return The builder.
         */
        public Builder expiresIn(final long duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Expiration time should be greater than zero!");
            this.expirationDuration = duration;
            checkArgument(nonNull(unit), "Wanna use a customized expiration but passing NULL as the unit of time? That does not make sense!");
            this.expirationTimeUnit = unit;
            return this;
        }

        /**
         * Set element expiration time, default to {@link Builder#DEFAULT_VALUE_EXPIRES_IN_MILLI}.
         *
         * @param milli expiration time in millisecond, should be greater than 0.
         * @return The builder.
         */
        public Builder expiresIn(final long milli) {
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
        public BlockingGuavaCache build() {
            final int maxSize = maxTotal <= 0 ? DEFAULT_MAX_TOTAL : maxTotal;
            if (isNull(expirationDuration) || isNull(expirationTimeUnit)) {
                return new BlockingGuavaCache(maxSize, DEFAULT_VALUE_EXPIRES_IN_MILLI, TimeUnit.MILLISECONDS);
            }
            return new BlockingGuavaCache(maxSize, expirationDuration, expirationTimeUnit);
        }

    }

}
