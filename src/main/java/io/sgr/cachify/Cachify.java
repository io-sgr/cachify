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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import io.sgr.cachify.generator.KeyGenerator;
import io.sgr.cachify.generator.SimpleKeyGenerator;
import io.sgr.cachify.guava.BlockingGuavaCache;
import io.sgr.cachify.serialization.JsonSerializer;
import io.sgr.cachify.serialization.ValueSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

public final class Cachify<V> implements BlockingCache<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cachify.class);

    private final String cacheName;
    private final BlockingCache<String> backend;
    private final ValueSerializer<V> serializer;
    private final KeyGenerator keyGenerator;

    private Cachify(
            @Nonnull final String cacheName,
            @Nonnull final BlockingCache<String> backend,
            @Nonnull final ValueSerializer<V> serializer,
            @Nonnull final KeyGenerator keyGenerator
    ) {
        this.cacheName = cacheName;
        this.backend = backend;
        this.serializer = serializer;
        this.keyGenerator = keyGenerator;
    }

    public static <V> Builder<V> register(@Nonnull final String cacheName) {
        return new Builder<>(cacheName);
    }

    @Nonnull
    @Override
    public Optional<V> get(@Nonnull final String key) {
        return backend.get(keyGenerator.generate(cacheName, key)).map(serializer::deserialize);
    }

    @Nonnull
    @Override
    public <E extends Exception> Optional<V> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, V, E> getter) throws E {
        final V value = get(key).orElse(getter.get(key));
        return Optional.ofNullable(value);
    }

    @Nonnull
    @Override
    public Optional<V> uncheckedGet(@Nonnull final String key, @Nonnull final ValueGetter<String, V> getter) {
        final V value = get(key).orElse(getter.get(key));
        return Optional.ofNullable(value);
    }

    @Override
    public void put(@Nonnull final String key, @Nonnull final V value) {
        backend.put(keyGenerator.generate(cacheName, key), serializer.serialize(value));
    }

    @Override
    public void evict(@Nonnull final String key) {
        backend.evict(keyGenerator.generate(cacheName, key));
    }

    @Override
    public void bulkEvict(@Nonnull final String keyPattern) {
        backend.bulkEvict(keyGenerator.generate(cacheName, keyPattern));
    }

    @Override
    public void close() throws Exception {
        this.backend.close();
    }


    public static final class Builder<V> {

        private static final long DEFAULT_VALUE_EXPIRES_IN_MILLI = TimeUnit.HOURS.toMillis(1);

        private final String name;

        private BlockingCache<String> backend = null;
        private KeyGenerator keyGenerator = null;
        private ValueSerializer<V> serializer = null;
        private Long valueExpiresInMilli = null;
        private boolean useInMemoryL2Cache = false;
        // private Long lv2ExpiresInMilli = null;

        private Builder(final String name) {
            checkArgument(!isNullOrEmpty(name), "Missing cache name!");
            this.name = name;
        }

        public Builder<V> backend(@Nonnull final BlockingCache<String> backend) {
            checkArgument(nonNull(backend), "Wanna use a specified cache backend but just passed NULL, that does not make sense!");
            this.backend = backend;
            return this;
        }

        public Builder<V> keyGenerator(@Nonnull final KeyGenerator keyGenerator) {
            checkArgument(nonNull(keyGenerator), "Wanna use key generator but just passed NULL, that does not make sense!");
            this.keyGenerator = keyGenerator;
            return this;
        }

        public Builder<V> serializer(@Nonnull final ValueSerializer<V> serializer) {
            checkArgument(nonNull(serializer), "Wanna use a customized serializer but passing NULL? That does not make sense!");
            this.serializer = serializer;
            return this;
        }

        public Builder<V> valueExpiresIn(final long duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Expiration time should be greater than zero!");
            checkArgument(nonNull(unit), "Wanna use a customized expiration but passing NULL as the unit of time? That does not make sense!");
            this.valueExpiresInMilli = unit.toMillis(duration);
            return this;
        }

        //        public Builder<V> useInMemoryL2Cache(final long duration, @Nonnull final TimeUnit unit) {
        //            checkArgument(duration > 0, "Expiration time should be greater than zero!");
        //            checkArgument(nonNull(unit), "Wanna use a customized expiration but passing NULL as the unit of time? That does not make sense!");
        //            this.useInMemoryL2Cache = true;
        //            this.lv2ExpiresInMilli = unit.toMillis(duration);
        //            return this;
        //        }

        @Nonnull
        public BlockingCache<V> build() {
            if (isNull(backend)) {
                return buildInMemoryCache(name, keyGenerator, serializer, valueExpiresInMilli);
            }
            if (useInMemoryL2Cache) {
                // TODO: Add support to level 2 cache.
                // final BlockingCache<V> lv2Cache = buildInMemoryCache(name, keyGenerator, serializer, lv2ExpiresInMilli);
                return new Cachify<>(name, backend, serializer, keyGenerator);
            }
            Optional.ofNullable(valueExpiresInMilli)
                    .ifPresent(expiration -> {
                        final String template = "Building cache with provided backend, given value expiration time '{}ms' will be ignored.";
                        LOGGER.warn(template, expiration);
                    });
            return new Cachify<>(name, backend, serializer, keyGenerator);
        }

        @Nonnull
        private BlockingCache<V> buildInMemoryCache(
                final String name, final KeyGenerator keyGenerator, final ValueSerializer<V> serializer, final Long valueExpiresInMilli
        ) {
            final long milli = Optional.ofNullable(valueExpiresInMilli)
                    .orElseGet(() -> {
                        LOGGER.warn("No expiration time for value been provided, using default {}ms", DEFAULT_VALUE_EXPIRES_IN_MILLI);
                        return DEFAULT_VALUE_EXPIRES_IN_MILLI;
                    });
            final BlockingGuavaCache backend = BlockingGuavaCache.newBuilder().expiresIn(milli).build();
            return new Cachify<>(
                    name, backend,
                    Optional.ofNullable(serializer)
                            .orElseGet(() -> {
                                LOGGER.warn("No value serializer specified, using default: {}", JsonSerializer.class);
                                return JsonSerializer.getDefault();
                            }),
                    Optional.ofNullable(keyGenerator)
                            .orElseGet(() -> {
                                LOGGER.warn("No key generator specified, using default: {}", SimpleKeyGenerator.class);
                                return SimpleKeyGenerator.getInstance();
                            }));
        }
    }

}
