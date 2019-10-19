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
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.nonNull;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

import io.sgr.cachify.BlockingCache;
import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.ValueGetter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public class BlockingRedisCache<V> implements BlockingCache<V>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRedisCache.class);

    private final JedisPoolAbstract jedisPool;
    private final ObjectMapper objectMapper;
    private final TypeReference<V> valueTypeRef = new TypeReference<V>() {
    };

    private BlockingRedisCache(@Nonnull final JedisPoolAbstract jedisPool, @Nonnull final ObjectMapper objectMapper) {
        this.jedisPool = jedisPool;
        this.objectMapper = objectMapper;
    }

    public static <V> Builder<V> newBuilder() {
        return new Builder<>();
    }

    @Nonnull
    @Override
    public Optional<V> get(@Nonnull final String key) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            final String json = jedis.get(key);
            if (isNullOrEmpty(json)) {
                return Optional.empty();
            }
            return Optional.ofNullable(objectMapper.readValue(json, valueTypeRef));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Nonnull
    @Override
    public <E extends Throwable> Optional<V> get(@Nonnull final String key, @Nonnull final CheckedValueGetter<String, V, E> getter) throws E {
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
    public void put(@Nonnull final String key, @Nonnull final V value, final long expirationInMilli) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            jedis.set(key, objectMapper.writeValueAsString(value), SetParams.setParams().px(expirationInMilli));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void evict(@Nonnull final String key) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            jedis.del(key);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void bulkEvict(@Nonnull final String keyPattern) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            String nextCursor = SCAN_POINTER_START;
            final ScanParams params = new ScanParams();
            params.match(keyPattern);
            ScanResult<String> scanResult;
            do {
                scanResult = jedis.scan(nextCursor, params);
                scanResult.getResult().forEach(this::evict);
                nextCursor = scanResult.getCursor();
            } while (!nextCursor.equals(SCAN_POINTER_START));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        jedisPool.close();
    }


    public static class Builder<V> {

        private static final int DEFAULT_MAX_TOTAL = Runtime.getRuntime().availableProcessors();
        private static final int DEFAULT_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(2);
        private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

        private JedisPoolAbstract pool;
        private int maxTotal;
        private int timeout;
        private ObjectMapper objectMapper;

        /*
         * The following fields are for single host
         */
        private String singleHost;
        private int singleHostPort;

        /*
         * The following fields are for sentinel
         */
        private String masterName;
        private Set<String> sentinels;

        private Builder() {
        }

        public Builder<V> singleHost(@Nonnull final String hostname, final int port) {
            checkArgument(!isNullOrEmpty(hostname), "The hostname should be provided!");
            checkArgument(port >= 1 && port <= 65535, "The port should be between 1 to 65535!");
            this.singleHost = hostname;
            this.singleHostPort = port;
            return this;
        }

        public Builder<V> sentinel(@Nonnull final String masterName, final String... sentinels) {
            checkArgument(!isNullOrEmpty(masterName), "Master name should be provided!");
            this.masterName = masterName;
            checkArgument(nonNull(sentinels), "Build without sentinel servers does not make sense!");
            Set<String> validServers = Stream.of(sentinels)
                    .map(servers -> new HashSet<>(Arrays.asList(servers.split(","))))
                    .reduce((servers, otherServers) -> {
                        servers.addAll(otherServers);
                        return otherServers;
                    })
                    .orElseThrow(() -> new IllegalArgumentException("Should provide at least 2 sentinel servers!"));
            checkArgument(validServers.size() > 1, "Should provide at least 2 sentinel servers!");
            this.sentinels = validServers;
            return this;
        }

        public Builder<V> pool(@Nonnull final JedisPoolAbstract pool) {
            checkArgument(nonNull(pool), "Build with a NULL jedis pool does not make sense!");
            this.pool = pool;
            return this;
        }

        public Builder<V> setMaxTotal(final int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder<V> timeout(final int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder<V> objectMapper(@Nonnull final ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public BlockingRedisCache<V> build() {
            GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
            config.setMaxTotal(maxTotal <= 0 ? DEFAULT_MAX_TOTAL : maxTotal);
            final JedisPoolAbstract pool = Optional.ofNullable(this.pool)
                    .orElseGet(() -> {
                        final int timeout = this.timeout <= 0 ? DEFAULT_TIMEOUT : this.timeout;
                        if (nonNull(singleHost)) {
                            return new JedisPool(config, singleHost, singleHostPort, timeout);
                        }
                        return new JedisSentinelPool(masterName, sentinels, config, timeout);
                    });
            return new BlockingRedisCache<>(pool, Optional.ofNullable(objectMapper).orElse(DEFAULT_OBJECT_MAPPER));
        }

    }

}
