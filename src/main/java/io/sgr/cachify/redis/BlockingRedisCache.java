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

import com.google.common.base.Strings;
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

public final class BlockingRedisCache implements BlockingCache<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRedisCache.class);

    private final JedisPoolAbstract jedisPool;
    private final long expirationInMilli;

    private BlockingRedisCache(@Nonnull final JedisPoolAbstract jedisPool, final long expirationInMilli) {
        this.jedisPool = jedisPool;
        this.expirationInMilli = expirationInMilli;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    @Override
    public Optional<String> get(@Nonnull final String key) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            final String result = jedis.get(key);
            return Optional.ofNullable(result).map(Strings::emptyToNull);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return Optional.empty();
        }
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
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            jedis.set(key, value, SetParams.setParams().px(expirationInMilli));
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


    public static class Builder {

        static final int DEFAULT_MAX_TOTAL = Integer.MAX_VALUE;
        static final int DEFAULT_TIMEOUT_IN_MILLI = (int) TimeUnit.SECONDS.toMillis(2);
        static final long DEFAULT_VALUE_EXPIRES_IN_MILLI = TimeUnit.HOURS.toMillis(1);

        private JedisPoolAbstract pool;
        private int maxTotal;
        private int timeoutInMilli;
        private long expirationInMilli;

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

        /**
         *
         * @param hostname
         * @param port
         * @return The builder.
         */
        public Builder singleHost(@Nonnull final String hostname, final int port) {
            checkArgument(!isNullOrEmpty(hostname), "The hostname should be provided!");
            checkArgument(port >= 1 && port <= 65535, "The port should be between 1 to 65535!");
            this.singleHost = hostname;
            this.singleHostPort = port;
            return this;
        }

        /**
         *
         * @param masterName
         * @param sentinels
         * @return The builder.
         */
        public Builder sentinel(@Nonnull final String masterName, final String... sentinels) {
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

        /**
         *
         * @param pool
         * @return The builder.
         */
        public Builder pool(@Nonnull final JedisPoolAbstract pool) {
            checkArgument(nonNull(pool), "Wanna use a pool but passing NULL? That does not make sense!");
            this.pool = pool;
            return this;
        }

        /**
         * Set the number of maximum elements to keep in memory, default to {@link Builder#DEFAULT_MAX_TOTAL}.
         *
         * @param maxTotal
         *         the maximum elements to keep in memory.
         * @return The builder.
         */
        public Builder setMaxTotal(final int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         *
         * @param duration
         *         The duration, should be grater than 0.
         * @param unit
         *         The time unit of duration.
         * @return The builder.
         */
        public Builder timeout(final int duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Timeout should be greater than zero!");
            checkArgument(nonNull(unit), "Wanna use a customized timeout but passing NULL as the unit of time? That does not make sense!");
            this.timeoutInMilli = (int) unit.toMillis(duration);
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
        public Builder expiresIn(final long duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Expiration time should be greater than zero!");
            checkArgument(nonNull(unit), "Wanna use a customized expiration but passing NULL as the unit of time? That does not make sense!");
            this.expirationInMilli = unit.toMillis(duration);
            return this;
        }

        /**
         * Build cache based on given parameter.
         *
         * @return The cache.
         */
        @Nonnull
        public BlockingRedisCache build() {
            GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
            config.setMaxTotal(maxTotal <= 0 ? DEFAULT_MAX_TOTAL : maxTotal);
            final JedisPoolAbstract pool = Optional.ofNullable(this.pool)
                    .orElseGet(() -> {
                        final int timeout = this.timeoutInMilli <= 0 ? DEFAULT_TIMEOUT_IN_MILLI : this.timeoutInMilli;
                        if (nonNull(singleHost)) {
                            return new JedisPool(config, singleHost, singleHostPort, timeout);
                        }
                        return new JedisSentinelPool(masterName, sentinels, config, timeout);
                    });
            return new BlockingRedisCache(pool, expirationInMilli <= 0 ? DEFAULT_VALUE_EXPIRES_IN_MILLI : expirationInMilli);
        }

    }

}
