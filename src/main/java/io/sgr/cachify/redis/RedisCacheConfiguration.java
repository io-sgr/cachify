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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public final class RedisCacheConfiguration {

    private final JedisPoolAbstract jedisPool;
    private final long expirationInMilli;

    private RedisCacheConfiguration(JedisPoolAbstract jedisPool, long expirationInMilli) {
        this.jedisPool = jedisPool;
        this.expirationInMilli = expirationInMilli;
    }

    public JedisPoolAbstract getJedisPool() {
        return jedisPool;
    }

    public long getExpirationInMilli() {
        return expirationInMilli;
    }

    /**
     * Create a new builder.
     *
     * @return The builder.
     */
    public static Builder newBuilder() {
        return new Builder();
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
         * Configure redis cache with single host server.
         *
         * @param hostname The hostname.
         * @param port The port.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder singleHost(@Nonnull final String hostname, final int port) {
            checkArgument(!isNullOrEmpty(hostname), "The hostname should be provided!");
            checkArgument(port >= 1 && port <= 65535, "The port should be between 1 to 65535!");
            this.singleHost = hostname;
            this.singleHostPort = port;
            return this;
        }

        /**
         * Configure redis cache with sentinels.
         *
         * @param masterName The master.
         * @param sentinels Sentinels.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder sentinel(@Nonnull final String masterName, final String... sentinels) {
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
         * Build with given jedis pool.
         *
         * @param pool The given jedis pool.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder pool(@Nonnull final JedisPoolAbstract pool) {
            checkArgument(nonNull(pool), "Wanna use a pool but passing NULL? That does not make sense!");
            this.pool = pool;
            return this;
        }

        /**
         * Set the number of maximum elements to keep in memory, default to {@link RedisCacheConfiguration.Builder#DEFAULT_MAX_TOTAL}.
         *
         * @param maxTotal The maximum elements to keep in memory.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder setMaxTotal(final int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * The connection timeout, default to {@link RedisCacheConfiguration.Builder#DEFAULT_TIMEOUT_IN_MILLI}.
         *
         * @param duration The duration, should be grater than 0.
         * @param unit The time unit of duration.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder timeout(final int duration, @Nonnull final TimeUnit unit) {
            checkArgument(duration > 0, "Timeout should be greater than zero!");
            checkArgument(nonNull(unit), "Wanna use a customized timeout but passing NULL as the unit of time? That does not make sense!");
            this.timeoutInMilli = (int) unit.toMillis(duration);
            return this;
        }

        /**
         * Set element expiration time, default to {@link RedisCacheConfiguration.Builder#DEFAULT_VALUE_EXPIRES_IN_MILLI}.
         *
         * @param duration The duration, should be grater than 0.
         * @param unit The time unit of duration.
         * @return The builder.
         */
        public RedisCacheConfiguration.Builder expiresIn(final long duration, @Nonnull final TimeUnit unit) {
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
        public RedisCacheConfiguration build() {
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
            return new RedisCacheConfiguration(pool, expirationInMilli <= 0 ? DEFAULT_VALUE_EXPIRES_IN_MILLI : expirationInMilli);
        }

    }

}
