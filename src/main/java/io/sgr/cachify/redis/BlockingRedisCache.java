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
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

import io.sgr.cachify.BlockingCache;
import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.ValueGetter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

public class BlockingRedisCache<V> implements BlockingCache<V>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRedisCache.class);

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;
    private final TypeReference<V> valueTypeRef = new TypeReference<V>() {
    };

    public BlockingRedisCache(@Nonnull final JedisPool jedisPool, @Nonnull final ObjectMapper objectMapper) {
        checkArgument(nonNull(jedisPool), "JedisPool should not be NULL!");
        this.jedisPool = jedisPool;
        checkArgument(nonNull(objectMapper), "ObjectMapper should not be NULL!");
        this.objectMapper = objectMapper;
    }

    @Nonnull
    @Override
    public Optional<V> get(@Nonnull final String key) {
        try (
                Jedis jedis = jedisPool.getResource()
        ) {
            return Optional.ofNullable(objectMapper.readValue(jedis.get(key), valueTypeRef));
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

}
