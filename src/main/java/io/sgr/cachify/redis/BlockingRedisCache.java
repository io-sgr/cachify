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

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

import io.sgr.cachify.BlockingCache;
import io.sgr.cachify.CheckedValueGetter;
import io.sgr.cachify.ValueGetter;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.Optional;

import javax.annotation.Nonnull;

public class BlockingRedisCache implements BlockingCache<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRedisCache.class);

    private final JedisPoolAbstract jedisPool;
    private final long expirationInMilli;

    public BlockingRedisCache(@Nonnull final RedisCacheConfiguration config) {
        this.jedisPool = config.getJedisPool();
        this.expirationInMilli = config.getExpirationInMilli();
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

}
