/*
 * Copyright 2017-2019 SgrAlpha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.sgr.cachify.redis;

import static com.google.common.base.Strings.emptyToNull;

import io.sgr.cachify.AbstractBlockingCacheTest;
import io.sgr.cachify.BlockingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BlockingRedisCacheIT extends AbstractBlockingCacheTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRedisCacheIT.class);

    private static final String REDIS_HOST = Optional.ofNullable(emptyToNull(System.getenv("IT_REDIS_HOST"))).orElse("127.0.0.1");
    private static final int REDIS_PORT = Integer.parseInt(Optional.ofNullable(emptyToNull(System.getenv("IT_REDIS_PORT"))).orElse("6379"));

    private final BlockingRedisCache redisCache = new BlockingRedisCache(
            RedisCacheConfiguration.newBuilder()
                    .singleHost(REDIS_HOST, REDIS_PORT)
                    .expiresIn(1, TimeUnit.HOURS)
                    .build()
    );

    @Override
    protected BlockingCache<String> getBlockingCache() {
        return redisCache;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}