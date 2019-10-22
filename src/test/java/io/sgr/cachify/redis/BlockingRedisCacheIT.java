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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BlockingRedisCacheIT {

    private static final String REDIS_HOST = Optional.ofNullable(emptyToNull(System.getenv("IT_REDIS_HOST"))).orElse("127.0.0.1");
    private static final int REDIS_PORT = Integer.parseInt(Optional.ofNullable(emptyToNull(System.getenv("IT_REDIS_PORT"))).orElse("6379"));

    @Test
    public void testSimpleGet() {
        final BlockingRedisCache cache = BlockingRedisCache.<String>newBuilder()
                .singleHost(REDIS_HOST, REDIS_PORT)
                .expiresIn(2, TimeUnit.MINUTES)
                .build();
        Optional<String> result = cache.get("not_exist");
        assertFalse(result.isPresent());
        cache.put("test_key", "some_value");
        result = cache.get("not_exist");
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetStringValue() {
        final BlockingRedisCache cache = BlockingRedisCache.<String>newBuilder()
                .singleHost(REDIS_HOST, REDIS_PORT)
                .expiresIn(2, TimeUnit.MINUTES)
                .build();
        cache.put("test_key", "some_value");
        Optional<String> result = cache.get("test_key");
        assertEquals("some_value", result.orElse(null));
    }

}