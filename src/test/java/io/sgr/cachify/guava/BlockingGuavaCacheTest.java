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

import static org.junit.Assert.fail;

import io.sgr.cachify.BlockingCache;

import org.junit.Test;

import java.io.IOException;

public class BlockingGuavaCacheTest {

    @Test(expected = IOException.class)
    public void testCheckedException() throws IOException {
        final BlockingCache<String> cache = BlockingGuavaCache.newBuilder().build();
        cache.get("some_key", key -> {
            throw new IOException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

    @Test(expected = RuntimeException.class)
    public void testUncheckedException() {
        final BlockingCache<String> cache = BlockingGuavaCache.newBuilder().build();
        cache.get("some_key", key -> {
            throw new RuntimeException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

}