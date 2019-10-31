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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public abstract class AbstractBlockingCacheTest {

    protected abstract BlockingCache<String> getBlockingCache();

    @After
    public void tearDown() {
        getBlockingCache().bulkEvict("");
    }

    @Test(expected = IOException.class)
    public void testCheckedException() throws IOException {
        final BlockingCache<String> cache = getBlockingCache();
        cache.get("some_key", key -> {
            throw new IOException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

    @Test(expected = RuntimeException.class)
    public void testUncheckedException() {
        final BlockingCache<String> cache = getBlockingCache();
        cache.get("some_key", key -> {
            throw new RuntimeException("Just throw exception");
        });
        fail("Expecting exception but succeeded.");
    }

    @Test
    public void testGetWithoutGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        Optional<String> result = cache.get("k2");
        assertFalse(result.isPresent());

        cache.put("k2", "v2");
        result = cache.get("k2");
        assertEquals("v2", result.orElse(null));
    }

    @Test
    public void testGetWithGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        Optional<String> result = cache.get("k3", key -> "v3");
        assertEquals("v3", result.orElse(null));
        result = cache.get("k3");
        assertEquals("v3", result.orElse(null));
    }

    @Test
    public void testUncheckedGetWithGetter() {
        final BlockingCache<String> cache = getBlockingCache();
        Optional<String> result = cache.uncheckedGet("k4", key -> "v4");
        assertEquals("v4", result.orElse(null));
        result = cache.get("k4");
        assertEquals("v4", result.orElse(null));
    }

}