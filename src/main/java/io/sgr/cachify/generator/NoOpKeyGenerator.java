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

package io.sgr.cachify.generator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import javax.annotation.Nonnull;

/**
 * No-operation generator which returns the given key directly.
 */
public final class NoOpKeyGenerator implements KeyGenerator {

    private static final KeyGenerator INSTANCE = new NoOpKeyGenerator();

    private NoOpKeyGenerator() {

    }

    /**
     * Return the default instance of this generator.
     *
     * @return The key generator.
     */
    public static KeyGenerator getInstance() {
        return INSTANCE;
    }

    @Nonnull
    @Override
    public String generate(@Nonnull final String cacheName, @Nonnull final String key) {
        checkArgument(!isNullOrEmpty(key), "Missing key!");
        return key;
    }
}
