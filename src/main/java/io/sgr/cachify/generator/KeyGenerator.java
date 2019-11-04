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

import javax.annotation.Nonnull;

/**
 * A cache key generator for users to manipulate key, for example adding pre/surfix or even version.
 */
public interface KeyGenerator {

    /**
     * Generate a new globally unique key to store data in cache. We recommend to include name of the cache in your algorithm to avoid key conflict because
     * sometime multiple components are sharing same cache backend, you thought a UUID should be globally unique, others thought so too.
     *
     * @param cacheName The name of the cache which this generator will work with.
     * @param key The original key or pattern of key in bulk operations.
     * @return A generated globally unique key.
     */
    @Nonnull
    String generate(@Nonnull String cacheName, @Nonnull String key);

}
