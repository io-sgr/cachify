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

import java.util.Optional;

import javax.annotation.Nonnull;

public interface BlockingCache<V> {

    /**
     * Get an object from cache with given key.
     *
     * @param key
     *         The cache key.
     * @return An cached object, may be empty.
     */
    @Nonnull
    Optional<V> get(@Nonnull String key);

    /**
     * Get an object from cache with given key, if missing will fallback to the given value getter.
     * The given value getter might failed to retrieve the value object and throw a checked exception.
     *
     * @param key
     *         The cache key.
     * @param getter
     *         The fallback value getter.
     * @param <E>
     *         Describes the type of potential checked exception through by the value getter.
     * @return An cached object, may be empty.
     * @throws E
     *         A checked exception if anything went wrong calling the value getter.
     */
    @Nonnull
    <E extends Exception> Optional<V> get(@Nonnull String key, @Nonnull CheckedValueGetter<String, V, E> getter) throws E;

    /**
     * Get an object from cache with given key, if missing will fallback to the given value getter.
     * The given value getter might failed to retrieve the value object and throw a unchecked runtime exception.
     *
     * @param key
     *         The cache key.
     * @param getter
     *         The fallback value getter.
     * @return An cached object, may be empty.
     */
    @Nonnull
    Optional<V> uncheckedGet(@Nonnull String key, @Nonnull ValueGetter<String, V> getter);

    /**
     * Put an object in cache with given expiration.
     *
     * @param key
     *         The cache key.
     * @param value
     *         The object to put in cache.
     */
    void put(@Nonnull String key, @Nonnull V value);

    /**
     * Removes an object from cache.
     *
     * @param key
     *         The cache key.
     */
    void evict(@Nonnull String key);

    /**
     * Bulk remove object from cache using a given prefix of key.
     *
     * @param keyPattern
     *         The pattern of key.
     */
    void bulkEvict(@Nonnull String keyPattern);

}
