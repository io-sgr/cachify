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

package io.sgr.cachify.serialization;

import io.sgr.cachify.exceptions.ValueProcessingException;

import javax.annotation.Nonnull;

/**
 * An interface for user to serialize an object to string and also backward.
 *
 * @param <V>
 *         The type of value.
 */
public interface ValueSerializer<V> {

    /**
     * Serialize a value to string.
     *
     * @param value
     *         The value. Should not be NULL because serialize NULL then put it in cache does not make sense!
     * @return The serialized string.
     * @throws ValueProcessingException
     *         If something goes wrong.
     */
    @Nonnull
    String serialize(@Nonnull V value) throws ValueProcessingException;

    /**
     * Deserialize a value from string.
     *
     * @param string
     *         The serialized string. Should not be NULL because deserialize NULL from cache does not make sense, it should NOT been stored at all!
     * @return The value.
     * @throws ValueProcessingException
     *         If something goes wrong.
     */
    @Nonnull
    V deserialize(@Nonnull String string) throws ValueProcessingException;

}
