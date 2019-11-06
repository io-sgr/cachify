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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;

import io.sgr.cachify.exceptions.ValueProcessingException;
import io.sgr.cachify.utils.JsonUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nonnull;

public final class Jackson2Serializer<V> implements ValueSerializer<V> {

    private final Class<V> clazz;
    private final ObjectMapper objectMapper;

    private Jackson2Serializer(final Class<V> clazz, final ObjectMapper objectMapper) {
        checkArgument(nonNull(clazz), "Wanna use a customized object mapper but passing NULL? That does not make sense!");
        this.clazz = clazz;
        checkArgument(nonNull(objectMapper), "Wanna use a customized object mapper but passing NULL? That does not make sense!");
        this.objectMapper = objectMapper;
    }

    /**
     * Build JSON serializer with given object class.
     *
     * @param clazz The object class.
     * @param <V> The type of object.
     * @return The serializer.
     */
    public static <V> Jackson2Serializer<V> using(@Nonnull final Class<V> clazz) {
        return using(clazz, JsonUtil.getObjectMapper());
    }

    /**
     * Build JSON serializer with given object class and mapper.
     *
     * @param clazz The object class.
     * @param objectMapper The object mapper.
     * @param <V> The type of object.
     * @return The serializer.
     */
    public static <V> Jackson2Serializer<V> using(@Nonnull final Class<V> clazz, @Nonnull final ObjectMapper objectMapper) {
        return new Jackson2Serializer<>(clazz, objectMapper);
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull final V value) throws ValueProcessingException {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new ValueProcessingException(e);
        }
    }

    @Nonnull
    @Override
    public V deserialize(@Nonnull final String string) throws ValueProcessingException {
        try {
            return objectMapper.readValue(string, clazz);
        } catch (Exception e) {
            throw new ValueProcessingException(e);
        }
    }

}
