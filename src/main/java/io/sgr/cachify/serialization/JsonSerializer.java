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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import javax.annotation.Nonnull;

public final class JsonSerializer<V> implements ValueSerializer<V> {

    private final TypeReference<V> valueTypeRef = new TypeReference<V>() {
    };

    private final ObjectMapper objectMapper;

    private JsonSerializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Build JSON serializer with given object mapper.
     *
     * @param objectMapper
     *         The object mapper.
     * @param <V>
     *         The type of object.
     * @return The serializer.
     */
    public static <V> JsonSerializer<V> using(@Nonnull final ObjectMapper objectMapper) {
        checkArgument(nonNull(objectMapper), "Wanna use a customized object mapper but passing NULL? That does not make sense!");
        return new JsonSerializer<>(objectMapper);
    }

    /**
     * Build default JSON serializer with default object mapper from {@link JsonUtil#getObjectMapper()}.
     *
     * @param <V>
     *         The type of object.
     * @return The serializer.
     */
    public static <V> JsonSerializer<V> getDefault() {
        return new JsonSerializer<>(JsonUtil.getObjectMapper());
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull final V value) throws ValueProcessingException {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ValueProcessingException(e);
        }
    }

    @Nonnull
    @Override
    public V deserialize(@Nonnull final String string) throws ValueProcessingException {
        try {
            return objectMapper.readValue(string, valueTypeRef);
        } catch (IOException e) {
            throw new ValueProcessingException(e);
        }
    }

}
