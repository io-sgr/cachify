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

public final class NoOpSerializer implements ValueSerializer<String> {

    private static final NoOpSerializer INSTANCE = new NoOpSerializer();

    private NoOpSerializer() {
    }

    public static NoOpSerializer getInstance() {
        return INSTANCE;
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull final String value) throws ValueProcessingException {
        return value;
    }

    @Nonnull
    @Override
    public String deserialize(@Nonnull final String string) throws ValueProcessingException {
        return string;
    }

}
