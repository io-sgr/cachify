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
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.assertEquals;

import io.sgr.cachify.exceptions.ValueProcessingException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.Test;

import java.util.Objects;

import javax.annotation.Nonnull;

public class Jackson2SerializerTest {

    private static final Jackson2Serializer<Sample> SERIALIZER = Jackson2Serializer.using(Sample.class);

    @Test
    public void testBiDirectionConvert() {
        final Sample ori = new Sample("test");
        final String json = SERIALIZER.serialize(ori);
        final Sample sample = SERIALIZER.deserialize(json);
        assertEquals(ori, sample);
    }

    @Test(expected = ValueProcessingException.class)
    public void testExceptionWhenDoingDeserialize() {
        //noinspection ConstantConditions
        SERIALIZER.deserialize(null);
    }

    private static class Sample {

        private final String name;

        @JsonCreator
        public Sample(@JsonProperty("name") @Nonnull final String name) {
            checkArgument(!isNullOrEmpty(name), "Missing name!");
            this.name = name;
        }

        @JsonProperty("name")
        @Nonnull
        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Sample)) {
                return false;
            }
            Sample sample = (Sample) o;
            return Objects.equals(getName(), sample.getName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName());
        }
    }

}