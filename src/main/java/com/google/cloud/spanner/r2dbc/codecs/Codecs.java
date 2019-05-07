/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc.codecs;


import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import reactor.util.annotation.Nullable;

/**
 * Encodes and decodes objects.
 */
public interface Codecs {

    /**
     * Decode a data to a value.
     *
     * @param row    the {@link Struct} containing the value to decode
     * @param type     the type to decode to
     * @param <T>      the type of item being returned
     * @return the decoded value
     * @throws NullPointerException if {@code format} or {@code type} is {@code null}
     */
    @Nullable
    <T> T decode(Struct row, Object identifier, Class<? extends T> type);

    /**
     * Encode a value.
     *
     * @param value the value to encode
     * @return the encoded value
     * @throws NullPointerException if {@code value} is {@code null}
     */
    Value encode(Object value);

    /**
     * Encode a null value.
     *
     * @param type  the type of the column
     * @return the encoded value
     * @throws NullPointerException if {@code value} is {@code null}
     */
    Value encodeNull(Type type);
}
