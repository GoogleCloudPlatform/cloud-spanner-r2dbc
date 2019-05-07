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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;

final class BytesCodec extends AbstractCodec<ByteArray> {

    BytesCodec() {
        super(ByteArray.class);
    }

    @Override
    boolean doCanDecode(Type dataType) {
        return dataType.equals(Type.bytes());
    }

    @Override
    ByteArray doDecode(Struct row, int index, Class<? extends ByteArray> type) {
        return row.getBytes(index);
    }

    @Override
    Value doEncode(ByteArray value) {
        return Value.bytes(value);
    }
}
