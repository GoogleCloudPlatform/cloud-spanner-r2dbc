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

import java.util.Arrays;

import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;

final class ArrayDateCodec extends AbstractCodec<Date[]> {

    ArrayDateCodec() {
        super(Date[].class);
    }

    @Override
    boolean doCanDecode(Type dataType) {
        return dataType.equals(Type.array(Type.date()));
    }

    @Override
    Date[] doDecode(Struct row, int index, Class<? extends Date[]> type) {
        return row.getDateList(index).toArray(new Date[0]);
    }

    @Override
    Value doEncode(Date[] value) {
        return Value.dateArray(value == null ? null : Arrays.asList(value));
    }
}
