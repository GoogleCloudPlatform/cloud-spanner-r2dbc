/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc.codecs;

import com.google.cloud.spanner.Type;
import java.util.Arrays;
import java.util.List;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * The default {@link Codecs} implementation. Delegates to type-specific codec
 * implementations.
 */
public final class DefaultCodecs implements Codecs {

	private final List<Codec<?>> codecs;

	DefaultCodecs() {
		this.codecs = Arrays.asList(
				new ArrayBoolCodec(),
				new ArrayBytesCodec(),
				new ArrayDateCodec(),
				new ArrayFloat64Codec(),
				new ArrayInt64Codec(),
				new ArraySqlTimestampCodec(),
				new ArrayStringCodec(),
				new ArrayTimestampCodec(),
				new BooleanCodec(),
				new BytesCodec(),
				new DateCodec(),
				new Float64Codec(),
				new Int64Codec(),
				new SqlTimestampCodec(),
				new StringCodec(),
				new TimestampCodec());
	}

	@Override
	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T decode(Struct row, Object identifier, Class<? extends T> type) {
		Assert.requireNonNull(type, "type must not be null");
		Assert.requireNonNull(row, "row must not be null");
		Assert.requireNonNull(row, "identifier must not be null");

		Integer index;
		if (identifier instanceof Integer) {
			index = (Integer) identifier;
		}
		else if (identifier instanceof String) {
			index = row.getColumnIndex((String) identifier);
		}
		else {
			throw new IllegalArgumentException("identifier must be a String or an Integer");
		}

		for (Codec<?> codec : this.codecs) {
			if (codec.canDecode(row.getColumnType(index), type)) {
				return ((Codec<T>) codec).decode(row, index, type);
			}
		}

		throw new IllegalArgumentException(String.format("Cannot decode value of type %s", type.getName()));
	}

	@Override
	public Value encode(Object value) {
		for (Codec<?> codec : this.codecs) {
			if (codec.canEncode(value)) {
				return codec.encode(value);
			}
		}

		throw new IllegalArgumentException(
				String.format("Cannot encode parameter of type %s", value.getClass().getName()));
	}

	@Override
	public Value encodeNull(Type type) {
		for (Codec<?> codec : this.codecs) {
			if (codec.canEncodeNull(type)) {
				return codec.encodeNull();
			}
		}

		throw new IllegalArgumentException(String.format("Cannot encode null parameter of type %s", type));
	}
}
