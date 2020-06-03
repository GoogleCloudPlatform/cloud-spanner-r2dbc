package com.google.cloud.spanner.r2dbc.v2;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.cloud.spanner.AbstractStructReader;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

public class ClientLibraryCodec {
	private static Map<Type, BiFunction<Struct, Integer, Object>> getters = new HashMap<>();
	static {
		getters.put(Type.int64(), AbstractStructReader::getLong);
		getters.put(Type.array(Type.int64()), AbstractStructReader::getLongList);

		getters.put(Type.float64(), AbstractStructReader::getDouble);
		getters.put(Type.array(Type.float64()), AbstractStructReader::getDoubleList);

		getters.put(Type.bool(), AbstractStructReader::getBoolean);
		getters.put(Type.array(Type.bool()), AbstractStructReader::getBooleanList);

		getters.put(Type.bytes(), AbstractStructReader::getBytes);
		getters.put(Type.array(Type.bytes()), AbstractStructReader::getBytesList);

		getters.put(Type.date(), AbstractStructReader::getDate);
		getters.put(Type.array(Type.date()), AbstractStructReader::getDateList);

		getters.put(Type.string(), AbstractStructReader::getString);
		getters.put(Type.array(Type.string()), AbstractStructReader::getStringList);

		getters.put(Type.timestamp(), AbstractStructReader::getTimestamp);
		getters.put(Type.array(Type.timestamp()), AbstractStructReader::getTimestampList);
	}

	public static  <T> T decode(Struct struct, int index, Class<T> type) {
		Object value = struct.isNull(index)
				? null
				: getters.get(struct.getColumnType(index)).apply(struct, index);
		return (T) value;
	}
}
