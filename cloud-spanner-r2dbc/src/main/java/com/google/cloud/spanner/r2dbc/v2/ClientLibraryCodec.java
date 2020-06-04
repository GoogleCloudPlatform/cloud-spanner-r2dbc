package com.google.cloud.spanner.r2dbc.v2;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.cloud.spanner.AbstractStructReader;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

public class ClientLibraryCodec {
	private static Map<Type, BiFunction<Struct, Integer, Object>> codecsMap = createCodecsMap();

	private static Map<Type, BiFunction<Struct, Integer, Object>> createCodecsMap() {
		Map<Type, BiFunction<Struct, Integer, Object>> codecs = new HashMap<>();
		codecs.put(Type.int64(), AbstractStructReader::getLong);
		codecs.put(Type.array(Type.int64()), AbstractStructReader::getLongList);

		codecs.put(Type.float64(), AbstractStructReader::getDouble);
		codecs.put(Type.array(Type.float64()), AbstractStructReader::getDoubleList);

		codecs.put(Type.bool(), AbstractStructReader::getBoolean);
		codecs.put(Type.array(Type.bool()), AbstractStructReader::getBooleanList);

		codecs.put(Type.bytes(), AbstractStructReader::getBytes);
		codecs.put(Type.array(Type.bytes()), AbstractStructReader::getBytesList);

		codecs.put(Type.date(), AbstractStructReader::getDate);
		codecs.put(Type.array(Type.date()), AbstractStructReader::getDateList);

		codecs.put(Type.string(), AbstractStructReader::getString);
		codecs.put(Type.array(Type.string()), AbstractStructReader::getStringList);

		codecs.put(Type.timestamp(), AbstractStructReader::getTimestamp);
		codecs.put(Type.array(Type.timestamp()), AbstractStructReader::getTimestampList);

		return codecs;
	}

	public static  <T> T decode(Struct struct, int index, Class<T> type) {
		Object value = struct.isNull(index)
				? null
				: codecsMap.get(struct.getColumnType(index)).apply(struct, index);
		return (T) value;
	}
}
