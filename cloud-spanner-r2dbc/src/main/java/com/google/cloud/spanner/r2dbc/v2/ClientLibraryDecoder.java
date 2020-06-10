package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.AbstractStructReader;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ClientLibraryDecoder {
  private static Map<Type, BiFunction<Struct, Integer, Object>> codecsMap = createCodecsMap();

  private static Map<Type, BiFunction<Struct, Integer, Object>> arrayCodecsMap =
      createArrayCodecsMap();

  private static Map<Type, BiFunction<Struct, Integer, Object>> createArrayCodecsMap() {
    Map<Type, BiFunction<Struct, Integer, Object>> codecs = new HashMap<>();
    codecs.put(Type.array(Type.int64()), AbstractStructReader::getLongArray);
    codecs.put(Type.array(Type.float64()), AbstractStructReader::getDoubleArray);
    codecs.put(Type.array(Type.bool()), AbstractStructReader::getBooleanArray);
    return codecs;
  }

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

  /**
   * Decodes result given index and type.
   *
   * @param struct the result struct
   * @param index the index of the result to decode
   * @param type the type of the result
   * @param <T> the type of the result
   * @return decoded value
   */
  public static <T> T decode(Struct struct, int index, Class<T> type) {
    Map<Type, BiFunction<Struct, Integer, Object>> selectedCodecsMap =
        type.isArray() ? arrayCodecsMap : codecsMap;
    Object value =
        struct.isNull(index)
            ? null
            : selectedCodecsMap.get(struct.getColumnType(index)).apply(struct, index);

    return (T) value;
  }
}
