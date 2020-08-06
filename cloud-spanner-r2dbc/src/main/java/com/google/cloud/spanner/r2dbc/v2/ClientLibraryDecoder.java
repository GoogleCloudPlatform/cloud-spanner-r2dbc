package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.AbstractStructReader;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ClientLibraryDecoder {
  private static final Map<Type, BiFunction<Struct, Integer, Object>> decodersMap = createDecoders();

  private static final Map<Type, BiFunction<Struct, Integer, Object>> arrayDecodersMap =
      createArrayDecoders();

  // Only 3 primitive array types are supported by client library.
  // Struct type is the same for arrays and lists, so array getters have to live in a separate map.
  private static Map<Type, BiFunction<Struct, Integer, Object>> createArrayDecoders() {
    Map<Type, BiFunction<Struct, Integer, Object>> decoders = new HashMap<>();
    decoders.put(Type.array(Type.int64()), AbstractStructReader::getLongArray);
    decoders.put(Type.array(Type.float64()), AbstractStructReader::getDoubleArray);
    decoders.put(Type.array(Type.bool()), AbstractStructReader::getBooleanArray);
    return decoders;
  }

  private static Map<Type, BiFunction<Struct, Integer, Object>> createDecoders() {
    Map<Type, BiFunction<Struct, Integer, Object>> decoders = new HashMap<>();
    decoders.put(Type.int64(), AbstractStructReader::getLong);
    decoders.put(Type.array(Type.int64()), AbstractStructReader::getLongList);

    decoders.put(Type.float64(), AbstractStructReader::getDouble);
    decoders.put(Type.array(Type.float64()), AbstractStructReader::getDoubleList);

    decoders.put(Type.bool(), AbstractStructReader::getBoolean);
    decoders.put(Type.array(Type.bool()), AbstractStructReader::getBooleanList);

    decoders.put(Type.bytes(), AbstractStructReader::getBytes);
    decoders.put(Type.array(Type.bytes()), AbstractStructReader::getBytesList);

    decoders.put(Type.date(), AbstractStructReader::getDate);
    decoders.put(Type.array(Type.date()), AbstractStructReader::getDateList);

    decoders.put(Type.string(), AbstractStructReader::getString);
    decoders.put(Type.array(Type.string()), AbstractStructReader::getStringList);

    decoders.put(Type.timestamp(), AbstractStructReader::getTimestamp);
    decoders.put(Type.array(Type.timestamp()), AbstractStructReader::getTimestampList);

    return decoders;
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
        type.isArray() ? arrayDecodersMap : decodersMap;
    Object value =
        struct.isNull(index)
            ? null
            : selectedCodecsMap.get(struct.getColumnType(index)).apply(struct, index);

    return (T) value;
  }
}