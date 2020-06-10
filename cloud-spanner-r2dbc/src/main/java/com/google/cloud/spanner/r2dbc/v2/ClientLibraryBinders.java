package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.r2dbc.statement.TypedNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class ClientLibraryBinders {
  private static List<ClientLibraryBinder> binders = buildEncoders();

  private static List<ClientLibraryBinder> buildEncoders() {
    List<ClientLibraryBinder> binders = new ArrayList<>();
    binders.add(new ClientLibraryValueBinder(Long.class, (binder, val) -> binder.to((Long) val)));
    binders.add(
        new ClientLibraryValueBinder(Double.class, (binder, val) -> binder.to((Double) val)));
    binders.add(
        new ClientLibraryValueBinder(Boolean.class, (binder, val) -> binder.to((Boolean) val)));
    binders.add(
        new ClientLibraryValueBinder(ByteArray.class, (binder, val) -> binder.to((ByteArray) val)));
    binders.add(new ClientLibraryValueBinder(Date.class, (binder, val) -> binder.to((Date) val)));
    binders.add(
        new ClientLibraryValueBinder(String.class, (binder, val) -> binder.to((String) val)));
    binders.add(
        new ClientLibraryValueBinder(Timestamp.class, (binder, val) -> binder.to((Timestamp) val)));

    // There is technically one more supported type -  binder.to(Type type, @Nullable Struct value),
    // but it is not clear how r2dbc could pass both the type and the value

    return binders;
  }

  static void bind(Statement.Builder builder, String name, Object value) {
    Class<?> valueClass = isTypedNull(value) ? ((TypedNull) value).getType() : value.getClass();

    Optional<ClientLibraryBinder> encoderOptional =
        binders.stream().filter(e -> e.canEncode(valueClass)).findFirst();
    if (!encoderOptional.isPresent()) {
      throw new RuntimeException("Can't find an encoder for type: " + valueClass);
    }
    encoderOptional.get().bind(builder, name, isTypedNull(value) ? null : value);
  }

  private static boolean isTypedNull(Object value) {
    return value.getClass().equals(TypedNull.class);
  }
}
