package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.ValueBinder;
import com.google.cloud.spanner.r2dbc.util.Assert;
import java.util.function.BiConsumer;

public class ClientLibraryValueBinder implements ClientLibraryBinder {

  private Class type;

  private BiConsumer<ValueBinder, Object> bindingConsumer;

  public ClientLibraryValueBinder(Class type, BiConsumer<ValueBinder, Object> bindingConsumer) {
    this.type = type;
    this.bindingConsumer = bindingConsumer;
  }

  @Override
  public boolean canEncode(Class type) {
    Assert.requireNonNull(type, "type to encode must not be null");

    return this.type.isAssignableFrom(type);
  }

  @Override
  public void bind(Builder builder, String name, Object value) {
    bindingConsumer.accept(builder.bind(name), value);
  }
}
