/*
 * Copyright 2021-2021 Google LLC
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

package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.r2dbc.ConversionFailureException;

class StringToJsonConverter implements SpannerClientLibrariesConverter<JsonHolder> {

  @Override
  public boolean canConvert(Class<?> inputClass, Class<?> resultClass) {
    return inputClass == String.class && resultClass == JsonHolder.class;
  }

  @Override
  public JsonHolder convert(Object input) {
    if (!canConvert(input.getClass(), JsonHolder.class)) {
      throw new ConversionFailureException(
          String.format("Unable to convert %s to %s", ((Object) input.getClass()).getClass(),
                  JsonHolder.class));
    }
    return new JsonHolder((String) input);
  }
}
