/*
 * Copyright 2021 Google LLC
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

package com.example;

import com.google.gson.Gson;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class CustomConfiguration extends AbstractR2dbcConfiguration {

  @Autowired
  ApplicationContext applicationContext;

  @Override
  public ConnectionFactory connectionFactory() {
    return null;
  }

  @Bean
  @Override
  public R2dbcCustomConversions r2dbcCustomConversions() {
    List<Converter<?, ?>> converters = new ArrayList<>();
    converters.add(applicationContext.getBean(JsonToMapConverter.class));
    converters.add(applicationContext.getBean(MapToJsonConverter.class));
    return new R2dbcCustomConversions(getStoreConversions(), converters);
  }

  @Bean
  public Gson gson() {
    return new Gson();
  }
}
