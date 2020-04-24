/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name = "DecimalArrayUdf", description = "return a decimal")
public class DecimalArrayUdf {

  @Udf(schema = "ARRAY<DECIMAL(64,2)>")
  public List<BigDecimal> getArray() {
    Decimal.builder(2).optional().parameter("connect.decimal.precision",
            "64").build();
    
    final List<BigDecimal> list = new ArrayList<>();
    list.add(BigDecimal.valueOf(123.45).setScale(2, RoundingMode.CEILING));
    return list;
  }

//  @UdfSchemaProvider
//  public List<SqlDecimal> decimalProvider(List<SqlType> types) {
//    List<SqlDecimal> list = new ArrayList<>();
//    SqlDecimal.of(64, 2);
//    return new ArrayList<>();
//  }
}
