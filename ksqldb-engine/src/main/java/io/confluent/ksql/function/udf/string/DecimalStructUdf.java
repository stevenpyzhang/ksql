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
import java.util.List;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

@UdfDescription(name = "DecimalStructUdf", description = "return a decimal")
public class DecimalStructUdf {
  
  @Udf(schema = "STRUCT<VAL DECIMAL(64,2)>")
  public Struct getDecimalStruct() {
    System.out.println("we ever get here?");
    final Schema schema = SchemaBuilder.struct()
            .optional()
            .field("VAL",
                    Decimal.builder(2).optional().parameter("connect.decimal.precision",
                            "64").build())
            .build();

    Struct struct = new Struct(schema);
    struct.put("VAL", BigDecimal.valueOf(123.45).setScale(2, RoundingMode.CEILING));
    return struct;
  }

  @Udf(schema = "STRUCT<VAL INTEGER)>")
  public Struct getDecimalStruct(
      @UdfParameter(description = "int", value = "int")final int val) {
    final Schema schema = SchemaBuilder.struct()
            .optional()
            .field("VAL", Schema.INT32_SCHEMA)
            .build();

    Struct struct = new Struct(schema);
    struct.put("VAL", val);
    return struct;
  }

  @Udf(schema = "STRUCT<A VARCHAR>")
  public Struct getDecimalStruct(
      @UdfParameter(description = "varchar", value = "val") final String value) {
    final Schema schema = SchemaBuilder.struct()
            .optional()
            .field("A", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    return new Struct(schema).put("A", value);
  }
}
