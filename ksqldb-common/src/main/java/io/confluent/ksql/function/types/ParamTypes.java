/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.types;

import static io.confluent.ksql.schema.ksql.SchemaConverters.functionToSqlBaseConverter;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Map.Entry;
import java.util.Optional;

public final class ParamTypes {

  private ParamTypes() {
  }

  public static final BooleanType BOOLEAN = BooleanType.INSTANCE;
  public static final IntegerType INTEGER = IntegerType.INSTANCE;
  public static final DoubleType DOUBLE = DoubleType.INSTANCE;
  public static final StringType STRING = StringType.INSTANCE;
  public static final LongType LONG = LongType.INSTANCE;
  public static final ParamType DECIMAL = DecimalType.INSTANCE;
  public static final TimestampType TIMESTAMP = TimestampType.INSTANCE;

  public static boolean areCompatible(final SqlType actual, final ParamType declared) {
    return areCompatible(SqlArgument.of(actual, null), declared, false);
  }

  public static boolean areCompatible(
      final SqlArgument argument,
      final ParamType declared,
      final boolean allowCast
  ) {
    SqlType actual = argument.getSqlType();
    SqlLambda sqlLambda = argument.getSqlLambda();
    if (actual.baseType() == SqlBaseType.ARRAY && declared instanceof ArrayType) {
      return areCompatible(
          SqlArgument.of(((SqlArray) actual).getItemType(), null),
          ((ArrayType) declared).element(),
          allowCast);
    }

    if (actual.baseType() == SqlBaseType.MAP && declared instanceof MapType) {
      final SqlMap sqlType = (SqlMap) actual;
      final MapType mapType = (MapType) declared;
      return areCompatible(SqlArgument.of(sqlType.getKeyType(), null), mapType.key(), allowCast)
          && areCompatible(SqlArgument.of(sqlType.getValueType(), null), mapType.value(), allowCast);
    }

    if (actual.baseType() == SqlBaseType.STRUCT && declared instanceof StructType) {
      return isStructCompatible(actual, declared);
    }

    if (sqlLambda != null && declared instanceof LambdaType) {
      final LambdaType declaredLambda = (LambdaType) declared;
      if (sqlLambda.getInputType().size() != declaredLambda.inputTypes().size()) {
        return false;
      }
      int i = 0;
      for (final ParamType paramType: declaredLambda.inputTypes()) {
        if (!areCompatible(sqlLambda.getInputType().get(i), paramType)) {
          return false;
        }
        i++;
      }
      return areCompatible(sqlLambda.getReturnType(), declaredLambda.returnType());
    }

    return isPrimitiveMatch(actual, declared, allowCast);
  }

  private static boolean isStructCompatible(final SqlType actual, final ParamType declared) {
    final SqlStruct actualStruct = (SqlStruct) actual;

    // consider a struct that is empty to match any other struct
    if (actualStruct.fields().isEmpty() || ((StructType) declared).getSchema().isEmpty()) {
      return true;
    }

    for (final Entry<String, ParamType> entry : ((StructType) declared).getSchema().entrySet()) {
      final String k = entry.getKey();
      final Optional<Field> field = actualStruct.field(k);
      // intentionally do not allow implicit casting within structs
      if (!field.isPresent() || !areCompatible(field.get().type(), entry.getValue())) {
        return false;
      }
    }
    return actualStruct.fields().size() == ((StructType) declared).getSchema().size();
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static boolean isPrimitiveMatch(
      final SqlType actual,
      final ParamType declared,
      final boolean allowCast
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    final SqlBaseType base = actual.baseType();
    return base == SqlBaseType.STRING   && declared instanceof StringType
        || base == SqlBaseType.INTEGER  && declared instanceof IntegerType
        || base == SqlBaseType.BIGINT   && declared instanceof LongType
        || base == SqlBaseType.BOOLEAN  && declared instanceof BooleanType
        || base == SqlBaseType.DOUBLE   && declared instanceof DoubleType
        || base == SqlBaseType.DECIMAL  && declared instanceof DecimalType
        || base == SqlBaseType.TIMESTAMP  && declared instanceof TimestampType
        || allowCast && base.canImplicitlyCast(functionToSqlBaseConverter().toBaseType(declared));
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
  }
}
