package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.schema.ksql.types.SqlType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeContext {
  private SqlType sqlType;
  private final List<SqlType> lambdaInputTypes = new ArrayList<>();
  private final Map<String, SqlType> lambdaInputTypeMapping = new HashMap<>();

  public SqlType getSqlType() {
    return sqlType;
  }

  public void setSqlType(final SqlType sqlType) {
    this.sqlType = sqlType;
  }

  public List<SqlType> getLambdaInputTypes() {
    if (lambdaInputTypes.size() == 0) {
      return null;
    }
    return lambdaInputTypes;
  }

  public void addLambdaInputType(final SqlType inputType) {
    this.lambdaInputTypes.add(inputType);
  }

  public void mapLambdaInputTypes(final List<String> argumentList) {
    if (lambdaInputTypes.size() != argumentList.size()) {
      throw new IllegalArgumentException("Was expecting " +
          lambdaInputTypes.size() +
          " arguments but found " +
          argumentList.size() + "," + argumentList +
          ". Check your lambda statement.");
    }
    for (int i = 0; i < argumentList.size(); i++) {
      this.lambdaInputTypeMapping.putIfAbsent(argumentList.get(i), lambdaInputTypes.get(i));
    }
  }

  public SqlType getLambdaType(final String name) {
    return lambdaInputTypeMapping.get(name);
  }

  public Boolean notAllInputsSeen() {
    return lambdaInputTypeMapping.size() != lambdaInputTypes.size() || lambdaInputTypes.size() == 0;
  }
}
