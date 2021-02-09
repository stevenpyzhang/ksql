package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.schema.ksql.types.SqlType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeContext {

  private final List<SqlType> inputTypes = new ArrayList<>();
  private final Map<String, SqlType> lambdaTypeMapping = new HashMap<>();

  List<SqlType> getInputTypes() {
    if (inputTypes.size() == 0) {
      return null;
    }
    return inputTypes;
  }

  void addInputType(final SqlType inputType) {
    this.inputTypes.add(inputType);
  }

  void mapInputTypes(final List<String> argumentList){
    for (int i = 0; i < argumentList.size(); i++) {
      this.lambdaTypeMapping.putIfAbsent(argumentList.get(i), inputTypes.get(i));
    }
  }

  SqlType getLambdaType(final String name) {
    return lambdaTypeMapping.get(name);
  }

  Map<String, SqlType> getLambdaTypeMapping() {
    return this.lambdaTypeMapping;
  }
}
