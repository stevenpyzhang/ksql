/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.ExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.function.UdafUtil;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VisitorUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExpressionTypeManager {

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;

  public ExpressionTypeManager(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public SqlType getExpressionSqlType(final Expression expression) {
    return getExpressionSqlType(
        expression,
        Collections.emptyMap(),
        Collections.emptyList()
    );
  }

  public SqlType getExpressionSqlType(
      final Expression expression,
      final Map<String, SqlType> inputMapping,
      final List<SqlType> inputTypes
  ) {
    final ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    expressionTypeContext.setLambdaTypes(inputMapping);
    expressionTypeContext.setInputTypes(inputTypes);
    new Visitor().process(expression, expressionTypeContext);
    final SqlType newSqlType = expressionTypeContext.getSqlType();
    return newSqlType;
  }

  private static class ExpressionTypeContext {

    private SqlType sqlType;
    private List<SqlType> inputTypes = new ArrayList<>();
    private Map<String, SqlType> lambdaTypeMapping = new HashMap<>();

    SqlType getSqlType() {
      return sqlType;
    }

    void setSqlType(final SqlType sqlType) {
      this.sqlType = sqlType;
    }

    List<SqlType> getInputTypes() {
      if (inputTypes.size() == 0) {
        return null;
      }
      return inputTypes;
    }

    void addInputType(final SqlType inputType) {
      this.inputTypes.add(inputType);
    }

    void setInputTypes(final List<SqlType> inputTypes) {
      if (inputTypes != null) {
        this.inputTypes = new ArrayList<>(inputTypes);
      }
    }

    void mapInputTypes(final List<String> argumentList) {
      for (int i = 0; i < inputTypes.size(); i++) {
        this.lambdaTypeMapping.putIfAbsent(argumentList.get(i), inputTypes.get(i));
      }
    }

    void setLambdaTypes(final Map<String, SqlType> mappings) {
      this.lambdaTypeMapping = new HashMap<>(mappings);
    }

    SqlType getLambdaType(final String name) {
      return lambdaTypeMapping.get(name);
    }
  }

  private class Visitor implements ExpressionVisitor<Void, ExpressionTypeContext> {

    @Override
    public Void visitArithmeticBinary(
        final ArithmeticBinaryExpression node, final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getLeft(), expressionTypeContext);
      final SqlType leftType = expressionTypeContext.getSqlType();

      process(node.getRight(), expressionTypeContext);
      final SqlType rightType = expressionTypeContext.getSqlType();

      final SqlType resultType = node.getOperator().resultType(leftType, rightType);

      expressionTypeContext.setSqlType(resultType);
      return null;
    }

    @Override
    public Void visitArithmeticUnary(
        final ArithmeticUnaryExpression node, final ExpressionTypeContext context
    ) {
      process(node.getValue(), context);
      return null;
    }

    @Override
    // CHECKSTYLE_RULES.OFF: TodoComment
    public Void visitLambdaExpression(
        final LambdaFunctionCall node, final ExpressionTypeContext context
    ) {
      //have access to arguments here, can map name to the input type based on the context passed in
      context.mapInputTypes(node.getArguments());
      process(node.getBody(), context);
      context.setSqlType(SqlLambda.of(context.getInputTypes(), context.getSqlType()));
      //context.setSqlType(SqlLambda.of(context.getInputType(), context.getSqlType()));
      return null;
    }

    @Override
    // CHECKSTYLE_RULES.OFF: TodoComment
    public Void visitLambdaVariable(
        final LambdaVariable node, final ExpressionTypeContext expressionTypeContext
    ) {
      // TODO: add proper type inference
      expressionTypeContext.setSqlType(expressionTypeContext.getLambdaType(node.getValue()));
      //expressionTypeContext.setSqlType(GenericType.of(node.getValue()));
      return null;
    }

    @Override
    public Void visitNotExpression(
        final NotExpression node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitCast(final Cast node, final ExpressionTypeContext expressionTypeContext) {
      expressionTypeContext.setSqlType(node.getType().getSqlType());
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node, final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getLeft(), expressionTypeContext);
      final SqlType leftSchema = expressionTypeContext.getSqlType();

      process(node.getRight(), expressionTypeContext);
      final SqlType rightSchema = expressionTypeContext.getSqlType();

      if (!ComparisonUtil.isValidComparison(leftSchema, node.getType(), rightSchema)) {
        throw new KsqlException("Cannot compare "
            + node.getLeft().toString() + " (" + leftSchema.toString() + ") to "
            + node.getRight().toString() + " (" + rightSchema.toString() + ") "
            + "with " + node.getType() + ".");
      }
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitBetweenPredicate(
        final BetweenPredicate node,
        final ExpressionTypeContext context
    ) {
      context.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node, final ExpressionTypeContext expressionTypeContext
    ) {
      final Optional<Column> possibleColumn = schema.findValueColumn(node.getColumnName());

      final Column schemaColumn = possibleColumn
          .orElseThrow(() -> new KsqlException("Unknown column " + node + "."));

      expressionTypeContext.setSqlType(schemaColumn.type());
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node, final ExpressionTypeContext expressionTypeContext
    ) {
      throw new IllegalStateException(
          "Qualified column references must be resolved to unqualified reference "
              + "before type can be resolved");
    }

    @Override
    public Void visitDereferenceExpression(
        final DereferenceExpression node, final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getBase(), expressionTypeContext);
      final SqlType sqlType = expressionTypeContext.getSqlType();
      if (!(sqlType instanceof SqlStruct)) {
        throw new IllegalStateException("Expected STRUCT type, got: " + sqlType);
      }

      final SqlStruct structType = (SqlStruct) sqlType;
      final String fieldName = node.getFieldName();

      final Field structField = structType
          .field(fieldName)
          .orElseThrow(() -> new KsqlException(
              "Could not find field '" + fieldName + "' in '" + node.getBase() + "'.")
          );

      expressionTypeContext.setSqlType(structField.type());
      return null;
    }

    @Override
    public Void visitStringLiteral(
        final StringLiteral node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.STRING);
      return null;
    }

    @Override
    public Void visitBooleanLiteral(
        final BooleanLiteral node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitLongLiteral(
        final LongLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BIGINT);
      return null;
    }

    @Override
    public Void visitIntegerLiteral(
        final IntegerLiteral node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.INTEGER);
      return null;
    }

    @Override
    public Void visitDoubleLiteral(
        final DoubleLiteral node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.DOUBLE);
      return null;
    }

    @Override
    public Void visitNullLiteral(final NullLiteral node, final ExpressionTypeContext context) {
      context.setSqlType(null);
      return null;
    }

    @Override
    public Void visitLikePredicate(
        final LikePredicate node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitIsNotNullPredicate(
        final IsNotNullPredicate node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitIsNullPredicate(
        final IsNullPredicate node, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitSearchedCaseExpression(
        final SearchedCaseExpression node, final ExpressionTypeContext context
    ) {
      final Optional<SqlType> whenType = validateWhenClauses(node.getWhenClauses(), context);

      final Optional<SqlType> defaultType = node.getDefaultValue()
          .map(ExpressionTypeManager.this::getExpressionSqlType);

      if (whenType.isPresent() && defaultType.isPresent()) {
        if (!whenType.get().equals(defaultType.get())) {
          throw new KsqlException("Invalid Case expression. "
              + "Type for the default clause should be the same as for 'THEN' clauses."
              + System.lineSeparator()
              + "THEN type: " + whenType.get() + "."
              + System.lineSeparator()
              + "DEFAULT type: " + defaultType.get() + "."
          );
        }

        context.setSqlType(whenType.get());
      } else if (whenType.isPresent()) {
        context.setSqlType(whenType.get());
      } else if (defaultType.isPresent()) {
        context.setSqlType(defaultType.get());
      } else {
        throw new KsqlException("Invalid Case expression. All case branches have NULL type");
      }
      return null;
    }

    @Override
    public Void visitSubscriptExpression(
        final SubscriptExpression node, final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getBase(), expressionTypeContext);
      final SqlType arrayMapType = expressionTypeContext.getSqlType();

      final SqlType valueType;
      if (arrayMapType instanceof SqlMap) {
        valueType = ((SqlMap) arrayMapType).getValueType();
      } else if (arrayMapType instanceof SqlArray) {
        valueType = ((SqlArray) arrayMapType).getItemType();
      } else {
        throw new UnsupportedOperationException("Unsupported container type: " + arrayMapType);
      }

      expressionTypeContext.setSqlType(valueType);
      return null;
    }

    @Override
    public Void visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final ExpressionTypeContext context
    ) {
      if (exp.getValues().isEmpty()) {
        throw new KsqlException(
            "Array constructor cannot be empty. Please supply at least one element "
                + "(see https://github.com/confluentinc/ksql/issues/4239).");
      }

      final SqlType elementType = CoercionUtil
          .coerceUserList(exp.getValues(), ExpressionTypeManager.this)
          .commonType()
          .orElseThrow(() -> new KsqlException("Cannot construct an array with all NULL elements "
              + "(see https://github.com/confluentinc/ksql/issues/4239). As a workaround, you may "
              + "cast a NULL value to the desired type."));

      context.setSqlType(SqlArray.of(elementType));
      return null;
    }

    @Override
    public Void visitCreateMapExpression(
        final CreateMapExpression exp,
        final ExpressionTypeContext context
    ) {
      final ImmutableMap<Expression, Expression> map = exp.getMap();
      if (map.isEmpty()) {
        throw new KsqlException("Map constructor cannot be empty. Please supply at least one key "
            + "value pair (see https://github.com/confluentinc/ksql/issues/4239).");
      }

      final SqlType keyType = CoercionUtil
          .coerceUserList(map.keySet(), ExpressionTypeManager.this)
          .commonType()
          .orElseThrow(() -> new KsqlException("Cannot construct a map with all NULL keys "
              + "(see https://github.com/confluentinc/ksql/issues/4239). As a workaround, you may "
              + "cast a NULL key to the desired type."));

      final SqlType valueType = CoercionUtil
          .coerceUserList(map.values(), ExpressionTypeManager.this)
          .commonType()
          .orElseThrow(() -> new KsqlException("Cannot construct a map with all NULL values "
              + "(see https://github.com/confluentinc/ksql/issues/4239). As a workaround, you may "
              + "cast a NULL value to the desired type."));

      context.setSqlType(SqlMap.of(keyType, valueType));
      return null;
    }

    @Override
    public Void visitStructExpression(
        final CreateStructExpression exp,
        final ExpressionTypeContext context
    ) {
      final Builder builder = SqlStruct.builder();

      for (final CreateStructExpression.Field field : exp.getFields()) {
        process(field.getValue(), context);
        builder.field(field.getName(), context.getSqlType());
      }

      context.setSqlType(builder.build());
      return null;
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public Void visitFunctionCall(
        final FunctionCall node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      if (functionRegistry.isAggregate(node.getName())) {
        final SqlType schema = node.getArguments().isEmpty()
            ? FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA
            : getExpressionSqlType(node.getArguments().get(0));

        // use an empty KsqlConfig here because the expression type
        // of an aggregate function does not depend on the configuration
        final AggregateFunctionInitArguments args =
            UdafUtil.createAggregateFunctionInitArgs(0, node, KsqlConfig.empty());

        final KsqlAggregateFunction<?,?,?> aggFunc = functionRegistry
            .getAggregateFunction(node.getName(), schema, args);

        expressionTypeContext.setSqlType(aggFunc.returnType());
        return null;
      }

      if (functionRegistry.isTableFunction(node.getName())) {
        final List<SqlType> argumentTypes = node.getArguments().isEmpty()
            ? ImmutableList.of(FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA)
            : node.getArguments().stream().map(ExpressionTypeManager.this::getExpressionSqlType)
                .collect(Collectors.toList());

        final KsqlTableFunction tableFunction = functionRegistry
            .getTableFunction(node.getName(), argumentTypes);

        expressionTypeContext.setSqlType(tableFunction.getReturnType(argumentTypes));
        return null;
      }

      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName());

      final List<SqlType> argTypes = new ArrayList<>();
      for (final Expression expression : node.getArguments()) {
        process(expression, expressionTypeContext);
        final SqlType newSqlType = expressionTypeContext.getSqlType();
        argTypes.add(newSqlType);
        /*argTypes.add(
            newSqlType == SqlTypes.LAMBDALITERAL ? expressionTypeContext.getInputType() : newSqlType
        );*/
        if (shouldSetInputType(node, expressionTypeContext)) {
          if (newSqlType instanceof SqlArray) {
            final SqlArray inputArray = (SqlArray) newSqlType;
            expressionTypeContext.addInputType(inputArray.getItemType());
          } else if (newSqlType instanceof SqlMap) {
            final SqlMap inputMap = (SqlMap) newSqlType;
            if (node.getName().equals(FunctionName.of("TRANFORM_KEY"))) {
              expressionTypeContext.addInputType(inputMap.getKeyType());
            } else if (node.getName().equals(FunctionName.of("TRANSFORM_VALUE"))) {
              expressionTypeContext.addInputType(inputMap.getValueType());
            } else {
              expressionTypeContext.addInputType(inputMap.getKeyType());
              expressionTypeContext.addInputType(inputMap.getValueType());
            }
          } else {
            expressionTypeContext.addInputType(newSqlType);
          }
        }
      }

      final SqlType returnSchema = udfFactory.getFunction(argTypes).getReturnType(argTypes);
      expressionTypeContext.setSqlType(returnSchema);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node, final ExpressionTypeContext context
    ) {
      process(node.getLeft(), context);
      process(node.getRight(), context);
      return null;
    }

    @Override
    public Void visitType(final Type type, final ExpressionTypeContext expressionTypeContext) {
      throw VisitorUtil.illegalState(this, type);
    }

    @Override
    public Void visitTimeLiteral(
        final TimeLiteral timeLiteral, final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, timeLiteral);
    }

    @Override
    public Void visitTimestampLiteral(
        final TimestampLiteral timestampLiteral, final ExpressionTypeContext context
    ) {
      context.setSqlType(SqlTypes.TIMESTAMP);
      return null;
    }

    @Override
    public Void visitDecimalLiteral(
        final DecimalLiteral decimalLiteral, final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(DecimalUtil.fromValue(decimalLiteral.getValue()));

      return null;
    }

    @Override
    public Void visitSimpleCaseExpression(
        final SimpleCaseExpression simpleCaseExpression,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, simpleCaseExpression);
    }

    @Override
    public Void visitInListExpression(
        final InListExpression inListExpression, final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, inListExpression);
    }

    @Override
    public Void visitInPredicate(
        final InPredicate inPredicate, final ExpressionTypeContext context
    ) {
      context.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitWhenClause(
        final WhenClause whenClause, final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.illegalState(this, whenClause);
    }

    private Optional<SqlType> validateWhenClauses(
        final List<WhenClause> whenClauses, final ExpressionTypeContext context
    ) {
      Optional<SqlType> previousResult = Optional.empty();
      for (final WhenClause whenClause : whenClauses) {
        process(whenClause.getOperand(), context);

        final SqlType operandType = context.getSqlType();

        if (operandType.baseType() != SqlBaseType.BOOLEAN) {
          throw new KsqlException("WHEN operand type should be boolean."
              + System.lineSeparator()
              + "Type for '" + whenClause.getOperand() + "' is " + operandType
          );
        }

        process(whenClause.getResult(), context);

        final SqlType resultType = context.getSqlType();
        if (resultType == null) {
          continue; // `null` type
        }

        if (!previousResult.isPresent()) {
          previousResult = Optional.of(resultType);
          continue;
        }

        if (!previousResult.get().equals(resultType)) {
          throw new KsqlException("Invalid Case expression. "
              + "Type for all 'THEN' clauses should be the same."
              + System.lineSeparator()
              + "THEN expression '" + whenClause + "' has type: " + resultType + "."
              + System.lineSeparator()
              + "Previous THEN expression(s) type: " + previousResult.get() + ".");
        }
      }

      return previousResult;
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private Boolean shouldSetInputType(
        final FunctionCall node,
        final ExpressionTypeContext context
    ) {
      return (context.getInputTypes() == null)
          || (node.getName().equals(FunctionName.of("MAP_REDUCE"))
              && context.getInputTypes().size() == 2)
          || (node.getName().equals(FunctionName.of("ARRAY_REDUCE"))
              && context.getInputTypes().size() == 1);
    }
  }
}
