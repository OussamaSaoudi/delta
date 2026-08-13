/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.*;
import static io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo;
import static io.delta.kernel.internal.util.ExpressionUtils.*;
import static io.delta.kernel.internal.util.PartitionUtils.isSupportedPartitionValueType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultViewVector;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.util.GeometryUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ExpressionEvaluator} for default {@link ExpressionHandler}. It takes
 * care of validating, adding necessary implicit casts and evaluating the {@link Expression} on
 * given {@link ColumnarBatch}.
 */
public class DefaultExpressionEvaluator implements ExpressionEvaluator {
  private final Expression expression;
  private final DataType outputType;

  /**
   * Create a {@link DefaultExpressionEvaluator} instance bound to the given expression and
   * <i>inputSchem</i>.
   *
   * @param inputSchema Input data schema
   * @param expression Expression to evaluate.
   * @param outputType Expected result data type.
   */
  public DefaultExpressionEvaluator(
      StructType inputSchema, Expression expression, DataType outputType) {
    ExpressionTransformResult transformResult =
        new ExpressionTransformer(inputSchema).transform(expression, outputType);
    if (!transformResult.outputType.equivalent(outputType)) {
      String reason =
          String.format(
              "Expression %s does not match expected output type %s", expression, outputType);
      throw unsupportedExpressionException(expression, reason);
    }
    this.expression = transformResult.expression;
    this.outputType = outputType;
  }

  @Override
  public ColumnVector eval(ColumnarBatch input) {
    return new ExpressionEvalVisitor(input).eval(expression, outputType);
  }

  @Override
  public void close() {
    /* nothing to close */
  }

  /** Encapsulates the result of {@link ExpressionTransformer} */
  private static class ExpressionTransformResult {
    public final Expression expression; // transformed expression
    public final DataType outputType; // output type of the expression

    ExpressionTransformResult(Expression expression, DataType outputType) {
      this.expression = expression;
      this.outputType = outputType;
    }
  }

  /** A coalesce whose children are transformed and evaluated only when execution reaches them. */
  private static class DeferredCoalesceExpression extends ScalarExpression {
    private final DataType outputType;

    DeferredCoalesceExpression(List<Expression> children, DataType outputType) {
      super("COALESCE", children);
      this.outputType = requireNonNull(outputType, "outputType is null");
    }

    DataType getOutputType() {
      return outputType;
    }
  }

  /**
   * Implementation of {@link ExpressionVisitor} to validate the given expression as follows.
   *
   * <ul>
   *   <li>given input column is part of the input data schema
   *   <li>expression inputs are of supported types. Insert cast according to the rules in {@link
   *       ImplicitCastExpression} to make the types compatible for evaluation by {@link
   *       ExpressionEvalVisitor}
   * </ul>
   *
   * <p>Return type of each expression visit is a tuple of new rewritten expression and its result
   * data type.
   */
  private static class ExpressionTransformer extends ExpressionVisitor<ExpressionTransformResult> {
    private StructType inputDataSchema;
    private DataType expectedType;

    ExpressionTransformer(StructType inputDataSchema) {
      this.inputDataSchema = requireNonNull(inputDataSchema, "inputDataSchema is null");
    }

    ExpressionTransformResult transform(Expression expression, DataType expectedType) {
      DataType previousExpectedType = this.expectedType;
      this.expectedType = expectedType;
      try {
        if (expression instanceof StructExpression) {
          return transformStruct((StructExpression) expression, expectedType);
        }
        if (expression instanceof StructPatch) {
          return transformStructPatch((StructPatch) expression, expectedType);
        }
        if (expression instanceof ParseJson) {
          return transformParseJson((ParseJson) expression, expectedType);
        }
        if (expression instanceof MapToStruct) {
          return transformMapToStruct((MapToStruct) expression, expectedType);
        }
        if (expression instanceof ScalarExpression
            && ((ScalarExpression) expression).getName().equalsIgnoreCase("COALESCE")) {
          return transformCoalesce((ScalarExpression) expression, expectedType);
        }
        return visit(expression);
      } finally {
        this.expectedType = previousExpectedType;
      }
    }

    private ExpressionTransformResult transformChild(Expression expression) {
      return transform(expression, null);
    }

    private ExpressionTransformResult transformMapToStruct(
        MapToStruct mapToStruct, DataType expectedType) {
      if (!(expectedType instanceof StructType)) {
        throw unsupportedExpressionException(
            mapToStruct,
            String.format("MapToStruct expects a StructType output, but got %s", expectedType));
      }
      StructType outputType = (StructType) expectedType;
      for (StructField field : outputType.fields()) {
        if (!isSupportedPartitionValueType(field.getDataType())) {
          throw unsupportedExpressionException(
              mapToStruct,
              String.format(
                  "MapToStruct only supports primitive partition types, but field %s has type %s",
                  field.getName(), field.getDataType()));
        }
      }

      ExpressionTransformResult map = transformChild(mapToStruct.getMapExpression());
      if (!(map.outputType instanceof MapType)
          || !((MapType) map.outputType).getKeyType().equivalent(StringType.STRING)
          || !((MapType) map.outputType).getValueType().equivalent(StringType.STRING)) {
        throw unsupportedExpressionException(
            mapToStruct,
            String.format(
                "MapToStruct expects map(string, string) input, but got %s", map.outputType));
      }
      return new ExpressionTransformResult(
          new ResolvedMapToStruct(map.expression, outputType), outputType);
    }

    private ExpressionTransformResult transformCoalesce(
        ScalarExpression coalesce, DataType expectedType) {
      if (coalesce.getChildren().isEmpty()) {
        throw unsupportedExpressionException(coalesce, "Coalesce requires at least one expression");
      }
      DataType outputType =
          expectedType == null
              ? transformChild(coalesce.getChildren().get(0)).outputType
              : expectedType;
      return new ExpressionTransformResult(
          new DeferredCoalesceExpression(coalesce.getChildren(), outputType), outputType);
    }

    private ExpressionTransformResult transformParseJson(
        ParseJson parseJson, DataType expectedType) {
      if (!(expectedType instanceof StructType)
          || !parseJson.getOutputSchema().equals(expectedType)) {
        throw unsupportedExpressionException(
            parseJson,
            String.format(
                "ParseJson output schema %s does not match expected output type %s",
                parseJson.getOutputSchema(), expectedType));
      }
      ExpressionTransformResult json = transformChild(parseJson.getJsonExpression());
      if (!(json.outputType instanceof StringType)) {
        throw unsupportedExpressionException(
            parseJson,
            String.format("ParseJson expects string input, but got %s", json.outputType));
      }
      return new ExpressionTransformResult(
          new ParseJson(json.expression, parseJson.getOutputSchema()), parseJson.getOutputSchema());
    }

    private ExpressionTransformResult transformStruct(
        StructExpression struct, DataType expectedType) {
      if (!(expectedType instanceof StructType)) {
        throw unsupportedExpressionException(
            struct,
            String.format(
                "Struct expression expects a StructType output, but got %s", expectedType));
      }

      StructType structType = (StructType) expectedType;
      if (struct.getFieldExpressions().size() != structType.length()) {
        throw unsupportedExpressionException(
            struct,
            String.format(
                "Struct expression field count mismatch: %s fields in expression but %s in schema",
                struct.getFieldExpressions().size(), structType.length()));
      }

      List<Expression> fields = new ArrayList<>(structType.length());
      for (int ordinal = 0; ordinal < structType.length(); ordinal++) {
        StructField field = structType.at(ordinal);
        ExpressionTransformResult result =
            transform(struct.getFieldExpressions().get(ordinal), field.getDataType());
        if (!result.outputType.equals(field.getDataType())) {
          throw unsupportedExpressionException(
              struct,
              String.format(
                  "Struct field %s type mismatch: expected %s but got %s",
                  field.getName(), field.getDataType(), result.outputType));
        }
        fields.add(result.expression);
      }

      Optional<Expression> predicate = Optional.empty();
      if (struct.getNullabilityPredicate().isPresent()) {
        ExpressionTransformResult result =
            transform(struct.getNullabilityPredicate().get(), BooleanType.BOOLEAN);
        if (!BooleanType.BOOLEAN.equals(result.outputType)) {
          throw unsupportedExpressionException(
              struct,
              String.format(
                  "Struct nullability predicate must be boolean, but got %s", result.outputType));
        }
        predicate = Optional.of(result.expression);
      }

      StructExpression transformed =
          predicate
              .map(value -> new StructExpression(fields, value))
              .orElseGet(() -> new StructExpression(fields));
      return new ExpressionTransformResult(transformed, structType);
    }

    private ExpressionTransformResult transformStructPatch(
        StructPatch patch, DataType expectedType) {
      if (!(expectedType instanceof StructType)) {
        throw unsupportedExpressionException(
            patch,
            String.format("Struct patch expects a StructType output, but got %s", expectedType));
      }

      StructType sourceType = resolvePatchSourceType(patch);
      List<Expression> fields = new ArrayList<>(patch.getPrependedFields());
      Set<String> usedFieldTransforms = new HashSet<>();
      for (StructField sourceField : sourceType.fields()) {
        StructPatch.FieldTransform fieldTransform =
            patch.getFieldTransforms().get(sourceField.getName());
        if (fieldTransform == null || !fieldTransform.isReplace()) {
          fields.add(sourceColumn(patch, sourceField.getName()));
        }
        if (fieldTransform != null) {
          fields.addAll(fieldTransform.getExpressions());
          usedFieldTransforms.add(sourceField.getName());
        }
      }
      patch
          .getFieldTransforms()
          .forEach(
              (fieldName, fieldTransform) -> {
                if (!fieldTransform.isOptional() && !usedFieldTransforms.contains(fieldName)) {
                  throw unsupportedExpressionException(
                      patch, "Required struct patch field does not exist: " + fieldName);
                }
              });
      fields.addAll(patch.getAppendedFields());

      StructExpression densePatch =
          patch
              .getInputPath()
              .<StructExpression>map(
                  path -> new StructExpression(fields, new Predicate("IS_NOT_NULL", path)))
              .orElseGet(() -> new StructExpression(fields));
      return transformStruct(densePatch, expectedType);
    }

    private StructType resolvePatchSourceType(StructPatch patch) {
      if (!patch.getInputPath().isPresent()) {
        return inputDataSchema;
      }
      ExpressionTransformResult inputPath = visitColumn(patch.getInputPath().get());
      if (!(inputPath.outputType instanceof StructType)) {
        throw unsupportedExpressionException(
            patch, "Struct patch input path does not point to a struct");
      }
      return (StructType) inputPath.outputType;
    }

    private Column sourceColumn(StructPatch patch, String fieldName) {
      return patch
          .getInputPath()
          .map(path -> path.appendNestedField(fieldName))
          .orElseGet(() -> new Column(fieldName));
    }

    @Override
    ExpressionTransformResult visitAnd(And and) {
      Predicate left = validateIsPredicate(and, transformChild(and.getLeft()));
      Predicate right = validateIsPredicate(and, transformChild(and.getRight()));
      return new ExpressionTransformResult(new And(left, right), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitOr(Or or) {
      Predicate left = validateIsPredicate(or, transformChild(or.getLeft()));
      Predicate right = validateIsPredicate(or, transformChild(or.getRight()));
      return new ExpressionTransformResult(new Or(left, right), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitAlwaysTrue(AlwaysTrue alwaysTrue) {
      // nothing to validate or rewrite.
      return new ExpressionTransformResult(alwaysTrue, BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitAlwaysFalse(AlwaysFalse alwaysFalse) {
      // nothing to validate or rewrite.
      return new ExpressionTransformResult(alwaysFalse, BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitComparator(Predicate predicate) {
      switch (predicate.getName()) {
        case "=":
        case ">":
        case ">=":
        case "<":
        case "<=":
        case "IS NOT DISTINCT FROM":
          return new ExpressionTransformResult(
              transformBinaryComparator(predicate), BooleanType.BOOLEAN);
        default:
          // We should never reach this based on the ExpressionVisitor
          throw new IllegalStateException(
              String.format("%s is not a recognized comparator", predicate.getName()));
      }
    }

    @Override
    ExpressionTransformResult visitLiteral(Literal literal) {
      // nothing to validate or rewrite
      return new ExpressionTransformResult(literal, literal.getDataType());
    }

    @Override
    ExpressionTransformResult visitColumn(Column column) {
      String[] names = column.getNames();
      DataType currentType = inputDataSchema;
      for (int level = 0; level < names.length; level++) {
        assertColumnExists(currentType instanceof StructType, inputDataSchema, column);
        StructType structSchema = ((StructType) currentType);
        int ordinal = structSchema.indexOf(names[level]);
        assertColumnExists(ordinal != -1, inputDataSchema, column);
        currentType = structSchema.at(ordinal).getDataType();
      }
      assertColumnExists(currentType != null, inputDataSchema, column);
      return new ExpressionTransformResult(column, currentType);
    }

    @Override
    ExpressionTransformResult visitStruct(StructExpression struct) {
      throw unsupportedExpressionException(
          struct, "A caller-supplied StructType is required to evaluate a struct expression");
    }

    @Override
    ExpressionTransformResult visitStructPatch(StructPatch structPatch) {
      throw unsupportedExpressionException(
          structPatch, "A caller-supplied StructType is required to evaluate a struct patch");
    }

    @Override
    ExpressionTransformResult visitParseJson(ParseJson parseJson) {
      return transformParseJson(parseJson, parseJson.getOutputSchema());
    }

    @Override
    ExpressionTransformResult visitMapToStruct(MapToStruct mapToStruct) {
      throw unsupportedExpressionException(
          mapToStruct, "A caller-supplied StructType is required to evaluate MapToStruct");
    }

    @Override
    ExpressionTransformResult visitResolvedMapToStruct(ResolvedMapToStruct mapToStruct) {
      throw new IllegalStateException("ResolvedMapToStruct is not valid evaluator input");
    }

    @Override
    ExpressionTransformResult visitCast(ImplicitCastExpression cast) {
      throw new UnsupportedOperationException("CAST expression is not expected.");
    }

    @Override
    ExpressionTransformResult visitBooleanExpression(BooleanExpression booleanExpression) {
      ExpressionTransformResult child =
          transform(booleanExpression.getExpression(), BooleanType.BOOLEAN);
      requireExactType(
          booleanExpression, child.outputType, BooleanType.BOOLEAN, "boolean expression");
      return new ExpressionTransformResult(
          new BooleanExpression(child.expression), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitJunction(Junction junction) {
      List<Predicate> predicates = new ArrayList<>();
      for (Predicate predicate : junction.getPredicates()) {
        ExpressionTransformResult child = transform(predicate, BooleanType.BOOLEAN);
        predicates.add(validateIsPredicate(junction, child));
      }
      return new ExpressionTransformResult(
          new Junction(junction.getOperator(), predicates), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitBinaryPredicate(BinaryPredicate predicate) {
      ExpressionTransformResult left = transformChild(predicate.getLeft());
      ExpressionTransformResult right = transformChild(predicate.getRight());
      requireExactType(predicate, left.outputType, right.outputType, "comparison operands");
      return new ExpressionTransformResult(
          new BinaryPredicate(predicate.getOperator(), left.expression, right.expression),
          BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitPartitionValue(PartitionValueExpression partitionValue) {
      ExpressionTransformResult serializedPartValueInput =
          transformChild(partitionValue.getInput());
      checkArgument(
          serializedPartValueInput.outputType instanceof StringType,
          "%s: expected string input, but got %s",
          partitionValue,
          serializedPartValueInput.outputType);
      DataType partitionColType = partitionValue.getDataType();
      if (partitionColType instanceof StructType
          || partitionColType instanceof ArrayType
          || partitionColType instanceof MapType) {
        throw unsupportedExpressionException(
            partitionValue, "unsupported partition data type: " + partitionColType);
      }
      return new ExpressionTransformResult(
          new PartitionValueExpression(serializedPartValueInput.expression, partitionColType),
          partitionColType);
    }

    @Override
    ExpressionTransformResult visitElementAt(ScalarExpression elementAt) {
      ExpressionTransformResult transformedMapInput = transformChild(childAt(elementAt, 0));
      ExpressionTransformResult transformedLookupKey = transformChild(childAt(elementAt, 1));

      ScalarExpression transformedExpression =
          ElementAtEvaluator.validateAndTransform(
              elementAt,
              transformedMapInput.expression,
              transformedMapInput.outputType,
              transformedLookupKey.expression,
              transformedLookupKey.outputType);

      return new ExpressionTransformResult(
          transformedExpression, ((MapType) transformedMapInput.outputType).getValueType());
    }

    @Override
    ExpressionTransformResult visitNot(Predicate predicate) {
      Predicate child =
          validateIsPredicate(predicate, transformChild(predicate.getChildren().get(0)));
      return new ExpressionTransformResult(
          new Predicate(predicate.getName(), child), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitIsNotNull(Predicate predicate) {
      Expression child = transformChild(predicate.getChildren().get(0)).expression;
      return new ExpressionTransformResult(
          new Predicate(predicate.getName(), child), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitIsNull(Predicate predicate) {
      Expression child = transformChild(getUnaryChild(predicate)).expression;
      return new ExpressionTransformResult(
          new Predicate(predicate.getName(), child), BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitCoalesce(ScalarExpression coalesce) {
      return transformCoalesce(coalesce, expectedType);
    }

    @Override
    ExpressionTransformResult visitArithmetic(ScalarExpression arithmetic) {
      List<ExpressionTransformResult> children =
          arithmetic.getChildren().stream().map(this::transformChild).collect(Collectors.toList());
      String operation = arithmetic.getName();
      if (children.size() != 2) {
        throw unsupportedExpressionException(
            arithmetic,
            format("%s requires exactly two arguments: left and right operands", operation));
      }
      DataType outputType = children.get(0).outputType;
      if (!outputType.equivalent(children.get(1).outputType)) {
        throw unsupportedExpressionException(
            arithmetic, format("%s is only supported for arguments of the same type", operation));
      }
      if (!isPrimitiveNumeric(outputType)) {
        throw unsupportedExpressionException(
            arithmetic,
            format(
                "%s is only supported for numeric types: byte, short, int, long, float, double",
                operation));
      }

      return new ExpressionTransformResult(
          new ScalarExpression(
              arithmetic.getName(),
              children.stream().map(c -> c.expression).collect(Collectors.toList())),
          outputType);
    }

    private static boolean isPrimitiveNumeric(DataType dataType) {
      return dataType instanceof ByteType
          || dataType instanceof ShortType
          || dataType instanceof IntegerType
          || dataType instanceof LongType
          || dataType instanceof FloatType
          || dataType instanceof DoubleType;
    }

    @Override
    ExpressionTransformResult visitTimeAdd(ScalarExpression timeAdd) {
      List<ExpressionTransformResult> children =
          timeAdd.getChildren().stream().map(this::transformChild).collect(Collectors.toList());

      if (children.size() != 2) {
        throw unsupportedExpressionException(
            timeAdd, "TIMEADD requires exactly two arguments: timestamp column and milliseconds");
      }

      Expression timestampColumn = children.get(0).expression;
      Expression durationMilliseconds = children.get(1).expression;
      DataType timestampColumnType = children.get(0).outputType;
      DataType literalColumnType = children.get(1).outputType;

      // Ensure the first child is either a TimestampType or a TimestampNTZType,
      // and the second is a LongType.
      if (!((timestampColumnType instanceof TimestampType
              || timestampColumnType instanceof TimestampNTZType)
          && (literalColumnType instanceof LongType))) {
        throw new IllegalArgumentException(
            "TIMEADD requires a timestamp and a Long (milliseconds) to add to it");
      }

      return new ExpressionTransformResult(
          new ScalarExpression("TIMEADD", Arrays.asList(timestampColumn, durationMilliseconds)),
          timestampColumnType // Result is also a timestamp
          );
    }

    @Override
    ExpressionTransformResult visitSubstring(ScalarExpression substring) {
      List<ExpressionTransformResult> children =
          substring.getChildren().stream().map(this::transformChild).collect(toList());
      ScalarExpression transformedExpression =
          SubstringEvaluator.validateAndTransform(
              substring,
              children.stream().map(e -> e.expression).collect(toList()),
              children.stream().map(e -> e.outputType).collect(toList()));
      return new ExpressionTransformResult(transformedExpression, StringType.STRING);
    }

    @Override
    ExpressionTransformResult visitLike(final Predicate like) {
      List<ExpressionTransformResult> children =
          like.getChildren().stream().map(this::transformChild).collect(toList());
      Predicate transformedExpression =
          LikeExpressionEvaluator.validateAndTransform(
              like,
              children.stream().map(e -> e.expression).collect(toList()),
              children.stream().map(e -> e.outputType).collect(toList()));

      return new ExpressionTransformResult(transformedExpression, BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitStartsWith(Predicate startsWith) {
      List<ExpressionTransformResult> children =
          startsWith.getChildren().stream().map(this::transformChild).collect(toList());
      Predicate transformedExpression =
          StartsWithExpressionEvaluator.validateAndTransform(
              startsWith,
              children.stream().map(e -> e.expression).collect(toList()),
              children.stream().map(e -> e.outputType).collect(toList()));
      return new ExpressionTransformResult(transformedExpression, BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitIn(In in) {
      ExpressionTransformResult visitedValue = transformChild(in.getValueExpression());
      List<ExpressionTransformResult> visitedInList =
          in.getInListElements().stream().map(this::transformChild).collect(toList());
      In transformedExpression =
          InExpressionEvaluator.validateAndTransform(
              in,
              visitedValue.expression,
              visitedValue.outputType,
              visitedInList.stream().map(e -> e.expression).collect(toList()),
              visitedInList.stream().map(e -> e.outputType).collect(toList()));
      return new ExpressionTransformResult(transformedExpression, BooleanType.BOOLEAN);
    }

    @Override
    ExpressionTransformResult visitStGeometryBoxesIntersectOnStats(Predicate predicate) {
      List<ExpressionTransformResult> children =
          predicate.getChildren().stream().map(this::transformChild).collect(Collectors.toList());
      checkArgument(
          children.size() == 4,
          "ST_GEOMETRY_BOXES_INTERSECT_ON_STATS expects 4 children but got %d",
          children.size());
      // All 4 children must be GeometryType.
      // Children 0,1 are stats columns, children 2,3 are query literals.
      DataType child0Type = children.get(0).outputType;
      checkArgument(
          child0Type instanceof GeometryType,
          "ST_GEOMETRY_BOXES_INTERSECT_ON_STATS child 0 must be " + "geometry type, got %s",
          child0Type);
      for (int i = 1; i < 4; i++) {
        checkArgument(
            child0Type.equals(children.get(i).outputType),
            "ST_GEOMETRY_BOXES_INTERSECT_ON_STATS child %d type %s "
                + "doesn't match child 0 type %s",
            i,
            children.get(i).outputType,
            child0Type);
      }
      return new ExpressionTransformResult(
          new Predicate(
              "ST_GEOMETRY_BOXES_INTERSECT_ON_STATS",
              children.stream().map(c -> c.expression).collect(Collectors.toList())),
          BooleanType.BOOLEAN);
    }

    private Predicate validateIsPredicate(
        Expression baseExpression, ExpressionTransformResult result) {
      checkArgument(
          result.outputType instanceof BooleanType && result.expression instanceof Predicate,
          "%s: expected a predicate expression but got %s with output type %s.",
          baseExpression,
          result.expression,
          result.outputType);
      return (Predicate) result.expression;
    }

    private Expression transformBinaryComparator(Predicate predicate) {
      ExpressionTransformResult leftResult = transformChild(getLeft(predicate));
      ExpressionTransformResult rightResult = transformChild(getRight(predicate));
      Expression left = leftResult.expression;
      Expression right = rightResult.expression;

      if (predicate.getCollationIdentifier().isPresent()) {
        CollationIdentifier collationIdentifier = predicate.getCollationIdentifier().get();
        checkIsUTF8BinaryCollation(predicate, collationIdentifier);

        for (DataType dataType : Arrays.asList(leftResult.outputType, rightResult.outputType)) {
          checkIsStringType(
              dataType,
              predicate,
              format("Predicate %s expects STRING type inputs", predicate.getName()));
        }
        return new Predicate(predicate.getName(), left, right, collationIdentifier);
      }

      if (!leftResult.outputType.equivalent(rightResult.outputType)) {
        if (canCastTo(leftResult.outputType, rightResult.outputType)) {
          left = new ImplicitCastExpression(left, rightResult.outputType);
        } else if (canCastTo(rightResult.outputType, leftResult.outputType)) {
          right = new ImplicitCastExpression(right, leftResult.outputType);
        } else {
          String msg =
              format(
                  "operands are of different types which are not "
                      + "comparable: left type=%s, right type=%s",
                  leftResult.outputType, rightResult.outputType);
          throw unsupportedExpressionException(predicate, msg);
        }
      }
      return new Predicate(predicate.getName(), left, right);
    }

    private void requireExactType(
        Expression expression, DataType actual, DataType expected, String context) {
      if (!expected.equals(actual)) {
        throw unsupportedExpressionException(
            expression,
            String.format(
                "%s requires exact type %s, but expression produced %s",
                context, expected, actual));
      }
    }
  }

  /**
   * Implementation of {@link ExpressionVisitor} to evaluate expression on a {@link ColumnarBatch}.
   */
  private static class ExpressionEvalVisitor extends ExpressionVisitor<ColumnVector> {
    private final ColumnarBatch input;

    ExpressionEvalVisitor(ColumnarBatch input) {
      this.input = input;
    }

    ColumnVector eval(Expression expression, DataType expectedType) {
      if (expression instanceof ParseJson) {
        checkArgument(
            ((ParseJson) expression).getOutputSchema().equals(expectedType),
            "ParseJson output schema %s does not match expected output type %s",
            ((ParseJson) expression).getOutputSchema(),
            expectedType);
        return visitParseJson((ParseJson) expression);
      }
      if (expression instanceof ResolvedMapToStruct) {
        checkArgument(
            ((ResolvedMapToStruct) expression).getOutputType().equals(expectedType),
            "MapToStruct output schema %s does not match expected output type %s",
            ((ResolvedMapToStruct) expression).getOutputType(),
            expectedType);
        return visitResolvedMapToStruct((ResolvedMapToStruct) expression);
      }
      if (expression instanceof StructExpression) {
        checkArgument(
            expectedType instanceof StructType,
            "Struct expression expects a StructType output, but got %s",
            expectedType);
        return StructExpressionEvaluator.eval(
            (StructExpression) expression, (StructType) expectedType, input.getSize(), this::eval);
      }
      return visit(expression);
    }

    /*
    | Operand 1 | Operand 2 | `AND`      | `OR`       |
    |-----------|-----------|------------|------------|
    | True      | True      | True       | True       |
    | True      | False     | False      | True       |
    | True      | NULL      | NULL       | True       |
    | False     | True      | False      | True       |
    | False     | False     | False      | False      |
    | False     | NULL      | False      | NULL       |
    | NULL      | True      | NULL       | True       |
    | NULL      | False     | False      | NULL       |
    | NULL      | NULL      | NULL       | NULL       |
     */
    @Override
    ColumnVector visitAnd(And and) {
      PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(and);
      ColumnVector left = argResults.leftResult;
      ColumnVector right = argResults.rightResult;
      int numRows = argResults.rowCount;
      boolean[] result = new boolean[numRows];
      boolean[] nullability = new boolean[numRows];
      for (int rowId = 0; rowId < numRows; rowId++) {
        boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
        boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
        boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
        boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

        if (leftIsFalse || rightIsFalse) {
          nullability[rowId] = false;
          result[rowId] = false;
        } else if (leftIsTrue && rightIsTrue) {
          nullability[rowId] = false;
          result[rowId] = true;
        } else {
          nullability[rowId] = true;
          // result[rowId] is undefined when nullability[rowId] = true
        }
      }
      return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
    }

    @Override
    ColumnVector visitOr(Or or) {
      PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(or);
      ColumnVector left = argResults.leftResult;
      ColumnVector right = argResults.rightResult;
      int numRows = argResults.rowCount;
      boolean[] result = new boolean[numRows];
      boolean[] nullability = new boolean[numRows];
      for (int rowId = 0; rowId < numRows; rowId++) {
        boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
        boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
        boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
        boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

        if (leftIsTrue || rightIsTrue) {
          nullability[rowId] = false;
          result[rowId] = true;
        } else if (leftIsFalse && rightIsFalse) {
          nullability[rowId] = false;
          result[rowId] = false;
        } else {
          nullability[rowId] = true;
          // result[rowId] is undefined when nullability[rowId] = true
        }
      }
      return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
    }

    @Override
    ColumnVector visitAlwaysTrue(AlwaysTrue alwaysTrue) {
      return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), true);
    }

    @Override
    ColumnVector visitAlwaysFalse(AlwaysFalse alwaysFalse) {
      return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), false);
    }

    @Override
    ColumnVector visitComparator(Predicate predicate) {
      PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(predicate);
      switch (predicate.getName()) {
        case "=":
          return comparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult == 0));
        case ">":
          return comparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult > 0));
        case ">=":
          return comparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult >= 0));
        case "<":
          return comparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult < 0));
        case "<=":
          return comparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult <= 0));
        case "IS NOT DISTINCT FROM":
          return nullSafeComparatorVector(
              argResults.leftResult,
              argResults.rightResult,
              (compareResult) -> (compareResult == 0));
        default:
          // We should never reach this based on the ExpressionVisitor
          throw new IllegalStateException(
              String.format("%s is not a recognized comparator", predicate.getName()));
      }
    }

    @Override
    ColumnVector visitLiteral(Literal literal) {
      DataType dataType = literal.getDataType();
      if (dataType instanceof BooleanType
          || dataType instanceof ByteType
          || dataType instanceof ShortType
          || dataType instanceof IntegerType
          || dataType instanceof LongType
          || dataType instanceof FloatType
          || dataType instanceof DoubleType
          || dataType instanceof StringType
          || dataType instanceof BinaryType
          || dataType instanceof DecimalType
          || dataType instanceof DateType
          || dataType instanceof TimestampType
          || dataType instanceof TimestampNTZType
          || dataType instanceof GeometryType
          || dataType instanceof GeographyType
          || dataType instanceof ArrayType
          || dataType instanceof MapType
          || dataType instanceof StructType) {
        return new DefaultConstantVector(dataType, input.getSize(), literal.getValue());
      }

      throw new UnsupportedOperationException("unsupported expression encountered: " + literal);
    }

    @Override
    ColumnVector visitColumn(Column column) {
      String[] names = column.getNames();
      DataType currentType = input.getSchema();
      ColumnVector columnVector = null;
      for (int level = 0; level < names.length; level++) {
        assertColumnExists(currentType instanceof StructType, input.getSchema(), column);
        StructType structSchema = ((StructType) currentType);
        int ordinal = structSchema.indexOf(names[level]);
        assertColumnExists(ordinal != -1, input.getSchema(), column);
        currentType = structSchema.at(ordinal).getDataType();

        if (level == 0) {
          ColumnVector inputVector = input.getColumnVector(ordinal);
          columnVector = new DefaultViewVector(inputVector, 0, inputVector.getSize());
        } else {
          columnVector = columnVector.getChild(ordinal);
        }
      }
      assertColumnExists(columnVector != null, input.getSchema(), column);
      return columnVector;
    }

    @Override
    ColumnVector visitStruct(StructExpression struct) {
      throw new IllegalArgumentException(
          "A caller-supplied StructType is required to evaluate a struct expression");
    }

    @Override
    ColumnVector visitStructPatch(StructPatch structPatch) {
      throw new IllegalArgumentException(
          "Struct patches must be lowered before expression evaluation");
    }

    @Override
    ColumnVector visitParseJson(ParseJson parseJson) {
      ColumnVector jsonVector = visit(parseJson.getJsonExpression());
      checkArgument(
          jsonVector.getDataType() instanceof StringType,
          "ParseJson expects string input, but got %s",
          jsonVector.getDataType());
      checkArgument(
          jsonVector.getSize() == input.getSize(),
          "ParseJson input size mismatch: expected %s but got %s",
          input.getSize(),
          jsonVector.getSize());

      try {
        List<Object> rows = new ArrayList<>(jsonVector.getSize());
        for (int rowId = 0; rowId < jsonVector.getSize(); rowId++) {
          String json = jsonVector.isNullAt(rowId) ? "{}" : jsonVector.getString(rowId);
          rows.add(DefaultJsonRow.fromJsonPermissively(json, parseJson.getOutputSchema()));
        }
        return DefaultGenericVector.fromList(parseJson.getOutputSchema(), rows);
      } catch (IOException | RuntimeException ignored) {
        return new DefaultConstantVector(parseJson.getOutputSchema(), jsonVector.getSize(), null);
      } finally {
        jsonVector.close();
      }
    }

    @Override
    ColumnVector visitMapToStruct(MapToStruct mapToStruct) {
      throw new IllegalStateException("MapToStruct must be resolved before expression evaluation");
    }

    @Override
    ColumnVector visitResolvedMapToStruct(ResolvedMapToStruct mapToStruct) {
      ColumnVector maps = visit(mapToStruct.getMapExpression());
      try {
        return MapToStructEvaluator.eval(maps, mapToStruct.getOutputType());
      } finally {
        maps.close();
      }
    }

    @Override
    ColumnVector visitCast(ImplicitCastExpression cast) {
      ColumnVector inputResult = visit(cast.getInput());
      return cast.eval(inputResult);
    }

    @Override
    ColumnVector visitBooleanExpression(BooleanExpression booleanExpression) {
      return eval(booleanExpression.getExpression(), BooleanType.BOOLEAN);
    }

    @Override
    ColumnVector visitJunction(Junction junction) {
      List<ColumnVector> children = new ArrayList<>();
      try {
        for (Predicate predicate : junction.getPredicates()) {
          children.add(eval(predicate, BooleanType.BOOLEAN));
        }
        return PlanPredicateEvaluator.junction(junction.getOperator(), children, input.getSize());
      } catch (RuntimeException failure) {
        Utils.closeCloseablesAndAddSuppressed(failure, children.toArray(new ColumnVector[0]));
        throw failure;
      }
    }

    @Override
    ColumnVector visitBinaryPredicate(BinaryPredicate predicate) {
      ColumnVector left = visit(predicate.getLeft());
      ColumnVector right;
      try {
        right = visit(predicate.getRight());
      } catch (RuntimeException failure) {
        Utils.closeCloseablesAndAddSuppressed(failure, left);
        throw failure;
      }
      try {
        return PlanPredicateEvaluator.strictComparison(predicate.getOperator(), left, right);
      } catch (RuntimeException failure) {
        Utils.closeCloseablesAndAddSuppressed(failure, left, right);
        throw failure;
      }
    }

    @Override
    ColumnVector visitPartitionValue(PartitionValueExpression partitionValue) {
      ColumnVector input = visit(partitionValue.getInput());
      return PartitionValueEvaluator.eval(input, partitionValue.getDataType());
    }

    @Override
    ColumnVector visitElementAt(ScalarExpression elementAt) {
      ColumnVector map = visit(childAt(elementAt, 0));
      ColumnVector lookupKey = visit(childAt(elementAt, 1));
      return ElementAtEvaluator.eval(map, lookupKey);
    }

    @Override
    ColumnVector visitNot(Predicate predicate) {
      ColumnVector childResult = visit(childAt(predicate, 0));
      return booleanWrapperVector(
          childResult,
          rowId -> !childResult.getBoolean(rowId),
          rowId -> childResult.isNullAt(rowId));
    }

    @Override
    ColumnVector visitIsNotNull(Predicate predicate) {
      ColumnVector childResult = visit(childAt(predicate, 0));
      return booleanWrapperVector(
          childResult, rowId -> !childResult.isNullAt(rowId), rowId -> false);
    }

    @Override
    ColumnVector visitIsNull(Predicate predicate) {
      ColumnVector childResult = visit(getUnaryChild(predicate));
      return booleanWrapperVector(
          childResult, rowId -> childResult.isNullAt(rowId), rowId -> false);
    }

    @Override
    ColumnVector visitCoalesce(ScalarExpression coalesce) {
      checkArgument(
          coalesce instanceof DeferredCoalesceExpression,
          "Coalesce must be transformed before evaluation");
      DataType outputType = ((DeferredCoalesceExpression) coalesce).getOutputType();
      List<ColumnVector> childResults = new ArrayList<>();
      boolean[] unresolvedRows = new boolean[input.getSize()];
      Arrays.fill(unresolvedRows, true);
      try {
        for (Expression child : coalesce.getChildren()) {
          ExpressionTransformResult transformed =
              new ExpressionTransformer(input.getSchema()).transform(child, outputType);
          if (!transformed.outputType.equals(outputType)) {
            throw unsupportedExpressionException(
                coalesce, "Coalesce is only supported for arguments of the same type");
          }
          ColumnVector result = eval(transformed.expression, outputType);
          childResults.add(result);
          checkArgument(
              result.getDataType().equals(outputType),
              "Coalesce input %s has type %s, expected %s",
              childResults.size() - 1,
              result.getDataType(),
              outputType);
          checkArgument(
              result.getSize() == input.getSize(),
              "Coalesce input %s has size %s, expected %s",
              childResults.size() - 1,
              result.getSize(),
              input.getSize());

          boolean hasUnresolvedRows = false;
          for (int rowId = 0; rowId < unresolvedRows.length; rowId++) {
            if (unresolvedRows[rowId] && !result.isNullAt(rowId)) {
              unresolvedRows[rowId] = false;
            }
            hasUnresolvedRows |= unresolvedRows[rowId];
          }
          if (!hasUnresolvedRows) {
            break;
          }
        }
        return DefaultExpressionUtils.combinationVector(
            childResults,
            rowId -> {
              for (int idx = 0; idx < childResults.size(); idx++) {
                if (!childResults.get(idx).isNullAt(rowId)) {
                  return idx;
                }
              }
              return 0;
            });
      } catch (RuntimeException failure) {
        Utils.closeCloseablesAndAddSuppressed(failure, childResults.toArray(new ColumnVector[0]));
        throw failure;
      }
    }

    @Override
    ColumnVector visitArithmetic(ScalarExpression arithmetic) {
      ColumnVector left = visit(childAt(arithmetic, 0));
      ColumnVector right;
      try {
        right = visit(childAt(arithmetic, 1));
      } catch (RuntimeException e) {
        Utils.closeCloseablesAndAddSuppressed(e, left);
        throw e;
      }
      try {
        return arithmeticVector(left, right, arithmetic.getName());
      } catch (RuntimeException e) {
        Utils.closeCloseablesAndAddSuppressed(e, left, right);
        throw e;
      }
    }

    @Override
    ColumnVector visitTimeAdd(ScalarExpression timeAdd) {
      ColumnVector timestampColumn = visit(timeAdd.getChildren().get(0));
      ColumnVector durationVector = visit(timeAdd.getChildren().get(1));

      return new ColumnVector() {
        @Override
        public DataType getDataType() {
          return timestampColumn.getDataType();
        }

        @Override
        public int getSize() {
          return timestampColumn.getSize();
        }

        @Override
        public void close() {
          timestampColumn.close();
          durationVector.close();
        }

        @Override
        public boolean isNullAt(int rowId) {
          return timestampColumn.isNullAt(rowId) || durationVector.isNullAt(rowId);
        }

        @Override
        public long getLong(int rowId) {
          if (isNullAt(rowId)) {
            return 0;
          }
          long durationMicros = durationVector.getLong(rowId) * 1000L;
          return timestampColumn.getLong(rowId) + durationMicros;
        }
      };
    }

    @Override
    ColumnVector visitSubstring(ScalarExpression subString) {
      return SubstringEvaluator.eval(
          subString.getChildren().stream().map(this::visit).collect(toList()));
    }

    @Override
    ColumnVector visitLike(final Predicate like) {
      List<Expression> children = like.getChildren();
      return LikeExpressionEvaluator.eval(
          children, children.stream().map(this::visit).collect(toList()));
    }

    @Override
    ColumnVector visitStartsWith(Predicate startsWith) {
      return StartsWithExpressionEvaluator.eval(
          startsWith.getChildren().stream().map(this::visit).collect(toList()));
    }

    @Override
    ColumnVector visitIn(In in) {
      return InExpressionEvaluator.eval(
          in.getChildren().stream().map(this::visit).collect(toList()));
    }

    @Override
    ColumnVector visitStGeometryBoxesIntersectOnStats(Predicate predicate) {
      List<Expression> children = predicate.getChildren();
      ColumnVector leftMin = visit(children.get(0));
      ColumnVector leftMax = visit(children.get(1));
      ColumnVector rightMin = visit(children.get(2));
      ColumnVector rightMax = visit(children.get(3));
      int numRows = input.getSize();
      boolean[] result = new boolean[numRows];
      boolean[] nullability = new boolean[numRows];
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (leftMin.isNullAt(rowId)
            || leftMax.isNullAt(rowId)
            || rightMin.isNullAt(rowId)
            || rightMax.isNullAt(rowId)) {
          nullability[rowId] = true;
          continue;
        }
        double[] lMin = GeometryUtils.parsePointXY(leftMin.getString(rowId));
        double[] lMax = GeometryUtils.parsePointXY(leftMax.getString(rowId));
        double[] rMin = GeometryUtils.parsePointXY(rightMin.getString(rowId));
        double[] rMax = GeometryUtils.parsePointXY(rightMax.getString(rowId));
        result[rowId] = boxesIntersect(lMin, lMax, rMin, rMax);
      }
      return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
    }

    private static boolean boxesIntersect(
        double[] lMin, double[] lMax, double[] rMin, double[] rMax) {
      return lMax[0] >= rMin[0] && rMax[0] >= lMin[0] && lMax[1] >= rMin[1] && rMax[1] >= lMin[1];
    }

    /**
     * Utility method to evaluate inputs to the binary input expression. Also validates the
     * evaluated expression result {@link ColumnVector}s are of the same size.
     *
     * @param predicate
     * @return Triplet of (result vector size, left operand result, left operand result)
     */
    private PredicateChildrenEvalResult evalBinaryExpressionChildren(Predicate predicate) {
      ColumnVector left = visit(getLeft(predicate));
      ColumnVector right = visit(getRight(predicate));
      checkArgument(
          left.getSize() == right.getSize(),
          "Left and right operand returned different results: left=%d, right=d",
          left.getSize(),
          right.getSize());
      return new PredicateChildrenEvalResult(left.getSize(), left, right);
    }
  }

  /** Encapsulates children expression result of binary input predicate */
  private static class PredicateChildrenEvalResult {
    public final int rowCount;
    public final ColumnVector leftResult;
    public final ColumnVector rightResult;

    PredicateChildrenEvalResult(int rowCount, ColumnVector leftResult, ColumnVector rightResult) {
      this.rowCount = rowCount;
      this.leftResult = leftResult;
      this.rightResult = rightResult;
    }
  }

  private static void assertColumnExists(boolean condition, StructType schema, Column column) {
    if (!condition) {
      throw new IllegalArgumentException(
          format("%s doesn't exist in input data schema: %s", column, schema));
    }
  }
}
