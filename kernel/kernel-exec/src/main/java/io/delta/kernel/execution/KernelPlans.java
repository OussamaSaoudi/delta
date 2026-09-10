/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.execution;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.InvalidProtocolBufferException;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.expressions.AlwaysFalse;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.MapToStruct;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.ParseJson;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.expressions.StructPatch;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.plans.Agg;
import io.delta.kernel.plans.Aggregate;
import io.delta.kernel.plans.DynamicScan;
import io.delta.kernel.plans.FileType;
import io.delta.kernel.plans.Filter;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.plans.Project;
import io.delta.kernel.plans.ScanFile;
import io.delta.kernel.plans.ScanJson;
import io.delta.kernel.plans.ScanParquet;
import io.delta.kernel.plans.SemiJoin;
import io.delta.kernel.plans.UnionAll;
import io.delta.kernel.plans.Values;
import io.delta.kernel.proto.expressions.JunctionPredicateOp;
import io.delta.kernel.proto.expressions.UnaryExpressionOp;
import io.delta.kernel.proto.expressions.UnaryPredicateOp;
import io.delta.kernel.proto.expressions.VariadicExpressionOp;
import io.delta.kernel.proto.plan.Plan;
import io.delta.kernel.proto.schema.EdgeInterpolationAlgorithm;
import io.delta.kernel.proto.schema.SimplePrimitiveType;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.GeographyType;
import io.delta.kernel.types.GeometryType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import io.delta.kernel.utils.FileStatus;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Decodes the kernel-rs plan wire format into passive Kernel Java plan objects. */
public final class KernelPlans {
  private KernelPlans() {}

  public static PlanNode fromProto(byte[] planBytes) {
    requireNonNull(planBytes, "planBytes is null");
    try {
      return new Decoder(Plan.parseFrom(planBytes)).decode();
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalArgumentException("Invalid kernel plan protobuf", failure);
    }
  }

  private static final class Decoder {
    private final Plan wire;
    private final List<PlanNode> nodes = new ArrayList<>();

    private Decoder(Plan wire) {
      this.wire = wire;
    }

    private PlanNode decode() {
      if (wire.getNodesCount() == 0) {
        throw invalid("Plan has no nodes");
      }
      for (int index = 0; index < wire.getNodesCount(); index++) {
        nodes.add(node(wire.getNodes(index), index));
      }
      return nodes.get(nodes.size() - 1);
    }

    private PlanNode node(io.delta.kernel.proto.plan.PlanNode node, int index) {
      if (!node.hasOp()) {
        throw invalid("Plan node " + index + " has no operator");
      }
      io.delta.kernel.proto.plan.Operator op = node.getOp();
      switch (op.getOpCase()) {
        case SCAN_PARQUET:
          requireInputs(node, index, 0);
          return scanParquet(op.getScanParquet());
        case SCAN_JSON:
          requireInputs(node, index, 0);
          return scanJson(op.getScanJson());
        case VALUES:
          requireInputs(node, index, 0);
          return values(op.getValues());
        case PROJECT:
          return project(input(node, index, 0, 1), op.getProject());
        case FILTER:
          return filter(input(node, index, 0, 1), op.getFilter());
        case UNION_ALL:
          return union(inputs(node, index, 1));
        case DYNAMIC_SCAN:
          return dynamicScan(input(node, index, 0, 1), op.getDynamicScan());
        case AGGREGATE:
          return aggregate(input(node, index, 0, 1), op.getAggregate());
        case SEMI_JOIN:
          requireInputs(node, index, 2);
          return semiJoin(input(node, index, 0), input(node, index, 1), op.getSemiJoin());
        case OP_NOT_SET:
        default:
          throw invalid("Plan node " + index + " has no supported operator");
      }
    }

    private PlanNode input(
        io.delta.kernel.proto.plan.PlanNode node, int index, int ordinal, int expected) {
      requireInputs(node, index, expected);
      return input(node, index, ordinal);
    }

    private PlanNode input(io.delta.kernel.proto.plan.PlanNode node, int index, int ordinal) {
      long input = Integer.toUnsignedLong(node.getInputs(ordinal));
      if (input >= index) {
        throw invalid("Plan node " + index + " has invalid input index " + input);
      }
      return nodes.get((int) input);
    }

    private List<PlanNode> inputs(
        io.delta.kernel.proto.plan.PlanNode node, int index, int minimum) {
      if (node.getInputsCount() < minimum) {
        throw invalid("Plan node " + index + " requires at least " + minimum + " inputs");
      }
      List<PlanNode> result = new ArrayList<>(node.getInputsCount());
      for (int ordinal = 0; ordinal < node.getInputsCount(); ordinal++) {
        result.add(input(node, index, ordinal));
      }
      return result;
    }

    private void requireInputs(io.delta.kernel.proto.plan.PlanNode node, int index, int expected) {
      if (node.getInputsCount() != expected) {
        throw invalid(
            "Plan node "
                + index
                + " requires "
                + expected
                + " inputs, found "
                + node.getInputsCount());
      }
    }
  }

  private static ScanParquet scanParquet(io.delta.kernel.proto.plan.ScanParquetNode scan) {
    StructType schema = structType(required(scan.hasSchema(), scan.getSchema(), "scan schema"));
    StructType constants = constantsSchema(schema, scan.getFileConstantColumnsList());
    return new ScanParquet(
        scanFiles(scan.getFilesList(), constants),
        Optional.empty(),
        scan.getFileConstantColumnsList(),
        schema,
        Optional.empty());
  }

  private static ScanJson scanJson(io.delta.kernel.proto.plan.ScanJsonNode scan) {
    StructType schema = structType(required(scan.hasSchema(), scan.getSchema(), "scan schema"));
    StructType constants = constantsSchema(schema, scan.getFileConstantColumnsList());
    return new ScanJson(
        scanFiles(scan.getFilesList(), constants),
        Optional.empty(),
        scan.getFileConstantColumnsList(),
        schema);
  }

  private static List<ScanFile> scanFiles(
      List<io.delta.kernel.proto.plan.ScanFile> files, StructType constantsSchema) {
    List<ScanFile> result = new ArrayList<>(files.size());
    for (io.delta.kernel.proto.plan.ScanFile file : files) {
      if (!file.hasMeta()) {
        throw invalid("Scan file has no metadata");
      }
      io.delta.kernel.proto.plan.FileMeta meta = file.getMeta();
      if (meta.getSize() <= 0) {
        throw invalid("Scan file size is not a positive signed long: " + meta.getSize());
      }
      Object[] constants = scalarValues(file.getFileConstantsList(), constantsSchema);
      result.add(
          new ScanFile(
              FileStatus.of(meta.getLocation(), meta.getSize(), meta.getLastModified()),
              GenericRow.fromOwnedValues(constantsSchema, constants),
              Optional.empty()));
    }
    return result;
  }

  private static StructType constantsSchema(StructType schema, List<String> names) {
    List<StructField> fields = new ArrayList<>(names.size());
    for (String name : names) {
      int ordinal = schema.indexOf(name);
      if (ordinal < 0) {
        throw invalid("File constant column `" + name + "` is absent from scan schema");
      }
      fields.add(schema.at(ordinal));
    }
    return new StructType(fields);
  }

  private static Values values(io.delta.kernel.proto.plan.ValuesNode values) {
    StructType schema =
        structType(required(values.hasSchema(), values.getSchema(), "values schema"));
    List<GenericRow> rows = new ArrayList<>(values.getRowsCount());
    for (io.delta.kernel.proto.plan.ValuesRow row : values.getRowsList()) {
      rows.add(GenericRow.fromOwnedValues(schema, scalarValues(row.getValuesList(), schema)));
    }
    return new Values(schema, rows);
  }

  private static Object[] scalarValues(
      List<io.delta.kernel.proto.expressions.Scalar> scalars, StructType schema) {
    if (scalars.size() != schema.length()) {
      throw invalid("Expected " + schema.length() + " scalar values, found " + scalars.size());
    }
    Object[] result = new Object[scalars.size()];
    for (int ordinal = 0; ordinal < scalars.size(); ordinal++) {
      Literal literal = literal(scalars.get(ordinal));
      DataType expected = schema.at(ordinal).getDataType();
      if (!expected.equals(literal.getDataType())) {
        throw invalid(
            "Scalar " + ordinal + " has type " + literal.getDataType() + ", expected " + expected);
      }
      result[ordinal] = literal.getValue();
    }
    return result;
  }

  private static Project project(PlanNode input, io.delta.kernel.proto.plan.ProjectNode project) {
    return new Project(
        input,
        expression(required(project.hasExpr(), project.getExpr(), "project expression")),
        structType(required(project.hasSchema(), project.getSchema(), "project schema")));
  }

  private static Filter filter(PlanNode input, io.delta.kernel.proto.plan.FilterNode filter) {
    return new Filter(
        input,
        predicate(required(filter.hasPredicate(), filter.getPredicate(), "filter predicate")));
  }

  private static UnionAll union(List<PlanNode> inputs) {
    return new UnionAll(inputs);
  }

  private static DynamicScan dynamicScan(
      PlanNode input, io.delta.kernel.proto.plan.DynamicScanNode scan) {
    URI tableRoot;
    try {
      tableRoot = URI.create(scan.getBaseUrl());
    } catch (IllegalArgumentException failure) {
      throw invalid("Dynamic scan has invalid base URL: " + scan.getBaseUrl(), failure);
    }
    if (tableRoot.isOpaque() || !tableRoot.getPath().endsWith("/")) {
      throw invalid("Dynamic scan base URL must be hierarchical and end in `/`: " + tableRoot);
    }
    StructType dataSchema =
        structType(required(scan.hasSchema(), scan.getSchema(), "dynamic scan schema"));
    Column path = column(required(scan.hasPathColumn(), scan.getPathColumn(), "path column"));
    Column size =
        column(required(scan.hasFileSizeColumn(), scan.getFileSizeColumn(), "file size column"));
    Column modified =
        column(
            required(
                scan.hasLastModifiedColumn(),
                scan.getLastModifiedColumn(),
                "last-modified column"));
    Column dv = column(required(scan.hasDvColumn(), scan.getDvColumn(), "deletion-vector column"));
    requireNonNullableType(input.outputSchema(), path, StringType.STRING, "path");
    requireNonNullableType(input.outputSchema(), size, LongType.LONG, "file size");
    requireNonNullableType(input.outputSchema(), modified, LongType.LONG, "last-modified");
    StructField dvField = resolveField(input.outputSchema(), dv);
    if (!io.delta.kernel.internal.actions.DeletionVectorDescriptor.READ_SCHEMA.equals(
            dvField.getDataType())
        || !dvField.isNullable()) {
      throw invalid("Dynamic scan deletion-vector column must have nullable DV schema");
    }
    validateDynamicConstants(input.outputSchema(), dataSchema, scan.getFileConstantColumnsList());
    return new DynamicScan(
        input,
        dataSchema,
        fileType(scan.getFileType()),
        tableRoot,
        scan.getFileConstantColumnsList(),
        path,
        size,
        modified,
        dv);
  }

  private static void validateDynamicConstants(
      StructType input, StructType output, List<String> names) {
    for (String name : names) {
      int inputOrdinal = input.indexOf(name);
      int outputOrdinal = output.indexOf(name);
      if (inputOrdinal < 0
          || outputOrdinal < 0
          || !input.at(inputOrdinal).equals(output.at(outputOrdinal))) {
        throw invalid("Dynamic scan constant `" + name + "` differs between input and output");
      }
    }
  }

  private static FileType fileType(io.delta.kernel.proto.plan.FileType fileType) {
    switch (fileType) {
      case FILE_TYPE_PARQUET:
        return FileType.PARQUET;
      case FILE_TYPE_JSON:
        return FileType.JSON;
      case FILE_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw invalid("Unsupported file type " + fileType);
    }
  }

  private static Aggregate aggregate(
      PlanNode input, io.delta.kernel.proto.plan.AggregateNode aggregate) {
    StructType output =
        structType(required(aggregate.hasSchema(), aggregate.getSchema(), "aggregate schema"));
    List<Expression> groups = new ArrayList<>(aggregate.getGroupByCount());
    for (int index = 0; index < aggregate.getGroupByCount(); index++) {
      Column group = column(aggregate.getGroupBy(index));
      StructField field = resolveField(input.outputSchema(), group);
      if (index >= output.length() || !field.equals(output.at(index))) {
        throw invalid("Aggregate group " + index + " has an inconsistent output field");
      }
      groups.add(group);
    }
    List<Agg> aggs = new ArrayList<>(aggregate.getAggsCount());
    for (io.delta.kernel.proto.plan.Agg agg : aggregate.getAggsList()) {
      aggs.add(agg(agg, input.outputSchema()));
    }
    return new Aggregate(input, groups, aggs, output);
  }

  private static Agg agg(io.delta.kernel.proto.plan.Agg agg, StructType input) {
    switch (agg.getFuncCase()) {
      case MIN:
        return minMax(
            input, required(agg.getMin().hasValue(), agg.getMin().getValue(), "min"), false);
      case MAX:
        return minMax(
            input, required(agg.getMax().hasValue(), agg.getMax().getValue(), "max"), true);
      case SUM:
        Column sum = column(required(agg.getSum().hasValue(), agg.getSum().getValue(), "sum"));
        requireType(input, sum, LongType.LONG, "sum");
        return Agg.sum(sum);
      case COUNT:
        Column count =
            column(required(agg.getCount().hasValue(), agg.getCount().getValue(), "count"));
        return Agg.count(count, resolveField(input, count).getDataType());
      case COUNT_STAR:
        return Agg.countStar();
      case MIN_NON_NULL_BY:
        return nonNullBy(input, agg.getMinNonNullBy(), false);
      case MAX_NON_NULL_BY:
        return nonNullBy(input, agg.getMaxNonNullBy(), true);
      case FUNC_NOT_SET:
      default:
        throw invalid("Aggregate function is absent or unsupported");
    }
  }

  private static Agg minMax(
      StructType input, io.delta.kernel.proto.expressions.ColumnName wireValue, boolean maximum) {
    Column value = column(wireValue);
    DataType type = resolveField(input, value).getDataType();
    return maximum ? Agg.max(value, type) : Agg.min(value, type);
  }

  private static Agg nonNullBy(
      StructType input, io.delta.kernel.proto.plan.MinNonNullByAgg agg, boolean maximum) {
    // In the two-operand wire format, field 2 was the key. It now decodes as the sentinel;
    // reusing it as the absent field-3 key preserves the exact old value/key semantics.
    io.delta.kernel.proto.expressions.ColumnName sentinel =
        required(agg.hasNullSentinel(), agg.getNullSentinel(), "non-null-by sentinel");
    return nonNullBy(
        input,
        required(agg.hasValue(), agg.getValue(), "non-null-by value"),
        sentinel,
        agg.hasKey() ? agg.getKey() : sentinel,
        maximum);
  }

  private static Agg nonNullBy(
      StructType input, io.delta.kernel.proto.plan.MaxNonNullByAgg agg, boolean maximum) {
    io.delta.kernel.proto.expressions.ColumnName sentinel =
        required(agg.hasNullSentinel(), agg.getNullSentinel(), "non-null-by sentinel");
    return nonNullBy(
        input,
        required(agg.hasValue(), agg.getValue(), "non-null-by value"),
        sentinel,
        agg.hasKey() ? agg.getKey() : sentinel,
        maximum);
  }

  private static Agg nonNullBy(
      StructType input,
      io.delta.kernel.proto.expressions.ColumnName wireValue,
      io.delta.kernel.proto.expressions.ColumnName wireSentinel,
      io.delta.kernel.proto.expressions.ColumnName wireKey,
      boolean maximum) {
    Column value = column(wireValue);
    Column sentinel = column(wireSentinel);
    Column key = column(wireKey);
    DataType valueType = resolveField(input, value).getDataType();
    DataType sentinelType = resolveField(input, sentinel).getDataType();
    DataType keyType = resolveField(input, key).getDataType();
    return maximum
        ? Agg.maxNonNullBy(value, valueType, sentinel, sentinelType, key, keyType)
        : Agg.minNonNullBy(value, valueType, sentinel, sentinelType, key, keyType);
  }

  private static SemiJoin semiJoin(
      PlanNode probe, PlanNode build, io.delta.kernel.proto.plan.SemiJoinNode join) {
    if (join.getProbeKeysCount() != join.getBuildKeysCount()) {
      throw invalid("SemiJoin probe/build key arity differs");
    }
    List<Expression> probeKeys = new ArrayList<>(join.getProbeKeysCount());
    List<Expression> buildKeys = new ArrayList<>(join.getBuildKeysCount());
    List<DataType> keyTypes = new ArrayList<>(join.getProbeKeysCount());
    for (int index = 0; index < join.getProbeKeysCount(); index++) {
      Column probeKey = column(join.getProbeKeys(index));
      Column buildKey = column(join.getBuildKeys(index));
      DataType probeType = resolveField(probe.outputSchema(), probeKey).getDataType();
      DataType buildType = resolveField(build.outputSchema(), buildKey).getDataType();
      if (!probeType.equals(buildType)) {
        throw invalid("SemiJoin key " + index + " has mismatched input types");
      }
      probeKeys.add(probeKey);
      buildKeys.add(buildKey);
      keyTypes.add(probeType);
    }
    return new SemiJoin(probe, build, probeKeys, buildKeys, keyTypes, join.getInverted());
  }

  private static Expression expression(io.delta.kernel.proto.expressions.Expression expression) {
    switch (expression.getKindCase()) {
      case LITERAL:
        return literal(expression.getLiteral());
      case COLUMN:
        return column(expression.getColumn());
      case PREDICATE:
        return predicate(expression.getPredicate());
      case STRUCT_EXPR:
        return structExpression(expression.getStructExpr());
      case TRANSFORM:
        return transform(expression.getTransform());
      case UNARY:
        return unary(expression.getUnary());
      case BINARY:
        return binary(expression.getBinary());
      case VARIADIC:
        return variadic(expression.getVariadic());
      case PARSE_JSON:
        io.delta.kernel.proto.expressions.ParseJsonExpression parse = expression.getParseJson();
        return new ParseJson(
            expression(required(parse.hasJsonExpr(), parse.getJsonExpr(), "parse-json input")),
            structType(
                required(parse.hasOutputSchema(), parse.getOutputSchema(), "parse-json schema")));
      case MAP_TO_STRUCT:
        io.delta.kernel.proto.expressions.MapToStructExpression map = expression.getMapToStruct();
        return new MapToStruct(
            expression(required(map.hasMapExpr(), map.getMapExpr(), "map-to-struct input")));
      case IF_EXPR:
        throw invalid("If expressions are not supported by Kernel Java");
      case OPAQUE:
        throw invalid(
            "Opaque expression `" + expression.getOpaque().getName() + "` is not decodable");
      case UNKNOWN:
        throw invalid("Unknown expression `" + expression.getUnknown() + "`");
      case KIND_NOT_SET:
      default:
        throw invalid("Expression kind is absent or unsupported");
    }
  }

  private static Expression structExpression(
      io.delta.kernel.proto.expressions.StructExpression struct) {
    List<Expression> fields = expressions(struct.getExprsList());
    return struct.hasNullabilityPredicate()
        ? new StructExpression(fields, expression(struct.getNullabilityPredicate()))
        : new StructExpression(fields);
  }

  private static Expression transform(io.delta.kernel.proto.expressions.Transform transform) {
    Optional<Column> input =
        transform.hasInputPath() ? Optional.of(column(transform.getInputPath())) : Optional.empty();
    Map<String, StructPatch.FieldTransform> fields = new LinkedHashMap<>();
    transform
        .getFieldTransformsMap()
        .forEach(
            (name, field) ->
                fields.put(
                    name,
                    new StructPatch.FieldTransform(
                        expressions(field.getExprsList()),
                        field.getIsReplace(),
                        field.getOptional())));
    return new StructPatch(
        input,
        fields,
        expressions(transform.getPrependedFieldsList()),
        expressions(transform.getAppendedFieldsList()));
  }

  private static Expression unary(io.delta.kernel.proto.expressions.UnaryExpression unary) {
    Expression child = expression(required(unary.hasExpr(), unary.getExpr(), "unary input"));
    if (unary.getOp() == UnaryExpressionOp.UNARY_EXPRESSION_OP_TO_JSON) {
      return new ScalarExpression("TO_JSON", Collections.singletonList(child));
    }
    throw invalid("Unsupported unary expression operator " + unary.getOp());
  }

  private static Expression binary(io.delta.kernel.proto.expressions.BinaryExpression binary) {
    Expression left = expression(required(binary.hasLeft(), binary.getLeft(), "binary left"));
    Expression right = expression(required(binary.hasRight(), binary.getRight(), "binary right"));
    String name;
    switch (binary.getOp()) {
      case BINARY_EXPRESSION_OP_PLUS:
        name = "ADD";
        break;
      case BINARY_EXPRESSION_OP_MINUS:
        name = "SUBTRACT";
        break;
      case BINARY_EXPRESSION_OP_MULTIPLY:
        name = "MULTIPLY";
        break;
      case BINARY_EXPRESSION_OP_DIVIDE:
        name = "DIVIDE";
        break;
      default:
        throw invalid("Unsupported binary expression operator " + binary.getOp());
    }
    return new ScalarExpression(name, Arrays.asList(left, right));
  }

  private static Expression variadic(
      io.delta.kernel.proto.expressions.VariadicExpression variadic) {
    String name;
    if (variadic.getOp() == VariadicExpressionOp.VARIADIC_EXPRESSION_OP_COALESCE) {
      name = "COALESCE";
    } else if (variadic.getOp() == VariadicExpressionOp.VARIADIC_EXPRESSION_OP_ARRAY) {
      name = "ARRAY";
    } else {
      throw invalid("Unsupported variadic expression operator " + variadic.getOp());
    }
    return new ScalarExpression(name, expressions(variadic.getExprsList()));
  }

  private static List<Expression> expressions(
      List<io.delta.kernel.proto.expressions.Expression> expressions) {
    List<Expression> result = new ArrayList<>(expressions.size());
    for (io.delta.kernel.proto.expressions.Expression expression : expressions) {
      result.add(expression(expression));
    }
    return result;
  }

  private static Predicate predicate(io.delta.kernel.proto.expressions.Predicate predicate) {
    switch (predicate.getKindCase()) {
      case BOOLEAN_EXPRESSION:
        Expression booleanExpression = expression(predicate.getBooleanExpression());
        return booleanExpression instanceof Predicate
            ? (Predicate) booleanExpression
            : new Predicate("=", booleanExpression, Literal.ofBoolean(true));
      case NOT:
        return new Predicate("NOT", predicate(predicate.getNot()));
      case UNARY:
        return unaryPredicate(predicate.getUnary());
      case BINARY:
        return binaryPredicate(predicate.getBinary());
      case JUNCTION:
        return junction(predicate.getJunction());
      case OPAQUE:
        throw invalid(
            "Opaque predicate `" + predicate.getOpaque().getName() + "` is not decodable");
      case UNKNOWN:
        throw invalid("Unknown predicate `" + predicate.getUnknown() + "`");
      case KIND_NOT_SET:
      default:
        throw invalid("Predicate kind is absent or unsupported");
    }
  }

  private static Predicate unaryPredicate(
      io.delta.kernel.proto.expressions.UnaryPredicate predicate) {
    if (predicate.getOp() != UnaryPredicateOp.UNARY_PREDICATE_OP_IS_NULL) {
      throw invalid("Unsupported unary predicate operator " + predicate.getOp());
    }
    return new Predicate(
        "IS_NULL",
        expression(required(predicate.hasExpr(), predicate.getExpr(), "unary predicate input")));
  }

  private static Predicate binaryPredicate(
      io.delta.kernel.proto.expressions.BinaryPredicate predicate) {
    Expression left =
        expression(required(predicate.hasLeft(), predicate.getLeft(), "predicate left"));
    Expression right =
        expression(required(predicate.hasRight(), predicate.getRight(), "predicate right"));
    switch (predicate.getOp()) {
      case BINARY_PREDICATE_OP_LESS_THAN:
        return new Predicate("<", left, right);
      case BINARY_PREDICATE_OP_GREATER_THAN:
        return new Predicate(">", left, right);
      case BINARY_PREDICATE_OP_EQUAL:
        return new Predicate("=", left, right);
      case BINARY_PREDICATE_OP_DISTINCT:
        return new Predicate("NOT", new Predicate("IS NOT DISTINCT FROM", left, right));
      case BINARY_PREDICATE_OP_IN:
        throw invalid("Rust binary IN has no equivalent Kernel Java expression");
      default:
        throw invalid("Unsupported binary predicate operator " + predicate.getOp());
    }
  }

  private static Predicate junction(io.delta.kernel.proto.expressions.JunctionPredicate junction) {
    if (junction.getPredsCount() == 0) {
      if (junction.getOp() == JunctionPredicateOp.JUNCTION_PREDICATE_OP_AND) {
        return AlwaysTrue.ALWAYS_TRUE;
      }
      if (junction.getOp() == JunctionPredicateOp.JUNCTION_PREDICATE_OP_OR) {
        return AlwaysFalse.ALWAYS_FALSE;
      }
      throw invalid("Unsupported junction predicate operator " + junction.getOp());
    }
    Predicate result = predicate(junction.getPreds(0));
    if (junction.getOp() == JunctionPredicateOp.JUNCTION_PREDICATE_OP_AND) {
      for (int index = 1; index < junction.getPredsCount(); index++) {
        result = new And(result, predicate(junction.getPreds(index)));
      }
    } else if (junction.getOp() == JunctionPredicateOp.JUNCTION_PREDICATE_OP_OR) {
      for (int index = 1; index < junction.getPredsCount(); index++) {
        result = new Or(result, predicate(junction.getPreds(index)));
      }
    } else {
      throw invalid("Unsupported junction predicate operator " + junction.getOp());
    }
    return result;
  }

  private static Literal literal(io.delta.kernel.proto.expressions.Scalar scalar) {
    switch (scalar.getValueCase()) {
      case INTEGER:
        return Literal.ofInt(scalar.getInteger());
      case LONG:
        return Literal.ofLong(scalar.getLong());
      case SHORT:
        return Literal.ofShort(narrowShort(scalar.getShort()));
      case BYTE:
        return Literal.ofByte(narrowByte(scalar.getByte()));
      case FLOAT:
        return Literal.ofFloat(scalar.getFloat());
      case DOUBLE:
        return Literal.ofDouble(scalar.getDouble());
      case STRING:
        return Literal.ofString(scalar.getString());
      case BOOLEAN:
        return Literal.ofBoolean(scalar.getBoolean());
      case TIMESTAMP:
        return Literal.ofTimestamp(scalar.getTimestamp());
      case TIMESTAMP_NTZ:
        return Literal.ofTimestampNtz(scalar.getTimestampNtz());
      case DATE:
        return Literal.ofDate(scalar.getDate());
      case BINARY:
        return Literal.ofBinary(scalar.getBinary().toByteArray());
      case DECIMAL:
        return decimal(scalar.getDecimal());
      case NULL:
        return Literal.ofNull(dataType(scalar.getNull()));
      case STRUCT:
        return structLiteral(scalar.getStruct());
      case ARRAY:
        return arrayLiteral(scalar.getArray());
      case MAP:
        return mapLiteral(scalar.getMap());
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        throw invalid("Interval literals are not supported by Kernel Java");
      case VALUE_NOT_SET:
      default:
        throw invalid("Scalar value is absent or unsupported");
    }
  }

  private static Literal decimal(io.delta.kernel.proto.expressions.DecimalData decimal) {
    if (!decimal.hasDecimalType()) {
      throw invalid("Decimal scalar has no type");
    }
    DecimalType type = decimalType(decimal.getDecimalType());
    byte[] bits = decimal.getBits().toByteArray();
    if (bits.length == 0) {
      throw invalid("Decimal scalar has empty unscaled bytes");
    }
    return Literal.ofDecimal(
        new BigDecimal(new BigInteger(bits), type.getScale()),
        type.getPrecision(),
        type.getScale());
  }

  private static Literal structLiteral(io.delta.kernel.proto.expressions.StructData struct) {
    StructType type = new StructType(structFields(struct.getFieldsList()));
    Object[] values = scalarValues(struct.getValuesList(), type);
    return Literal.ofStruct(GenericRow.fromOwnedValues(type, values), type);
  }

  private static Literal arrayLiteral(io.delta.kernel.proto.expressions.ArrayData array) {
    ArrayType type = arrayType(required(array.hasArrayType(), array.getArrayType(), "array type"));
    List<Object> values = scalarObjects(array.getElementsList(), type.getElementType());
    ArrayValue value = VectorUtils.buildArrayValue(values, type.getElementType());
    return Literal.ofArray(value, type);
  }

  private static Literal mapLiteral(io.delta.kernel.proto.expressions.MapData map) {
    MapType type = mapType(required(map.hasMapType(), map.getMapType(), "map type"));
    List<Object> keys = new ArrayList<>(map.getPairsCount());
    List<Object> values = new ArrayList<>(map.getPairsCount());
    for (io.delta.kernel.proto.expressions.MapEntry pair : map.getPairsList()) {
      keys.add(scalarObject(required(pair.hasKey(), pair.getKey(), "map key"), type.getKeyType()));
      values.add(
          scalarObject(
              required(pair.hasValue(), pair.getValue(), "map value"), type.getValueType()));
    }
    MapValue value = VectorUtils.buildMapValue(keys, values, type);
    return Literal.ofMap(value, type);
  }

  private static List<Object> scalarObjects(
      List<io.delta.kernel.proto.expressions.Scalar> scalars, DataType expected) {
    List<Object> result = new ArrayList<>(scalars.size());
    for (io.delta.kernel.proto.expressions.Scalar scalar : scalars) {
      result.add(scalarObject(scalar, expected));
    }
    return result;
  }

  private static Object scalarObject(
      io.delta.kernel.proto.expressions.Scalar scalar, DataType expected) {
    Literal literal = literal(scalar);
    if (!expected.equals(literal.getDataType())) {
      throw invalid("Scalar has type " + literal.getDataType() + ", expected " + expected);
    }
    return literal.getValue();
  }

  private static StructType structType(io.delta.kernel.proto.schema.StructType struct) {
    return new StructType(structFields(struct.getFieldsList()));
  }

  private static List<StructField> structFields(
      List<io.delta.kernel.proto.schema.StructField> fields) {
    List<StructField> result = new ArrayList<>(fields.size());
    for (io.delta.kernel.proto.schema.StructField field : fields) {
      if (!field.hasDataType()) {
        throw invalid("Struct field `" + field.getName() + "` has no data type");
      }
      StructField decoded =
          new StructField(
              field.getName(),
              dataType(field.getDataType()),
              field.getNullable(),
              metadata(field.getMetadataMap()));
      result.add(DataTypeJsonSerDe.applyFieldLevelMetadata(decoded));
    }
    return result;
  }

  private static DataType dataType(io.delta.kernel.proto.schema.DataType type) {
    switch (type.getKindCase()) {
      case PRIMITIVE:
        return primitive(type.getPrimitive());
      case ARRAY:
        return arrayType(type.getArray());
      case STRUCT:
        return structType(type.getStruct());
      case MAP:
        return mapType(type.getMap());
      case VARIANT:
        return VariantType.VARIANT;
      case KIND_NOT_SET:
      default:
        throw invalid("Data type kind is absent or unsupported");
    }
  }

  private static DataType primitive(io.delta.kernel.proto.schema.PrimitiveType primitive) {
    switch (primitive.getKindCase()) {
      case SIMPLE:
        return simple(primitive.getSimple());
      case DECIMAL:
        return decimalType(primitive.getDecimal());
      case GEOMETRY:
        return new GeometryType(primitive.getGeometry().getCrs());
      case GEOGRAPHY:
        return new GeographyType(
            primitive.getGeography().getCrs(), algorithm(primitive.getGeography().getAlgorithm()));
      case KIND_NOT_SET:
      default:
        throw invalid("Primitive type kind is absent or unsupported");
    }
  }

  private static DataType simple(SimplePrimitiveType type) {
    switch (type) {
      case SIMPLE_PRIMITIVE_TYPE_STRING:
        return StringType.STRING;
      case SIMPLE_PRIMITIVE_TYPE_LONG:
        return LongType.LONG;
      case SIMPLE_PRIMITIVE_TYPE_INTEGER:
        return IntegerType.INTEGER;
      case SIMPLE_PRIMITIVE_TYPE_SHORT:
        return ShortType.SHORT;
      case SIMPLE_PRIMITIVE_TYPE_BYTE:
        return ByteType.BYTE;
      case SIMPLE_PRIMITIVE_TYPE_FLOAT:
        return FloatType.FLOAT;
      case SIMPLE_PRIMITIVE_TYPE_DOUBLE:
        return DoubleType.DOUBLE;
      case SIMPLE_PRIMITIVE_TYPE_BOOLEAN:
        return BooleanType.BOOLEAN;
      case SIMPLE_PRIMITIVE_TYPE_BINARY:
        return BinaryType.BINARY;
      case SIMPLE_PRIMITIVE_TYPE_DATE:
        return DateType.DATE;
      case SIMPLE_PRIMITIVE_TYPE_TIMESTAMP:
        return TimestampType.TIMESTAMP;
      case SIMPLE_PRIMITIVE_TYPE_TIMESTAMP_NTZ:
        return TimestampNTZType.TIMESTAMP_NTZ;
      case SIMPLE_PRIMITIVE_TYPE_VOID:
      case SIMPLE_PRIMITIVE_TYPE_INTERVAL_YEAR_MONTH:
      case SIMPLE_PRIMITIVE_TYPE_INTERVAL_DAY_TIME:
        throw invalid("Data type " + type + " is not supported by Kernel Java");
      case SIMPLE_PRIMITIVE_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw invalid("Simple primitive type is absent or unsupported: " + type);
    }
  }

  private static DecimalType decimalType(io.delta.kernel.proto.schema.DecimalType decimal) {
    long precision = Integer.toUnsignedLong(decimal.getPrecision());
    long scale = Integer.toUnsignedLong(decimal.getScale());
    if (precision < 1 || precision > 38 || scale > precision) {
      throw invalid("Invalid decimal precision/scale: " + precision + "/" + scale);
    }
    return new DecimalType((int) precision, (int) scale);
  }

  private static ArrayType arrayType(io.delta.kernel.proto.schema.ArrayType array) {
    return new ArrayType(
        dataType(required(array.hasElementType(), array.getElementType(), "array element type")),
        array.getContainsNull());
  }

  private static MapType mapType(io.delta.kernel.proto.schema.MapType map) {
    return new MapType(
        dataType(required(map.hasKeyType(), map.getKeyType(), "map key type")),
        dataType(required(map.hasValueType(), map.getValueType(), "map value type")),
        map.getValueContainsNull());
  }

  private static String algorithm(EdgeInterpolationAlgorithm algorithm) {
    switch (algorithm) {
      case EDGE_INTERPOLATION_ALGORITHM_SPHERICAL:
        return "spherical";
      case EDGE_INTERPOLATION_ALGORITHM_VINCENTY:
        return "vincenty";
      case EDGE_INTERPOLATION_ALGORITHM_THOMAS:
        return "thomas";
      case EDGE_INTERPOLATION_ALGORITHM_ANDOYER:
        return "andoyer";
      case EDGE_INTERPOLATION_ALGORITHM_KARNEY:
        return "karney";
      case EDGE_INTERPOLATION_ALGORITHM_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw invalid("Geography edge algorithm is absent or unsupported: " + algorithm);
    }
  }

  private static FieldMetadata metadata(
      Map<String, io.delta.kernel.proto.schema.MetadataValue> metadata) {
    FieldMetadata.Builder builder = FieldMetadata.builder();
    metadata.forEach((key, value) -> metadata(builder, key, value));
    return builder.build();
  }

  private static void metadata(
      FieldMetadata.Builder builder,
      String key,
      io.delta.kernel.proto.schema.MetadataValue metadata) {
    switch (metadata.getValueCase()) {
      case NUMBER:
        builder.putLong(key, metadata.getNumber());
        break;
      case STRING:
        if (StructField.METADATA_SPEC_KEY.equals(key)) {
          builder.putMetadataColumnSpec(key, MetadataColumnSpec.fromString(metadata.getString()));
        } else {
          builder.putString(key, metadata.getString());
        }
        break;
      case BOOLEAN:
        builder.putBoolean(key, metadata.getBoolean());
        break;
      case OTHER_JSON:
        Object value =
            DataTypeJsonSerDe.deserializeFieldMetadata(
                    "{\"value\":" + metadata.getOtherJson() + "}")
                .get("value");
        putMetadata(builder, key, value);
        break;
      case VALUE_NOT_SET:
      default:
        throw invalid("Metadata value for `" + key + "` is absent or unsupported");
    }
  }

  private static void putMetadata(FieldMetadata.Builder builder, String key, Object value) {
    if (value == null) {
      builder.putNull(key);
    } else if (value instanceof Long) {
      builder.putLong(key, (Long) value);
    } else if (value instanceof Double) {
      builder.putDouble(key, (Double) value);
    } else if (value instanceof Boolean) {
      builder.putBoolean(key, (Boolean) value);
    } else if (value instanceof String) {
      builder.putString(key, (String) value);
    } else if (value instanceof FieldMetadata) {
      builder.putFieldMetadata(key, (FieldMetadata) value);
    } else if (value instanceof Long[]) {
      builder.putLongArray(key, (Long[]) value);
    } else if (value instanceof Double[]) {
      builder.putDoubleArray(key, (Double[]) value);
    } else if (value instanceof Boolean[]) {
      builder.putBooleanArray(key, (Boolean[]) value);
    } else if (value instanceof String[]) {
      builder.putStringArray(key, (String[]) value);
    } else if (value instanceof FieldMetadata[]) {
      builder.putFieldMetadataArray(key, (FieldMetadata[]) value);
    } else {
      throw invalid("Unsupported field metadata value for `" + key + "`: " + value);
    }
  }

  private static Column column(io.delta.kernel.proto.expressions.ColumnName column) {
    if (column.getPathCount() == 0) {
      throw invalid("Column path is empty");
    }
    return new Column(column.getPathList().toArray(new String[0]));
  }

  private static StructField resolveField(StructType schema, Column column) {
    DataType current = schema;
    StructField field = null;
    for (String name : column.getNames()) {
      if (!(current instanceof StructType)) {
        throw invalid("Column " + column + " traverses a non-struct type");
      }
      StructType struct = (StructType) current;
      int ordinal = struct.indexOf(name);
      if (ordinal < 0) {
        throw invalid("Column " + column + " is absent from schema " + schema);
      }
      field = struct.at(ordinal);
      current = field.getDataType();
    }
    return field;
  }

  private static void requireType(
      StructType schema, Column column, DataType expected, String context) {
    DataType actual = resolveField(schema, column).getDataType();
    if (!expected.equals(actual)) {
      throw invalid(context + " column " + column + " has type " + actual + ", not " + expected);
    }
  }

  private static void requireNonNullableType(
      StructType schema, Column column, DataType expected, String context) {
    DataType current = schema;
    StructField field = null;
    for (String name : column.getNames()) {
      if (!(current instanceof StructType)) {
        throw invalid("Dynamic scan " + context + " traverses a non-struct column");
      }
      StructType struct = (StructType) current;
      int ordinal = struct.indexOf(name);
      if (ordinal < 0) {
        throw invalid("Dynamic scan " + context + " column is absent: " + column);
      }
      field = struct.at(ordinal);
      if (field.isNullable()) {
        throw invalid("Dynamic scan " + context + " column is nullable: " + column);
      }
      current = field.getDataType();
    }
    if (!expected.equals(field.getDataType())) {
      throw invalid("Dynamic scan " + context + " column has type " + field.getDataType());
    }
  }

  private static byte narrowByte(int value) {
    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
      throw invalid("Byte scalar is out of range: " + value);
    }
    return (byte) value;
  }

  private static short narrowShort(int value) {
    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
      throw invalid("Short scalar is out of range: " + value);
    }
    return (short) value;
  }

  private static <T> T required(boolean present, T value, String name) {
    if (!present) {
      throw invalid("Missing " + name);
    }
    return value;
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException(message);
  }

  private static IllegalArgumentException invalid(String message, Throwable cause) {
    return new IllegalArgumentException(message, cause);
  }
}
