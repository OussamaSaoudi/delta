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
package io.delta.kernel.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.expressions.StructPatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Projects one input through a struct expression into a declared output schema.
 *
 * <p>The expression must be a dense {@link StructExpression} or sparse {@link StructPatch}. The
 * declared schema supplies the result's logical field names and types; expression evaluators
 * validate field type compatibility when they bind the expression to an input.
 */
public final class Project implements Operator {
  private final Expression expression;
  private final StructType schema;

  public Project(Expression expression, StructType schema) {
    this.expression = requireNonNull(expression, "expression is null");
    this.schema = requireNonNull(schema, "schema is null");
    if (!(expression instanceof StructExpression) && !(expression instanceof StructPatch)) {
      throw new IllegalArgumentException(
          "Project expression must be a StructExpression or StructPatch");
    }
  }

  public Expression getExpression() {
    return expression;
  }

  public StructType getSchema() {
    return schema;
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.size() != 1) {
      throw new IllegalArgumentException("Project requires one input, got " + inputSchemas.size());
    }
    StructType input = requireNonNull(inputSchemas.get(0), "input schema is null");
    int fields = validateAndCountFields(input, expression);
    if (fields != schema.length()) {
      throw new IllegalArgumentException(
          "Project expression produces "
              + fields
              + " fields, but output schema declares "
              + schema.length());
    }
    return schema;
  }

  private static int validateAndCountFields(StructType input, Expression expression) {
    if (expression instanceof StructExpression) {
      StructExpression struct = (StructExpression) expression;
      validateReferences(input, struct);
      return struct.getFieldExpressions().size();
    }
    return validatePatchAndCountFields(input, (StructPatch) expression);
  }

  private static int validatePatchAndCountFields(StructType input, StructPatch patch) {
    StructType source = resolvePatchSource(input, patch);
    int fields = patch.getPrependedFields().size() + patch.getAppendedFields().size();
    Set<String> matchedTransforms = new HashSet<>();

    validateExpressions(input, patch.getPrependedFields());
    for (StructField sourceField : source.fields()) {
      StructPatch.FieldTransform transform = patch.getFieldTransforms().get(sourceField.getName());
      if (transform == null) {
        fields++;
        continue;
      }
      matchedTransforms.add(sourceField.getName());
      if (!transform.isReplace()) {
        fields++;
      }
      fields += transform.getExpressions().size();
      validateExpressions(input, transform.getExpressions());
    }

    patch
        .getFieldTransforms()
        .forEach(
            (name, transform) -> {
              if (!transform.isOptional() && !matchedTransforms.contains(name)) {
                throw new IllegalArgumentException(
                    "Required Project struct patch field does not exist: " + name);
              }
            });
    validateExpressions(input, patch.getAppendedFields());
    return fields;
  }

  private static StructType resolvePatchSource(StructType input, StructPatch patch) {
    if (!patch.getInputPath().isPresent()) {
      return input;
    }
    StructField field =
        PlanSchemaUtils.resolveField(input, patch.getInputPath().get(), "Project struct patch");
    DataType type = field.getDataType();
    if (!(type instanceof StructType)) {
      throw new IllegalArgumentException(
          "Project struct patch input path must resolve to a struct");
    }
    return (StructType) type;
  }

  private static void validateExpressions(StructType input, List<Expression> expressions) {
    for (Expression child : expressions) {
      validateReferences(input, child);
    }
  }

  private static void validateReferences(StructType input, Expression expression) {
    if (expression instanceof StructPatch) {
      validatePatchAndCountFields(input, (StructPatch) expression);
      return;
    }
    if (expression instanceof Column) {
      PlanSchemaUtils.resolveField(input, (Column) expression, "Project expression");
    }
    for (Expression child : expression.getChildren()) {
      validateReferences(input, child);
    }
  }
}
