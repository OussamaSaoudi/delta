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
package io.delta.spark.internal.v2.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultExpressionHandler;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

public class SparkExpressionHandlerTest {
  private static final StructType INPUT_SCHEMA =
      new StructType().add("left", IntegerType.INTEGER).add("right", IntegerType.INTEGER);
  private static final org.apache.spark.sql.types.StructType SPARK_INPUT_SCHEMA =
      new org.apache.spark.sql.types.StructType()
          .add("left", DataTypes.IntegerType, false)
          .add("right", DataTypes.IntegerType, false);

  private final ExpressionHandler delegate = new DefaultExpressionHandler();
  private final SparkExpressionHandler expressions = new SparkExpressionHandler(delegate);

  @Test
  public void evaluatesStructValuedProjectWithoutCopyingInputRows() {
    Add sum = new Add(intReference(0), intReference(1));
    CreateNamedStruct row =
        new CreateNamedStruct(
            ScalaUtils.toScalaList(
                new org.apache.spark.sql.catalyst.expressions.Expression[] {
                  Literal.create("sum", DataTypes.StringType), sum
                }));
    StructType outputSchema = new StructType().add("sum", IntegerType.INTEGER, false);

    ExpressionEvaluator evaluator =
        expressions.getEvaluator(INPUT_SCHEMA, new SparkOpaqueExpression(row), outputSchema);
    FilteredColumnarBatch result = evaluator.eval(inputBatch(FilteredColumnarBatch.Lifetime.OWNED));

    assertInstanceOf(SparkRowArrayBatch.class, result.getData());
    assertEquals(FilteredColumnarBatch.Lifetime.BORROWED, result.getLifetime());
    assertEquals(3, result.getData().getColumnVector(0).getInt(0));
    assertEquals(7, result.getData().getColumnVector(0).getInt(1));
  }

  @Test
  public void evaluatesExecutorStructWrapper() {
    StructType outputSchema = new StructType().add("sum", IntegerType.INTEGER, false);
    StructExpression wrapper =
        new StructExpression(
            List.of(new SparkOpaqueExpression(new Add(intReference(0), intReference(1)))));

    ExpressionEvaluator evaluator = expressions.getEvaluator(INPUT_SCHEMA, wrapper, outputSchema);
    FilteredColumnarBatch result =
        evaluator.eval(inputBatch(FilteredColumnarBatch.Lifetime.BORROWED));

    assertEquals(3, result.getData().getColumnVector(0).getInt(0));
    assertEquals(11, result.getData().getColumnVector(0).getInt(2));
  }

  @Test
  public void predicateNarrowsExistingSelectionAndPreservesLifetime() {
    GreaterThan greaterThanOne =
        new GreaterThan(intReference(0), Literal.create(1, DataTypes.IntegerType));
    PredicateEvaluator evaluator =
        expressions.getPredicateEvaluator(INPUT_SCHEMA, new SparkOpaquePredicate(greaterThanOne));
    FilteredColumnarBatch input = inputBatch(FilteredColumnarBatch.Lifetime.OWNED);
    ColumnVector existing = delegate.createSelectionVector(new boolean[] {true, false, true}, 0, 3);

    FilteredColumnarBatch result =
        evaluator.eval(input.withSelectionVector(existing, FilteredColumnarBatch.Lifetime.OWNED));

    assertEquals(FilteredColumnarBatch.Lifetime.OWNED, result.getLifetime());
    assertFalse(result.isSelected(0));
    assertFalse(result.isSelected(1));
    assertTrue(result.isSelected(2));
  }

  @Test
  public void opaqueEqualityUsesCanonicalCatalystSemantics() {
    SparkOpaqueExpression first =
        new SparkOpaqueExpression(
            new Add(intReference(0), Literal.create(1, DataTypes.IntegerType)));
    SparkOpaqueExpression same =
        new SparkOpaqueExpression(
            new Add(intReference(0), Literal.create(1, DataTypes.IntegerType)));
    SparkOpaqueExpression different =
        new SparkOpaqueExpression(
            new Add(intReference(0), Literal.create(2, DataTypes.IntegerType)));

    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertFalse(first.equals(different));
  }

  private FilteredColumnarBatch inputBatch(FilteredColumnarBatch.Lifetime lifetime) {
    InternalRow[] rows = {
      new GenericInternalRow(new Object[] {1, 2}),
      new GenericInternalRow(new Object[] {3, 4}),
      new GenericInternalRow(new Object[] {5, 6})
    };
    SparkRowArrayBatch batch =
        new SparkRowArrayBatch(INPUT_SCHEMA, SPARK_INPUT_SCHEMA, rows, rows.length);
    return new FilteredColumnarBatch(batch, Optional.empty(), lifetime);
  }

  private static BoundReference intReference(int ordinal) {
    return new BoundReference(ordinal, DataTypes.IntegerType, false);
  }
}
