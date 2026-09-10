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
import static org.junit.jupiter.api.Assertions.assertSame;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class SparkRowArrayBatchTest {
  @Test
  public void exposesCachedZeroCopyViewsOfRootAndNestedRows() {
    StructType nestedKernelSchema = new StructType().add("text", StringType.STRING, true);
    StructType kernelSchema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("nested", nestedKernelSchema, true);
    org.apache.spark.sql.types.StructType nestedSparkSchema =
        new org.apache.spark.sql.types.StructType().add("text", DataTypes.StringType, true);
    org.apache.spark.sql.types.StructType sparkSchema =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("nested", nestedSparkSchema, true);

    GenericInternalRow nested =
        new GenericInternalRow(new Object[] {UTF8String.fromString("before")});
    GenericInternalRow row = new GenericInternalRow(new Object[] {1, nested});
    SparkRowArrayBatch batch =
        new SparkRowArrayBatch(kernelSchema, sparkSchema, new InternalRow[] {row}, 1);

    ColumnVector ids = batch.getColumnVector(0);
    ColumnVector texts = batch.getColumnVector(1).getChild(0);
    assertSame(ids, batch.getColumnVector(0));
    assertSame(texts, batch.getColumnVector(1).getChild(0));
    assertSame(row, batch.backingRow(0));
    assertEquals(1, ids.getInt(0));
    assertEquals("before", texts.getString(0));

    row.update(0, 2);
    nested.update(0, UTF8String.fromString("after"));
    assertEquals(2, ids.getInt(0));
    assertEquals("after", texts.getString(0));
  }
}
