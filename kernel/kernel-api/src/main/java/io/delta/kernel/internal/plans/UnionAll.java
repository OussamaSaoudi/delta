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

import io.delta.kernel.types.StructType;
import java.util.List;

/** Unordered bag union whose inputs and output share one exact schema. */
public final class UnionAll implements Operator {
  public static final UnionAll UNION_ALL = new UnionAll();

  private UnionAll() {}

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.isEmpty()) {
      throw new IllegalArgumentException("UnionAll requires at least one input");
    }

    StructType schema = requireNonNull(inputSchemas.get(0), "input schema is null");
    for (int inputIndex = 1; inputIndex < inputSchemas.size(); inputIndex++) {
      StructType inputSchema = requireNonNull(inputSchemas.get(inputIndex), "input schema is null");
      if (!schema.equals(inputSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "UnionAll input %s schema differs from input 0: %s vs %s",
                inputIndex, inputSchema, schema));
      }
    }
    return schema;
  }
}
