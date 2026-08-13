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
package io.delta.kernel.expressions;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.StructType;
import java.util.List;

/**
 * Permissively parses a JSON string expression into a struct.
 *
 * <p>Unparseable input produces null output instead of failing expression evaluation. A null JSON
 * string is treated as an empty object, producing a non-null struct whose fields are null when the
 * output schema permits it.
 *
 * @since 4.4.0
 */
@Evolving
public final class ParseJson implements Expression {
  private final Expression jsonExpression;
  private final StructType outputSchema;

  public ParseJson(Expression jsonExpression, StructType outputSchema) {
    this.jsonExpression = requireNonNull(jsonExpression, "jsonExpression is null");
    this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
  }

  /** Returns the expression producing JSON strings. */
  public Expression getJsonExpression() {
    return jsonExpression;
  }

  /** Returns the schema used to decode each JSON object. */
  public StructType getOutputSchema() {
    return outputSchema;
  }

  @Override
  public List<Expression> getChildren() {
    return singletonList(jsonExpression);
  }

  @Override
  public String toString() {
    return String.format("PARSE_JSON(%s, %s)", jsonExpression, outputSchema);
  }
}
