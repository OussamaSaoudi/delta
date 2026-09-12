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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One struct-valued expression with one child per output field. */
@Evolving
public final class StructExpression implements Expression {
  private final List<Expression> fields;

  public StructExpression(List<? extends Expression> fields) {
    requireNonNull(fields, "fields is null");
    ArrayList<Expression> copy = new ArrayList<>(fields.size());
    for (Expression field : fields) {
      copy.add(requireNonNull(field, "field expression is null"));
    }
    this.fields = Collections.unmodifiableList(copy);
  }

  public List<Expression> fields() {
    return fields;
  }

  @Override
  public List<Expression> getChildren() {
    return fields;
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || (other instanceof StructExpression && fields.equals(((StructExpression) other).fields));
  }

  @Override
  public int hashCode() {
    return Objects.hash(StructExpression.class, fields);
  }

  @Override
  public String toString() {
    return "STRUCT" + fields;
  }
}
