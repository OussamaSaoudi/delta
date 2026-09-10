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

/**
 * An engine-defined expression whose semantics are opaque to Kernel.
 *
 * <p>The default engine rejects opaque expressions. Engine implementations provide a structural
 * semantic key that completely identifies their expression's behavior.
 */
@Evolving
public abstract class OpaqueExpression implements Expression {
  private final List<Expression> children;

  protected OpaqueExpression(List<Expression> children) {
    requireNonNull(children, "children are null");
    List<Expression> copy = new ArrayList<>(children.size());
    for (Expression child : children) {
      copy.add(requireNonNull(child, "child is null"));
    }
    this.children = Collections.unmodifiableList(copy);
  }

  /** Engine-owned, immutable key with structural equality for this expression's full semantics. */
  protected abstract Object semanticKey();

  @Override
  public List<Expression> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return String.format("OpaqueExpression(%s)", getClass().getName());
  }

  @Override
  public final boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    OpaqueExpression that = (OpaqueExpression) other;
    return Objects.equals(semanticKey(), that.semanticKey()) && children.equals(that.children);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(getClass(), semanticKey(), children);
  }
}
