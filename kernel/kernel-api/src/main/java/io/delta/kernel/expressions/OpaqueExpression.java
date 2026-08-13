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

/**
 * An engine-defined expression whose semantics are opaque to Kernel.
 *
 * <p>The default engine rejects opaque expressions. Engines may use this node to preserve an
 * expression name and its children while routing the expression to an engine-specific handler.
 */
@Evolving
public final class OpaqueExpression implements Expression {
  private final String name;
  private final List<Expression> children;

  public OpaqueExpression(String name, List<Expression> children) {
    this.name = requireNonNull(name, "name is null");
    this.children =
        Collections.unmodifiableList(
            new ArrayList<>(requireNonNull(children, "children are null")));
  }

  public String getName() {
    return name;
  }

  @Override
  public List<Expression> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return String.format("OpaqueExpression(%s)", name);
  }
}
