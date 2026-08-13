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

/** An n-ary SQL AND or OR predicate. Empty AND is true and empty OR is false. */
@Evolving
public final class Junction extends Predicate {
  public enum Operator {
    AND,
    OR
  }

  private final Operator operator;
  private final List<Predicate> predicates;

  public Junction(Operator operator, List<Predicate> predicates) {
    super(requireNonNull(operator, "operator is null").name(), asExpressions(predicates));
    this.operator = operator;
    this.predicates =
        Collections.unmodifiableList(
            new ArrayList<>(requireNonNull(predicates, "predicates are null")));
  }

  public Operator getOperator() {
    return operator;
  }

  public List<Predicate> getPredicates() {
    return predicates;
  }

  private static List<Expression> asExpressions(List<Predicate> predicates) {
    requireNonNull(predicates, "predicates are null");
    List<Expression> expressions = new ArrayList<>(predicates.size());
    for (Predicate predicate : predicates) {
      expressions.add(requireNonNull(predicate, "predicate is null"));
    }
    return expressions;
  }
}
