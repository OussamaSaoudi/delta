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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;

/**
 * A predicate kind that is unknown to this version of Kernel.
 *
 * <p>Evaluation must fail instead of treating this predicate as null or otherwise guessing its
 * semantics.
 */
@Evolving
public final class UnknownPredicate extends Predicate {
  private final String unknownName;

  public UnknownPredicate(String name) {
    super("UNKNOWN_PREDICATE", emptyList());
    this.unknownName = requireNonNull(name, "name is null");
  }

  public String getUnknownName() {
    return unknownName;
  }

  @Override
  public String toString() {
    return String.format("UnknownPredicate(%s)", unknownName);
  }
}
