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

import io.delta.kernel.annotation.Evolving;
import java.util.Collections;
import java.util.Objects;

/**
 * An engine-defined plan predicate whose semantics are opaque to Kernel.
 *
 * <p>The default engine rejects opaque predicates. Engine implementations provide a structural
 * semantic key that completely identifies their predicate's behavior.
 */
@Evolving
public abstract class OpaquePredicate extends Predicate {
  protected OpaquePredicate() {
    super("OPAQUE_PREDICATE", Collections.emptyList());
  }

  /** Engine-owned, immutable key with structural equality for this predicate's full semantics. */
  protected abstract Object semanticKey();

  @Override
  public String toString() {
    return String.format("OpaquePredicate(%s)", getClass().getName());
  }

  @Override
  public final boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    OpaquePredicate that = (OpaquePredicate) other;
    return Objects.equals(semanticKey(), that.semanticKey());
  }

  @Override
  public final int hashCode() {
    return Objects.hash(getClass(), semanticKey());
  }
}
