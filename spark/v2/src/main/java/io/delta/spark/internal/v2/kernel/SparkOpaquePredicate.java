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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.OpaquePredicate;
import java.util.Collections;
import org.apache.spark.sql.types.BooleanType;

/** A Kernel predicate whose complete semantics are carried by a Catalyst expression. */
public final class SparkOpaquePredicate extends OpaquePredicate {
  private final org.apache.spark.sql.catalyst.expressions.Expression catalyst;
  private final org.apache.spark.sql.catalyst.expressions.Expression semanticKey;

  /** Creates a carrier under the same binding contract as {@link SparkOpaqueExpression}. */
  public SparkOpaquePredicate(org.apache.spark.sql.catalyst.expressions.Expression catalyst) {
    super(Collections.emptyList());
    this.catalyst = requireNonNull(catalyst, "catalyst is null");
    if (!catalyst.resolved()) {
      throw new IllegalArgumentException("Catalyst predicate must be resolved: " + catalyst);
    }
    if (!catalyst.deterministic()) {
      throw new IllegalArgumentException("Catalyst predicate must be deterministic: " + catalyst);
    }
    if (!(catalyst.dataType() instanceof BooleanType)) {
      throw new IllegalArgumentException("Catalyst predicate must return boolean: " + catalyst);
    }
    this.semanticKey = catalyst.canonicalized();
  }

  public org.apache.spark.sql.catalyst.expressions.Expression catalyst() {
    return catalyst;
  }

  @Override
  protected Object semanticKey() {
    return semanticKey;
  }
}
