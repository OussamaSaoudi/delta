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
package io.delta.kernel.plans;

import io.delta.kernel.types.StructType;
import java.util.List;

/** One immutable, declarative operation in a Kernel execution plan. */
public abstract class PlanNode {
  private volatile boolean hashCodeMemoized;
  private int memoizedHashCode;

  PlanNode() {}

  /** The schema produced by this node. */
  public abstract StructType outputSchema();

  /** This node's inputs in operator-defined order. */
  public abstract List<PlanNode> children();

  protected final boolean hasMemoizedHashCode() {
    return hashCodeMemoized;
  }

  protected final int memoizedHashCode() {
    return memoizedHashCode;
  }

  protected final int memoizeHashCode(int hashCode) {
    memoizedHashCode = hashCode;
    hashCodeMemoized = true;
    return hashCode;
  }

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();
}
