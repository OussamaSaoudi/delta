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
package io.delta.kernel.execution;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.execution.OperatorDispatcher;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.utils.CloseableIterator;

/** Executes passive Kernel plans against one engine and optional shared-result policy. */
public final class PlanExecutor {
  private final PlanEngine engine;
  private final PlanResultCache cache;

  /** Uses the existing Kernel engine handlers as the execution engine. */
  public PlanExecutor(Engine engine, PlanResultCache cache) {
    this(new DefaultPlanEngine(engine), cache);
  }

  public PlanExecutor(PlanEngine engine, PlanResultCache cache) {
    this.engine = requireNonNull(engine, "engine is null");
    this.cache = requireNonNull(cache, "cache is null");
  }

  public CloseableIterator<ColumnarBatch> execute(PlanNode root) {
    return OperatorDispatcher.execute(requireNonNull(root, "root is null"), engine, cache);
  }
}
