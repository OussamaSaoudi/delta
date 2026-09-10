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
package io.delta.kernel.internal.execution;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.execution.PlanResultCache;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.utils.CloseableIterator;
import java.util.IdentityHashMap;

/** Per-execution cache resolution, identity counts, and shared results. */
public final class ExecutionContext implements AutoCloseable {
  private final Engine engine;
  private ExpressionHandler expressions;
  private final PlanResultCache cache;
  private final IdentityHashMap<PlanNode, Integer> consumers = new IdentityHashMap<>();
  private final IdentityHashMap<PlanNode, CloseableIterator<FilteredColumnarBatch>> cached =
      new IdentityHashMap<>();
  private final IdentityHashMap<PlanNode, LazySharedIterable> shared = new IdentityHashMap<>();
  private boolean closed;

  public ExecutionContext(Engine engine, PlanResultCache cache, PlanNode root) {
    this.engine = requireNonNull(engine, "engine is null");
    this.cache = requireNonNull(cache, "cache is null");
    discover(requireNonNull(root, "root is null"));
  }

  Engine engine() {
    return engine;
  }

  ExpressionHandler expressions() {
    if (expressions == null) {
      expressions = requireNonNull(engine.getExpressionHandler(), "expression handler is null");
    }
    return expressions;
  }

  public CloseableIterator<FilteredColumnarBatch> open(PlanNode node) {
    requireOpen();
    int consumerCount = consumers.get(node);
    if (consumerCount <= 1) {
      return openOnce(node);
    }
    LazySharedIterable result = shared.get(node);
    if (result == null) {
      result = new LazySharedIterable(openOnce(node), consumerCount);
      shared.put(node, result);
    }
    return result.iterator();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    AutoCloseable[] closeables = new AutoCloseable[shared.size() + cached.size()];
    int index = 0;
    for (LazySharedIterable result : shared.values()) {
      closeables[index++] = result;
    }
    for (CloseableIterator<FilteredColumnarBatch> result : cached.values()) {
      closeables[index++] = result;
    }
    Utils.closeCloseables(closeables);
    shared.clear();
    cached.clear();
  }

  private CloseableIterator<FilteredColumnarBatch> openOnce(PlanNode node) {
    CloseableIterator<FilteredColumnarBatch> hit = cached.remove(node);
    return hit != null ? hit : OperatorDispatcher.open(node, this);
  }

  private void discover(PlanNode node) {
    if (consumers.merge(node, 1, Integer::sum) > 1) {
      return;
    }
    CloseableIterator<FilteredColumnarBatch> hit = cache.get(node);
    if (hit != null) {
      cached.put(node, hit);
      return;
    }
    for (PlanNode child : node.children()) {
      discover(child);
    }
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Execution is closed");
    }
  }
}
