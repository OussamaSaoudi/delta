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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.execution.ExecutionContext;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.utils.CloseableIterator;
import java.util.NoSuchElementException;

/** Executes passive Kernel plans against one engine and optional shared-result policy. */
public final class PlanExecutor {
  private final Engine engine;
  private final PlanResultCache cache;

  public PlanExecutor(Engine engine, PlanResultCache cache) {
    this.engine = requireNonNull(engine, "engine is null");
    this.cache = requireNonNull(cache, "cache is null");
  }

  public CloseableIterator<FilteredColumnarBatch> execute(PlanNode root) {
    requireNonNull(root, "root is null");
    ExecutionContext context = new ExecutionContext(engine, cache, root);
    try {
      return new RootIterator(context.open(root), context);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, context);
      throw failure;
    }
  }

  private static final class RootIterator implements CloseableIterator<FilteredColumnarBatch> {
    private CloseableIterator<FilteredColumnarBatch> delegate;
    private ExecutionContext context;
    private boolean closed;

    private RootIterator(
        CloseableIterator<FilteredColumnarBatch> delegate, ExecutionContext context) {
      this.delegate = delegate;
      this.context = context;
    }

    @Override
    public boolean hasNext() {
      requireOpen();
      try {
        boolean hasNext = delegate.hasNext();
        if (!hasNext) {
          close();
        }
        return hasNext;
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public FilteredColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return delegate.next();
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      CloseableIterator<FilteredColumnarBatch> iterator = delegate;
      ExecutionContext execution = context;
      delegate = null;
      context = null;
      Utils.closeCloseables(iterator, execution);
    }

    private void closeAfterFailure(Throwable failure) {
      if (closed) {
        return;
      }
      closed = true;
      CloseableIterator<FilteredColumnarBatch> iterator = delegate;
      ExecutionContext execution = context;
      delegate = null;
      context = null;
      Utils.closeCloseablesAndAddSuppressed(failure, iterator, execution);
    }

    private void requireOpen() {
      if (closed) {
        throw new IllegalStateException("Execution result is closed");
      }
    }
  }
}
