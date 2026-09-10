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
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Waits for a prefetched one-shot result only when first pulled. */
public final class FutureResultIterator implements CloseableIterator<FilteredColumnarBatch> {
  private CompletableFuture<CloseableIterator<FilteredColumnarBatch>> future;
  private CloseableIterator<FilteredColumnarBatch> delegate;
  private boolean closed;

  public FutureResultIterator(CompletableFuture<CloseableIterator<FilteredColumnarBatch>> future) {
    this.future = requireNonNull(future, "future is null");
  }

  @Override
  public boolean hasNext() {
    requireOpen();
    return delegate().hasNext();
  }

  @Override
  public FilteredColumnarBatch next() {
    requireOpen();
    CloseableIterator<FilteredColumnarBatch> iterator = delegate();
    if (!iterator.hasNext()) {
      throw new NoSuchElementException();
    }
    return iterator.next();
  }

  @Override
  public void close() {
    CompletableFuture<CloseableIterator<FilteredColumnarBatch>> pending;
    CloseableIterator<FilteredColumnarBatch> iterator;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      pending = future;
      iterator = delegate;
      future = null;
      delegate = null;
    }
    if (pending != null) {
      pending.whenComplete((result, failure) -> Utils.closeCloseablesSilently(result));
      pending.cancel(false);
    } else {
      Utils.closeCloseables(iterator);
    }
  }

  private CloseableIterator<FilteredColumnarBatch> delegate() {
    CompletableFuture<CloseableIterator<FilteredColumnarBatch>> pending;
    synchronized (this) {
      if (delegate != null) {
        return delegate;
      }
      pending = future;
    }
    CloseableIterator<FilteredColumnarBatch> resolved;
    try {
      resolved = pending.join();
    } catch (CompletionException failure) {
      throw rethrow(failure.getCause());
    }
    if (resolved == null) {
      throw new NullPointerException("Prefetch future produced a null iterator");
    }
    synchronized (this) {
      if (closed) {
        throw new IllegalStateException("Prefetched result iterator is closed");
      }
      if (delegate == null) {
        delegate = resolved;
        future = null;
      } else if (delegate != resolved) {
        Utils.closeCloseables(resolved);
      }
      return delegate;
    }
  }

  private synchronized void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Prefetched result iterator is closed");
    }
  }

  private static RuntimeException rethrow(Throwable failure) {
    if (failure instanceof RuntimeException) {
      return (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return new CompletionException(failure);
  }
}
