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
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/** Lazily pulls one child into an owned prefix shared by independent cursors. */
final class LazySharedIterable implements CloseableIterable<FilteredColumnarBatch> {
  private final ArrayList<FilteredColumnarBatch> buffered = new ArrayList<>();
  private final int expectedCursors;
  private CloseableIterator<FilteredColumnarBatch> child;
  private int openedCursors;
  private int closedCursors;
  private boolean exhausted;
  private boolean closed;
  private Throwable failure;

  LazySharedIterable(CloseableIterator<FilteredColumnarBatch> child, int expectedCursors) {
    this.child = requireNonNull(child, "child is null");
    if (expectedCursors <= 1) {
      throw new IllegalArgumentException("A shared result requires at least two cursors");
    }
    this.expectedCursors = expectedCursors;
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> iterator() {
    requireOpen();
    if (openedCursors == expectedCursors) {
      throw new IllegalStateException("Too many cursors opened for a shared result");
    }
    openedCursors++;
    return new Cursor();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    CloseableIterator<FilteredColumnarBatch> source = child;
    child = null;
    buffered.clear();
    Utils.closeCloseables(source);
  }

  private boolean ensureAvailable(int index) {
    requireOpen();
    rethrowFailure();
    if (index < buffered.size()) {
      return true;
    }
    if (exhausted) {
      return false;
    }
    try {
      if (!child.hasNext()) {
        exhausted = true;
        CloseableIterator<FilteredColumnarBatch> source = child;
        child = null;
        Utils.closeCloseables(source);
        return false;
      }
      buffered.add(OwnedBatches.retain(child.next()));
      return true;
    } catch (Throwable pullFailure) {
      failure = pullFailure;
      CloseableIterator<FilteredColumnarBatch> source = child;
      child = null;
      Utils.closeCloseablesAndAddSuppressed(pullFailure, source);
      rethrowFailure();
      return false;
    }
  }

  private void cursorClosed() {
    closedCursors++;
    if (closedCursors == expectedCursors) {
      close();
    }
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Shared result is closed");
    }
  }

  private void rethrowFailure() {
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure != null) {
      throw new RuntimeException(failure);
    }
  }

  private final class Cursor implements CloseableIterator<FilteredColumnarBatch> {
    private int index;
    private boolean cursorClosed;

    @Override
    public boolean hasNext() {
      requireCursorOpen();
      boolean available = ensureAvailable(index);
      if (!available) {
        close();
      }
      return available;
    }

    @Override
    public FilteredColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return buffered.get(index++);
    }

    @Override
    public void close() {
      if (cursorClosed) {
        return;
      }
      cursorClosed = true;
      cursorClosed();
    }

    private void requireCursorOpen() {
      if (cursorClosed) {
        throw new IllegalStateException("Shared cursor is closed");
      }
    }
  }
}
