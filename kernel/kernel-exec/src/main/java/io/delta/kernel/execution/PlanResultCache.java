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
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode.FileScan;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** A thread-safe FIFO map from scan nodes to one-shot prefetched row sources. */
public final class PlanResultCache implements AutoCloseable {
  private final int maxEntries;
  private final long maxBytes;
  private final LinkedHashMap<FileScan, RowSource> scans = new LinkedHashMap<>();
  private long weightBytes;
  private boolean closed;

  public PlanResultCache(int maxEntries, long maxBytes) {
    if (maxEntries <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Cache limits must be positive");
    }
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
  }

  /** Claims a prefetched scan result, or returns null on a miss. */
  public CloseableIterator<ColumnarBatch> get(FileScan scan) {
    RowSource source;
    synchronized (this) {
      requireOpen();
      requireNonNull(scan, "scan is null");
      source = scans.remove(scan);
      if (source == null) {
        return null;
      }
      weightBytes -= source.weight;
    }
    return source.open();
  }

  /** Installs a future one-shot streaming result without waiting for it. */
  public boolean prefetch(
      FileScan scan,
      CompletableFuture<CloseableIterator<ColumnarBatch>> result,
      long approximateBytes) {
    return install(scan, new RowSource(result, checkWeight(approximateBytes)));
  }

  public synchronized long weightBytes() {
    return weightBytes;
  }

  public void invalidateAll() {
    List<RowSource> removed;
    synchronized (this) {
      requireOpen();
      removed = new ArrayList<>(scans.values());
      scans.clear();
      weightBytes = 0;
    }
    closeAll(removed);
  }

  @Override
  public void close() {
    List<RowSource> removed;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      removed = new ArrayList<>(scans.values());
      scans.clear();
      weightBytes = 0;
    }
    closeAll(removed);
  }

  private boolean install(FileScan scan, RowSource offered) {
    requireNonNull(scan, "scan is null");
    requireNonNull(offered, "result is null");
    List<RowSource> removed = new ArrayList<>();
    boolean accepted;
    synchronized (this) {
      requireOpen();
      if (scans.containsKey(scan)) {
        removed.add(offered);
        accepted = false;
      } else {
        Iterator<Map.Entry<FileScan, RowSource>> iterator = scans.entrySet().iterator();
        while ((scans.size() >= maxEntries || offered.weight > maxBytes - weightBytes)
            && iterator.hasNext()) {
          RowSource evicted = iterator.next().getValue();
          removed.add(evicted);
          iterator.remove();
          weightBytes -= evicted.weight;
        }
        if (offered.weight > maxBytes) {
          removed.add(offered);
          accepted = false;
        } else {
          scans.put(scan, offered);
          weightBytes += offered.weight;
          accepted = true;
        }
      }
    }
    closeAll(removed);
    return accepted;
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Cache is closed");
    }
  }

  private static long checkWeight(long approximateBytes) {
    if (approximateBytes < 0) {
      throw new IllegalArgumentException("Result weight must be nonnegative");
    }
    return approximateBytes;
  }

  private static void closeAll(List<RowSource> sources) {
    Utils.closeCloseables(sources.toArray(new AutoCloseable[0]));
  }

  private static final class RowSource implements AutoCloseable {
    private final CompletableFuture<CloseableIterator<ColumnarBatch>> result;
    private final long weight;

    private RowSource(
        CompletableFuture<CloseableIterator<ColumnarBatch>> result, long approximateBytes) {
      this.result = requireNonNull(result, "result is null");
      this.weight = approximateBytes;
    }

    private CloseableIterator<ColumnarBatch> open() {
      try {
        return requireNonNull(result.join(), "Prefetch future produced a null iterator");
      } catch (CompletionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new CompletionException(cause);
      }
    }

    @Override
    public void close() {
      result.whenComplete((iterator, failure) -> Utils.closeCloseablesSilently(iterator));
      result.cancel(false);
    }
  }
}
