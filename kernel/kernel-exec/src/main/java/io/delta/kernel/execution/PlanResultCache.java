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
import io.delta.kernel.internal.execution.FutureResultIterator;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** A thread-safe FIFO cache of replayable results and one-shot prefetched results. */
public final class PlanResultCache implements AutoCloseable {
  private final int maxEntries;
  private final long maxBytes;
  private final boolean disabled;
  private final LinkedHashMap<PlanNode, Entry> entries = new LinkedHashMap<>();
  private long weightBytes;
  private boolean closed;

  public PlanResultCache(int maxEntries, long maxBytes) {
    this(maxEntries, maxBytes, false);
    if (maxEntries <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Cache limits must be positive");
    }
  }

  private PlanResultCache(int maxEntries, long maxBytes, boolean disabled) {
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
    this.disabled = disabled;
  }

  /** Returns a cache that always misses and closes every result offered to it. */
  public static PlanResultCache disabled() {
    return new PlanResultCache(0, 0, true);
  }

  /** Returns one execution cursor, or null on a miss. */
  public synchronized CloseableIterator<FilteredColumnarBatch> get(PlanNode plan) {
    requireOpen();
    requireNonNull(plan, "plan is null");
    Entry entry = entries.get(plan);
    if (entry == null) {
      return null;
    }
    CloseableIterator<FilteredColumnarBatch> result = entry.open();
    if (entry.oneShot()) {
      entries.remove(plan);
      recomputeWeight();
    }
    return result;
  }

  /** Installs an owned, complete result that may be read more than once. */
  public void put(
      PlanNode plan, CloseableIterable<FilteredColumnarBatch> ownedResult, long approximateBytes) {
    install(plan, new ReplayableEntry(ownedResult, checkWeight(approximateBytes)));
  }

  /** Installs a ready, one-shot streaming result. */
  public boolean prefetch(
      PlanNode plan, CloseableIterator<FilteredColumnarBatch> result, long approximateBytes) {
    requireNonNull(result, "result is null");
    return prefetch(plan, CompletableFuture.completedFuture(result), approximateBytes);
  }

  /** Installs a future one-shot streaming result without waiting for it. */
  public boolean prefetch(
      PlanNode plan,
      CompletableFuture<CloseableIterator<FilteredColumnarBatch>> result,
      long approximateBytes) {
    return install(plan, new OneShotEntry(result, checkWeight(approximateBytes)));
  }

  public synchronized long weightBytes() {
    return weightBytes;
  }

  public void invalidateAll() {
    List<Entry> removed;
    synchronized (this) {
      requireOpen();
      removed = new ArrayList<>(entries.values());
      entries.clear();
      weightBytes = 0;
    }
    closeAll(removed);
  }

  @Override
  public void close() {
    List<Entry> removed;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      removed = new ArrayList<>(entries.values());
      entries.clear();
      weightBytes = 0;
    }
    closeAll(removed);
  }

  private boolean install(PlanNode plan, Entry offered) {
    requireNonNull(plan, "plan is null");
    requireNonNull(offered, "result is null");
    List<Entry> removed = new ArrayList<>();
    boolean accepted;
    synchronized (this) {
      requireOpen();
      if (disabled || entries.containsKey(plan)) {
        removed.add(offered);
        accepted = false;
      } else {
        entries.put(plan, offered);
        weightBytes = saturatedAdd(weightBytes, offered.weight());
        Iterator<Map.Entry<PlanNode, Entry>> iterator = entries.entrySet().iterator();
        while ((entries.size() > maxEntries || weightBytes > maxBytes) && iterator.hasNext()) {
          removed.add(iterator.next().getValue());
          iterator.remove();
          recomputeWeight();
        }
        accepted = entries.get(plan) == offered;
      }
    }
    closeAll(removed);
    return accepted;
  }

  private void recomputeWeight() {
    weightBytes = 0;
    for (Map.Entry<PlanNode, Entry> entry : entries.entrySet()) {
      weightBytes = saturatedAdd(weightBytes, entry.getValue().weight());
    }
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

  private static long saturatedAdd(long left, long right) {
    return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
  }

  private static void closeAll(List<Entry> entries) {
    Utils.closeCloseables(entries.toArray(new AutoCloseable[0]));
  }

  private interface Entry extends AutoCloseable {
    CloseableIterator<FilteredColumnarBatch> open();

    long weight();

    boolean oneShot();

    void close();
  }

  private static final class ReplayableEntry implements Entry {
    private final CloseableIterable<FilteredColumnarBatch> result;
    private final long weight;
    private int pins;
    private boolean evicted;

    private ReplayableEntry(
        CloseableIterable<FilteredColumnarBatch> result, long approximateBytes) {
      this.result = requireNonNull(result, "ownedResult is null");
      this.weight = approximateBytes;
    }

    @Override
    public synchronized CloseableIterator<FilteredColumnarBatch> open() {
      if (evicted) {
        throw new IllegalStateException("Cache entry is evicted");
      }
      pins++;
      try {
        return new PinnedIterator(result.iterator(), this);
      } catch (RuntimeException | Error failure) {
        release();
        throw failure;
      }
    }

    @Override
    public long weight() {
      return weight;
    }

    @Override
    public boolean oneShot() {
      return false;
    }

    @Override
    public synchronized void close() {
      evicted = true;
      closeIfUnpinned();
    }

    private synchronized void release() {
      if (pins <= 0) {
        throw new IllegalStateException("Cache entry is not pinned");
      }
      pins--;
      closeIfUnpinned();
    }

    private void closeIfUnpinned() {
      if (evicted && pins == 0) {
        Utils.closeCloseables(result);
      }
    }
  }

  private static final class OneShotEntry implements Entry {
    private final CompletableFuture<CloseableIterator<FilteredColumnarBatch>> result;
    private final long weight;

    private OneShotEntry(
        CompletableFuture<CloseableIterator<FilteredColumnarBatch>> result, long approximateBytes) {
      this.result = requireNonNull(result, "result is null");
      this.weight = approximateBytes;
    }

    @Override
    public CloseableIterator<FilteredColumnarBatch> open() {
      return new FutureResultIterator(result);
    }

    @Override
    public long weight() {
      return weight;
    }

    @Override
    public boolean oneShot() {
      return true;
    }

    @Override
    public void close() {
      result.whenComplete((iterator, failure) -> Utils.closeCloseablesSilently(iterator));
      result.cancel(false);
    }
  }

  private static final class PinnedIterator implements CloseableIterator<FilteredColumnarBatch> {
    private CloseableIterator<FilteredColumnarBatch> delegate;
    private ReplayableEntry owner;

    private PinnedIterator(
        CloseableIterator<FilteredColumnarBatch> delegate, ReplayableEntry owner) {
      this.delegate = requireNonNull(delegate, "cache iterator is null");
      this.owner = owner;
    }

    @Override
    public boolean hasNext() {
      requireOpen();
      return delegate.hasNext();
    }

    @Override
    public FilteredColumnarBatch next() {
      requireOpen();
      return delegate.next();
    }

    @Override
    public void close() {
      if (delegate == null) {
        return;
      }
      CloseableIterator<FilteredColumnarBatch> iterator = delegate;
      ReplayableEntry entry = owner;
      delegate = null;
      owner = null;
      Utils.closeCloseables(iterator, entry::release);
    }

    private void requireOpen() {
      if (delegate == null) {
        throw new IllegalStateException("Cached result iterator is closed");
      }
    }
  }
}
