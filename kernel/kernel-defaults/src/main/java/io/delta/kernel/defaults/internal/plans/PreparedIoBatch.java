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
package io.delta.kernel.defaults.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A set of file-I/O tasks registered before fair, deterministic submission. */
final class PreparedIoBatch implements AutoCloseable {
  private static final int FALLBACK_MAX_ACTIVE_READERS = 32;

  @FunctionalInterface
  interface IteratorPreparer<T> {
    CloseableIterator<T> prepare();
  }

  private enum State {
    REGISTERING,
    LAUNCHED,
    CLOSED
  }

  private final Object lock = new Object();
  private final ExecutorService executor;
  private final int maxActiveReaders;
  private final List<TrackedFutureTask<?>> registeredTasks = new ArrayList<>();
  private final Set<TrackedFutureTask<?>> submittedTasks =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final List<SourceGroup> iteratorGroups = new ArrayList<>();
  private final Deque<SourceGroup> pendingGroups = new ArrayDeque<>();
  private State state = State.REGISTERING;
  private int effectiveMaxActiveReaders;
  private int activeReaders;
  private boolean ownershipClaimed;

  PreparedIoBatch(ExecutorService executor) {
    this(executor, defaultMaxActiveReaders(executor));
  }

  /** Test hook for exercising a deterministic read-ahead window. */
  PreparedIoBatch(ExecutorService executor, int maxActiveReaders) {
    this.executor = requireNonNull(executor, "I/O executor is null");
    if (maxActiveReaders <= 0) {
      throw new IllegalArgumentException("Maximum active readers must be positive");
    }
    this.maxActiveReaders = maxActiveReaders;
  }

  private static int defaultMaxActiveReaders(ExecutorService executor) {
    requireNonNull(executor, "I/O executor is null");
    if (executor instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
      if (threadPool.getCorePoolSize() == threadPool.getMaximumPoolSize()) {
        return Math.max(1, threadPool.getMaximumPoolSize());
      }
    }
    return FALLBACK_MAX_ACTIVE_READERS;
  }

  /** Registers a task that may be associated with a reader as its leading submission. */
  <T> Future<T> registerTask(Callable<T> callable) {
    requireNonNull(callable, "I/O task is null");
    synchronized (lock) {
      ensureRegistering();
      TrackedFutureTask<T> task = trackedTask(callable);
      registeredTasks.add(task);
      return new RegisteredFuture<>(this, task);
    }
  }

  /**
   * Registers readers as one ordered source group. Launch interleaves reader groups by ordinal,
   * while each returned list retains its declared output order.
   */
  <T> List<CloseableIterator<T>> registerIteratorGroup(
      List<? extends IteratorPreparer<T>> preparers) {
    requireNonNull(preparers, "reader preparers are null");
    return registerIteratorGroup(preparers, Collections.nCopies(preparers.size(), null));
  }

  /**
   * Registers readers with one optional leading task per reader. Leading tasks only control fair
   * submission order; reader workers must not wait for them.
   */
  <T> List<CloseableIterator<T>> registerIteratorGroup(
      List<? extends IteratorPreparer<T>> preparers, List<? extends Future<?>> leadingTasks) {
    requireNonNull(preparers, "reader preparers are null");
    requireNonNull(leadingTasks, "leading tasks are null");
    if (preparers.size() != leadingTasks.size()) {
      throw new IllegalArgumentException("Each reader must have one leading-task slot");
    }
    synchronized (lock) {
      ensureRegistering();
      SourceGroup group = new SourceGroup();
      List<CloseableIterator<T>> result = new ArrayList<>(preparers.size());
      for (int ordinal = 0; ordinal < preparers.size(); ordinal++) {
        IteratorPreparer<T> preparer = preparers.get(ordinal);
        IteratorSlot<T> slot =
            new IteratorSlot<>(
                this,
                requireNonNull(preparer, "reader preparer is null"),
                registeredTask(leadingTasks.get(ordinal)),
                group);
        group.add(slot);
        result.add(slot.iterator);
      }
      iteratorGroups.add(group);
      return Collections.unmodifiableList(result);
    }
  }

  /** Finalizes registration and fills the bounded reader window in fair source order. */
  void launch() {
    List<TrackedFutureTask<?>> submitted = new ArrayList<>();
    Throwable failure = null;
    synchronized (lock) {
      if (state == State.LAUNCHED) {
        return;
      }
      if (state == State.CLOSED) {
        throw new IllegalStateException("Prepared I/O batch is closed");
      }
      try {
        Set<TrackedFutureTask<?>> associated = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SourceGroup group : iteratorGroups) {
          for (IteratorSlot<?> slot : group.pending) {
            if (slot.leadingTask != null) {
              associated.add(slot.leadingTask);
            }
          }
        }
        for (TrackedFutureTask<?> task : registeredTasks) {
          if (!associated.contains(task)) {
            submit(task, submitted, submittedTasks);
          }
        }
        state = State.LAUNCHED;
        launchFirstReaderPerGroup(submitted, submittedTasks);
        fillReaderWindow(submitted, submittedTasks);
      } catch (RuntimeException | Error launchFailure) {
        failure = launchFailure;
      }
    }
    if (failure != null) {
      for (TrackedFutureTask<?> task : submitted) {
        task.cancelTracked();
      }
      closeAfterFailure(failure);
      throwFailure(failure);
    }
  }

  /** Retains this batch's task and reader ownership for a composed output iterator. */
  <T> CloseableIterator<T> own(CloseableIterator<T> delegate) {
    requireNonNull(delegate, "output iterator is null");
    synchronized (lock) {
      if (state == State.CLOSED) {
        throw new IllegalStateException("Prepared I/O batch is closed");
      }
      if (ownershipClaimed) {
        throw new IllegalStateException("Prepared I/O batch already has an output owner");
      }
      ownershipClaimed = true;
    }
    return new CloseableIterator<T>() {
      private boolean closed;

      @Override
      public boolean hasNext() {
        try {
          return !closed && delegate.hasNext();
        } catch (RuntimeException | Error failure) {
          closeAfterFailure(failure);
          throw failure;
        }
      }

      @Override
      public T next() {
        try {
          return delegate.next();
        } catch (RuntimeException | Error failure) {
          closeAfterFailure(failure);
          throw failure;
        }
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          Utils.closeCloseables(PreparedIoBatch.this, delegate);
        }
      }

      private void closeAfterFailure(Throwable failure) {
        if (!closed) {
          closed = true;
          Utils.closeCloseablesAndAddSuppressed(failure, PreparedIoBatch.this, delegate);
        }
      }
    };
  }

  @Override
  public void close() throws IOException {
    List<PreparedIterator<?>> iterators;
    List<TrackedFutureTask<?>> tasks;
    synchronized (lock) {
      if (state == State.CLOSED) {
        return;
      }
      state = State.CLOSED;
      iterators = flattenIteratorGroups();
      tasks = new ArrayList<>(registeredTasks);
      pendingGroups.clear();
      activeReaders = 0;
      for (SourceGroup group : iteratorGroups) {
        for (IteratorSlot<?> slot : group.slots) {
          slot.state = IteratorState.RELEASED;
        }
        group.pending.clear();
      }
    }

    List<PreparedIterator.CloseState<?>> closeStates = new ArrayList<>(iterators.size());
    for (PreparedIterator<?> iterator : iterators) {
      closeStates.add(iterator.beginClose());
    }
    tasks.forEach(TrackedFutureTask::cancelTracked);

    Throwable failure = null;
    for (PreparedIterator.CloseState<?> closeState : closeStates) {
      failure = closeState.closeReader(failure);
    }
    for (TrackedFutureTask<?> task : tasks) {
      task.awaitExit();
    }
    for (PreparedIterator.CloseState<?> closeState : closeStates) {
      closeState.awaitExit();
      failure = closeState.addAsynchronousFailure(failure);
    }
    throwCloseFailure(failure);
  }

  private <T> TrackedFutureTask<T> trackedTask(Callable<T> callable) {
    return new TrackedFutureTask<>(callable);
  }

  private void submit(
      TrackedFutureTask<?> task,
      List<TrackedFutureTask<?>> submitted,
      Set<TrackedFutureTask<?>> submittedSet) {
    executor.execute(task);
    submitted.add(task);
    submittedSet.add(task);
  }

  private void launchFirstReaderPerGroup(
      List<TrackedFutureTask<?>> submitted, Set<TrackedFutureTask<?>> submittedSet) {
    List<IteratorSlot<?>> firstReaders = new ArrayList<>();
    for (SourceGroup group : iteratorGroups) {
      IteratorSlot<?> first = group.pollPending();
      if (first != null) {
        firstReaders.add(first);
      }
    }
    effectiveMaxActiveReaders = Math.max(maxActiveReaders, firstReaders.size());
    for (IteratorSlot<?> slot : firstReaders) {
      activate(slot, submitted, submittedSet);
      if (slot.group.hasPending()) {
        pendingGroups.addLast(slot.group);
      }
    }
  }

  /** Caller holds {@link #lock}. */
  private void fillReaderWindow(
      List<TrackedFutureTask<?>> submitted, Set<TrackedFutureTask<?>> submittedSet) {
    while (state == State.LAUNCHED
        && activeReaders < effectiveMaxActiveReaders
        && !pendingGroups.isEmpty()) {
      SourceGroup group = pendingGroups.removeFirst();
      IteratorSlot<?> slot = group.pollPending();
      if (slot == null) {
        continue;
      }
      activate(slot, submitted, submittedSet);
      if (group.hasPending()) {
        pendingGroups.addLast(group);
      }
    }
  }

  /** Caller holds {@link #lock}. */
  private void activate(
      IteratorSlot<?> slot,
      List<TrackedFutureTask<?>> submitted,
      Set<TrackedFutureTask<?>> submittedSet) {
    slot.state = IteratorState.ACTIVE;
    activeReaders++;
    if (slot.leadingTask != null && !submittedSet.contains(slot.leadingTask)) {
      submit(slot.leadingTask, submitted, submittedSet);
    }
    submit(slot.iterator.initialRead(), submitted, submittedSet);
  }

  private void release(IteratorSlot<?> slot) {
    List<TrackedFutureTask<?>> submitted = new ArrayList<>();
    Throwable failure = null;
    synchronized (lock) {
      if (slot.state == IteratorState.RELEASED) {
        return;
      }
      boolean wasActive = slot.state == IteratorState.ACTIVE;
      if (wasActive) {
        activeReaders--;
      }
      slot.state = IteratorState.RELEASED;
      if (slot.leadingTask != null && !slot.leadingTask.isDone()) {
        slot.leadingTask.cancelTracked();
      }
      if (state == State.LAUNCHED) {
        try {
          if (wasActive && slot.group.hasPending()) {
            pendingGroups.remove(slot.group);
            IteratorSlot<?> sameGroup = slot.group.pollPending();
            if (sameGroup != null) {
              activate(sameGroup, submitted, submittedTasks);
            }
            if (slot.group.hasPending()) {
              pendingGroups.addLast(slot.group);
            }
          }
          fillReaderWindow(submitted, submittedTasks);
        } catch (RuntimeException | Error submitFailure) {
          failure = submitFailure;
        }
      }
    }
    if (failure != null) {
      for (TrackedFutureTask<?> task : submitted) {
        task.cancelTracked();
      }
      closeAfterFailure(failure);
      throwFailure(failure);
    }
  }

  private TrackedFutureTask<?> registeredTask(Future<?> future) {
    if (future == null) {
      return null;
    }
    if (!(future instanceof RegisteredFuture)) {
      throw new IllegalArgumentException("Leading task belongs to another I/O mechanism");
    }
    RegisteredFuture<?> registered = (RegisteredFuture<?>) future;
    if (registered.owner != this) {
      throw new IllegalArgumentException("Leading task belongs to another I/O batch");
    }
    return registered.delegate;
  }

  private void ensureRegistering() {
    if (state != State.REGISTERING) {
      throw new IllegalStateException("Prepared I/O batch is no longer registering");
    }
  }

  private void ensureLaunched() {
    synchronized (lock) {
      if (state == State.REGISTERING) {
        throw new IllegalStateException("Prepared I/O batch has not been launched");
      }
      if (state == State.CLOSED) {
        throw new IllegalStateException("Prepared I/O batch is closed");
      }
    }
  }

  private List<PreparedIterator<?>> flattenIteratorGroups() {
    List<PreparedIterator<?>> iterators = new ArrayList<>();
    for (SourceGroup group : iteratorGroups) {
      for (IteratorSlot<?> slot : group.slots) {
        iterators.add(slot.iterator);
      }
    }
    return iterators;
  }

  private void closeAfterFailure(Throwable failure) {
    try {
      close();
    } catch (Throwable closeFailure) {
      if (failure != closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private static void throwFailure(Throwable failure) {
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    throw (Error) failure;
  }

  private static void throwCloseFailure(Throwable failure) throws IOException {
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure != null) {
      throw new IOException("Failed to close prepared I/O", failure);
    }
  }

  private static final class RegisteredFuture<T> implements Future<T> {
    private final PreparedIoBatch owner;
    private final TrackedFutureTask<T> delegate;

    private RegisteredFuture(PreparedIoBatch owner, TrackedFutureTask<T> delegate) {
      this.owner = owner;
      this.delegate = delegate;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      owner.ensureLaunched();
      return delegate.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      owner.ensureLaunched();
      return delegate.get(timeout, unit);
    }
  }

  private enum IteratorState {
    PENDING,
    ACTIVE,
    RELEASED
  }

  private static final class SourceGroup {
    private final List<IteratorSlot<?>> slots = new ArrayList<>();
    private final Deque<IteratorSlot<?>> pending = new ArrayDeque<>();

    private void add(IteratorSlot<?> slot) {
      slots.add(slot);
      pending.addLast(slot);
    }

    private IteratorSlot<?> pollPending() {
      while (!pending.isEmpty()) {
        IteratorSlot<?> slot = pending.removeFirst();
        if (slot.state == IteratorState.PENDING) {
          return slot;
        }
      }
      return null;
    }

    private boolean hasPending() {
      while (!pending.isEmpty() && pending.peekFirst().state != IteratorState.PENDING) {
        pending.removeFirst();
      }
      return !pending.isEmpty();
    }
  }

  private static final class IteratorSlot<T> {
    private final PreparedIterator<T> iterator;
    private final TrackedFutureTask<?> leadingTask;
    private final SourceGroup group;
    private IteratorState state = IteratorState.PENDING;

    private IteratorSlot(
        PreparedIoBatch owner,
        IteratorPreparer<T> preparer,
        TrackedFutureTask<?> leadingTask,
        SourceGroup group) {
      this.iterator = new PreparedIterator<>(owner, preparer, this);
      this.leadingTask = leadingTask;
      this.group = group;
    }
  }

  /** Opens a reader asynchronously and keeps one read buffered ahead of the consumer. */
  private static final class PreparedIterator<T> implements CloseableIterator<T> {
    private final Object lock = new Object();
    private final PreparedIoBatch owner;
    private final IteratorPreparer<T> preparer;
    private final IteratorSlot<T> slot;
    private final TrackedFutureTask<ReadResult<T>> initialRead;
    private CloseableIterator<T> delegate;
    private TrackedFutureTask<ReadResult<T>> readAhead;
    private ReadResult<T> buffered;
    private Throwable asynchronousCloseFailure;
    private boolean exhausted;
    private volatile boolean closed;

    private PreparedIterator(
        PreparedIoBatch owner, IteratorPreparer<T> preparer, IteratorSlot<T> slot) {
      this.owner = owner;
      this.preparer = preparer;
      this.slot = slot;
      this.initialRead = owner.trackedTask(this::readOne);
      this.readAhead = initialRead;
    }

    private TrackedFutureTask<ReadResult<T>> initialRead() {
      return initialRead;
    }

    @Override
    public boolean hasNext() {
      owner.ensureLaunched();
      if (exhausted) {
        return false;
      }
      ensureOpen();
      try {
        if (buffered == null && !exhausted) {
          buffered = awaitRead();
          synchronized (lock) {
            readAhead = null;
          }
          exhausted = !buffered.isAvailable();
          if (exhausted) {
            finish();
          }
        }
        return !exhausted;
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public T next() {
      try {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        T value = buffered.getValue();
        buffered = null;
        submitRead();
        return value;
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public void close() throws IOException {
      CloseState<T> closeState = beginClose();
      Throwable failure = closeState.closeReader(null);
      closeState.awaitExit();
      failure = closeState.addAsynchronousFailure(failure);
      if (failure != null) {
        owner.closeAfterFailure(failure);
        throwCloseFailure(failure);
      }
      owner.release(slot);
    }

    private void finish() {
      try {
        close();
      } catch (IOException failure) {
        throw new KernelEngineException("close an exhausted plan file reader", failure);
      }
    }

    private ReadResult<T> readOne() throws IOException {
      CloseableIterator<T> reader = openReader();
      if (!reader.hasNext()) {
        return ReadResult.empty();
      }
      return ReadResult.available(requireNonNull(reader.next(), "reader batch is null"));
    }

    private void submitRead() {
      TrackedFutureTask<ReadResult<T>> task = new TrackedFutureTask<>(this::readOne);
      synchronized (lock) {
        ensureOpen();
        readAhead = task;
        try {
          owner.executor.execute(task);
        } catch (RuntimeException | Error failure) {
          readAhead = null;
          throw failure;
        }
      }
    }

    private CloseableIterator<T> openReader() throws IOException {
      CloseableIterator<T> existing;
      synchronized (lock) {
        existing = delegate;
      }
      if (existing != null) {
        return existing;
      }

      CloseableIterator<T> opened = requireNonNull(preparer.prepare(), "file reader is null");
      synchronized (lock) {
        if (!closed) {
          delegate = opened;
          return opened;
        }
      }
      try {
        opened.close();
      } catch (Throwable closeFailure) {
        synchronized (lock) {
          asynchronousCloseFailure = closeFailure;
        }
      }
      throw new CancellationException("Plan file reader was closed while opening");
    }

    private ReadResult<T> awaitRead() {
      TrackedFutureTask<ReadResult<T>> task;
      synchronized (lock) {
        task = readAhead;
      }
      try {
        return task.get();
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new KernelEngineException("await a plan file read", failure);
      } catch (ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new KernelEngineException("prefetch a plan file", cause);
      } catch (CancellationException failure) {
        throw new KernelEngineException("await a cancelled plan file read", failure);
      }
    }

    private CloseState<T> beginClose() {
      synchronized (lock) {
        if (closed) {
          return CloseState.empty(this);
        }
        closed = true;
        TrackedFutureTask<ReadResult<T>> task = readAhead;
        if (task != null) {
          task.cancelTracked();
        }
        return new CloseState<>(this, task, delegate);
      }
    }

    private void closeAfterFailure(Throwable failure) {
      owner.closeAfterFailure(failure);
    }

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException("Plan file reader is closed");
      }
    }

    private static final class CloseState<T> {
      private final PreparedIterator<T> owner;
      private final TrackedFutureTask<ReadResult<T>> task;
      private final CloseableIterator<T> reader;

      private CloseState(
          PreparedIterator<T> owner,
          TrackedFutureTask<ReadResult<T>> task,
          CloseableIterator<T> reader) {
        this.owner = owner;
        this.task = task;
        this.reader = reader;
      }

      private static <T> CloseState<T> empty(PreparedIterator<T> owner) {
        return new CloseState<>(owner, null, null);
      }

      private Throwable closeReader(Throwable failure) {
        if (reader != null) {
          try {
            reader.close();
          } catch (Throwable closeFailure) {
            failure = addFailure(failure, closeFailure);
          }
        }
        return failure;
      }

      private void awaitExit() {
        if (task != null) {
          task.awaitExit();
        }
      }

      private Throwable addAsynchronousFailure(Throwable failure) {
        synchronized (owner.lock) {
          if (owner.asynchronousCloseFailure != null) {
            failure = addFailure(failure, owner.asynchronousCloseFailure);
            owner.asynchronousCloseFailure = null;
          }
          owner.readAhead = null;
          owner.buffered = null;
          owner.delegate = null;
        }
        return failure;
      }

      private static Throwable addFailure(Throwable failure, Throwable added) {
        if (failure == null) {
          return added;
        }
        if (failure != added) {
          failure.addSuppressed(added);
        }
        return failure;
      }
    }
  }

  private static final class ReadResult<T> {
    private final T value;

    private ReadResult(T value) {
      this.value = value;
    }

    private static <T> ReadResult<T> empty() {
      return new ReadResult<>(null);
    }

    private static <T> ReadResult<T> available(T value) {
      return new ReadResult<>(value);
    }

    private boolean isAvailable() {
      return value != null;
    }

    private T getValue() {
      return value;
    }
  }

  /** Future whose exit latch distinguishes queued cancellation from a running task unwinding. */
  private static final class TrackedFutureTask<T> extends FutureTask<T> {
    private final CountDownLatch exited = new CountDownLatch(1);
    private boolean entered;
    private boolean cancelledBeforeRun;

    private TrackedFutureTask(Callable<T> callable) {
      super(callable);
    }

    @Override
    public void run() {
      synchronized (this) {
        if (cancelledBeforeRun) {
          return;
        }
        entered = true;
      }
      try {
        super.run();
      } finally {
        exited.countDown();
      }
    }

    private void cancelTracked() {
      boolean queued;
      synchronized (this) {
        queued = !entered;
        if (queued) {
          cancelledBeforeRun = true;
        }
        cancel(true);
      }
      if (queued) {
        exited.countDown();
      }
    }

    private void awaitExit() {
      boolean interrupted = false;
      while (true) {
        try {
          exited.await();
          break;
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
