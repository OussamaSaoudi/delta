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

import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/** Centralizes iterator state, failure handling, and deterministic resource ownership. */
abstract class ManagedIterator<T> implements CloseableIterator<T> {
  private AutoCloseable[] resources;
  private boolean exhausted;

  static <T, R> CloseableIterator<R> map(
      CloseableIterator<T> input, Function<T, R> mapper, AutoCloseable... resources) {
    requireNonNull(input, "input is null");
    requireNonNull(mapper, "mapper is null");
    AutoCloseable[] owned = new AutoCloseable[resources.length + 1];
    System.arraycopy(resources, 0, owned, 0, resources.length);
    owned[resources.length] = input;
    return new ManagedIterator<R>(owned) {
      @Override
      protected boolean hasNextOpen() {
        return input.hasNext();
      }

      @Override
      protected R nextOpen() {
        return mapper.apply(input.next());
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <T> CloseableIterator<T> concat(List<CloseableIterator<T>> inputs) {
    requireNonNull(inputs, "inputs is null");
    CloseableIterator<T>[] delegates = inputs.toArray(new CloseableIterator[0]);
    return new ManagedIterator<T>(delegates) {
      private int current;

      @Override
      protected boolean hasNextOpen() {
        while (current < delegates.length && !delegates[current].hasNext()) {
          Utils.closeCloseables(delegates[current]);
          delegates[current] = null;
          current++;
        }
        return current < delegates.length;
      }

      @Override
      protected T nextOpen() {
        return delegates[current].next();
      }
    };
  }

  /** Closes every resource and suppresses close failures on the original failure. */
  static void closeAndSuppress(Throwable failure, AutoCloseable... resources) {
    try {
      Utils.closeCloseables(resources);
    } catch (RuntimeException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  protected ManagedIterator(AutoCloseable... resources) {
    this.resources = requireNonNull(resources, "resources is null");
    for (AutoCloseable resource : resources) {
      requireNonNull(resource, "resource is null");
    }
  }

  @Override
  public final boolean hasNext() {
    if (exhausted) {
      return false;
    }
    requireOpen();
    try {
      boolean hasNext = hasNextOpen();
      if (!hasNext) {
        exhausted = true;
        close();
      }
      return hasNext;
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure);
      throw failure;
    }
  }

  @Override
  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      return nextOpen();
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure);
      throw failure;
    }
  }

  @Override
  public final void close() {
    if (resources == null) {
      return;
    }
    Utils.closeCloseables(takeResources());
  }

  protected abstract boolean hasNextOpen();

  protected abstract T nextOpen();

  private void closeAfterFailure(Throwable failure) {
    if (resources == null) {
      return;
    }
    closeAndSuppress(failure, takeResources());
  }

  private AutoCloseable[] takeResources() {
    AutoCloseable[] owned = resources;
    resources = null;
    return owned;
  }

  private void requireOpen() {
    if (resources == null) {
      throw new IllegalStateException("Iterator is closed");
    }
  }
}
