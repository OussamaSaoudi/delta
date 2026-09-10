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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Boxed execution state with zero-copy lookup and copy-on-insert keys. */
final class StateTable implements AutoCloseable {
  private final int valueSlotCount;
  private final List<Entry> entries = new ArrayList<>();
  private final Map<Key, Key> keyedEntries;
  private final Probe probe = new Probe();
  private boolean closed;

  /** Creates initially empty keyed state, including an empty-key join set. */
  static StateTable keyed(StructType keySchema, int valueSlotCount) {
    return new StateTable(keySchema, valueSlotCount, false);
  }

  /** Creates the single entry required by a global aggregate, even for empty input. */
  static StateTable global(int valueSlotCount) {
    return new StateTable(new StructType(), valueSlotCount, true);
  }

  private StateTable(StructType keySchema, int valueSlotCount, boolean global) {
    requireNonNull(keySchema, "keySchema is null");
    if (valueSlotCount < 0) {
      throw new IllegalArgumentException("valueSlotCount is negative: " + valueSlotCount);
    }
    this.valueSlotCount = valueSlotCount;
    this.keyedEntries = keySchema.length() == 0 ? null : new HashMap<>();
    if (global) {
      entries.add(new Entry(GenericRow.fromOwnedValues(keySchema, new Object[0])));
    }
  }

  int probeOrInsert(Row candidate) {
    checkCandidate(candidate);
    if (keyedEntries == null) {
      if (entries.isEmpty()) {
        entries.add(new Entry(RowKernels.materialize(candidate)));
      }
      return 0;
    }

    probe.reset(candidate);
    Key existing = keyedEntries.get(probe);
    if (existing != null) {
      return existing.entryRef;
    }

    Row ownedKey = RowKernels.materialize(candidate);
    int entryRef = entries.size();
    entries.add(new Entry(ownedKey));
    Key key = new Key(ownedKey, probe.hashCode(), entryRef);
    keyedEntries.put(key, key);
    return entryRef;
  }

  boolean contains(Row candidate) {
    checkCandidate(candidate);
    if (keyedEntries == null) {
      return !entries.isEmpty();
    }
    probe.reset(candidate);
    return keyedEntries.containsKey(probe);
  }

  Row getKey(int entryRef) {
    return entry(entryRef).key;
  }

  Object getValue(int entryRef, int slot) {
    return entry(entryRef).values[checkSlot(slot)];
  }

  void setValue(int entryRef, int slot, Object ownedValue) {
    entry(entryRef).values[checkSlot(slot)] = ownedValue;
  }

  int size() {
    checkOpen();
    return entries.size();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      entries.clear();
      if (keyedEntries != null) {
        keyedEntries.clear();
      }
      probe.clear();
    }
  }

  private Entry entry(int entryRef) {
    checkOpen();
    if (entryRef < 0 || entryRef >= entries.size()) {
      throw new IndexOutOfBoundsException("Invalid entry reference: " + entryRef);
    }
    return entries.get(entryRef);
  }

  private int checkSlot(int slot) {
    if (slot < 0 || slot >= valueSlotCount) {
      throw new IndexOutOfBoundsException("Invalid value slot: " + slot);
    }
    return slot;
  }

  private void checkCandidate(Row candidate) {
    checkOpen();
    requireNonNull(candidate, "candidate is null");
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("State table is closed");
    }
  }

  private final class Entry {
    private final Row key;
    private final Object[] values = new Object[valueSlotCount];

    private Entry(Row key) {
      this.key = key;
    }
  }

  private static final class Key {
    private final Row row;
    private final int hashCode;
    private final int entryRef;

    private Key(Row row, int hashCode, int entryRef) {
      this.row = row;
      this.hashCode = hashCode;
      this.entryRef = entryRef;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && RowKernels.equalValues(row, ((Key) other).row);
    }
  }

  private final class Probe {
    private Row row;
    private int hashCode;

    private void reset(Row row) {
      this.row = row;
      this.hashCode = RowKernels.hashValues(row);
    }

    private void clear() {
      row = null;
      hashCode = 0;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && RowKernels.equalValues(row, ((Key) other).row);
    }
  }
}
