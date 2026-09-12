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
import io.delta.kernel.execution.PlanEngine;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Boxed execution state with zero-copy lookup and copy-on-insert keys. */
final class StateTable {
  private static final Row[] NO_VALUES = new Row[0];

  private final StructType keySchema;
  private final PlanEngine engine;
  private final int valueSlotCount;
  private final List<Entry> entries = new ArrayList<>();
  private final Map<Entry, Entry> keyedEntries;
  private final Probe probe = new Probe();

  /** Creates initially empty keyed state, including an empty-key join set. */
  static StateTable keyed(PlanEngine engine, StructType keySchema, int valueSlotCount) {
    return new StateTable(engine, keySchema, valueSlotCount, false);
  }

  /** Creates the single entry required by a global aggregate, even for empty input. */
  static StateTable global(PlanEngine engine, int valueSlotCount) {
    return new StateTable(engine, new StructType(), valueSlotCount, true);
  }

  private StateTable(PlanEngine engine, StructType keySchema, int valueSlotCount, boolean global) {
    this.engine = requireNonNull(engine, "engine is null");
    this.keySchema = requireNonNull(keySchema, "keySchema is null");
    if (valueSlotCount < 0) {
      throw new IllegalArgumentException("valueSlotCount is negative: " + valueSlotCount);
    }
    this.valueSlotCount = valueSlotCount;
    this.keyedEntries = keySchema.length() == 0 ? null : new HashMap<>();
    if (global) {
      entries.add(new Entry(new GenericRow(keySchema, Collections.emptyMap()), 0));
    }
  }

  int probeOrInsert(Row candidate) {
    checkCandidate(candidate);
    if (keyedEntries == null) {
      if (entries.isEmpty()) {
        entries.add(new Entry(new GenericRow(keySchema, Collections.emptyMap()), 0));
      }
      return 0;
    }

    probe.reset(candidate);
    Entry existing = keyedEntries.get(probe);
    if (existing != null) {
      return existing.entryRef;
    }

    Row ownedKey = engine.retainRow(candidate, keySchema);
    int entryRef = entries.size();
    Entry entry = new Entry(ownedKey, probe.hashCode());
    entries.add(entry);
    keyedEntries.put(entry, entry);
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

  Row getValue(int entryRef, int slot) {
    return entry(entryRef).values[checkSlot(slot)];
  }

  void setValue(int entryRef, int slot, Row ownedValue) {
    entry(entryRef).values[checkSlot(slot)] = ownedValue;
  }

  int size() {
    return entries.size();
  }

  private Entry entry(int entryRef) {
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
    if (keySchema.length() == 0) {
      return;
    }
    requireNonNull(candidate, "candidate is null");
    if (candidate.getSchema().length() < keySchema.length()) {
      throw new IllegalArgumentException("Candidate is narrower than the state key");
    }
  }

  private final class Entry {
    private final Row key;
    private final int hashCode;
    private final int entryRef;
    private final Row[] values = valueSlotCount == 0 ? NO_VALUES : new Row[valueSlotCount];

    private Entry(Row key, int hashCode) {
      this.key = key;
      this.hashCode = hashCode;
      this.entryRef = entries.size();
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof StateTable.Entry
          && RowKernels.equalValues(key, keySchema, ((Entry) other).key);
    }
  }

  private final class Probe {
    private Row row;
    private int hashCode;

    private void reset(Row row) {
      this.row = row;
      this.hashCode = RowKernels.hashValues(row, keySchema);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof StateTable.Entry
          && RowKernels.equalValues(row, keySchema, ((Entry) other).key);
    }
  }
}
