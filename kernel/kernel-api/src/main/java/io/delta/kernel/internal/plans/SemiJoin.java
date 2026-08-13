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
package io.delta.kernel.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Filters probe rows by the presence or absence of matching keys on a build input. */
public final class SemiJoin implements Operator {
  private final boolean inverted;
  private final List<Column> probeKeys;
  private final List<Column> buildKeys;

  public SemiJoin(boolean inverted, List<Column> probeKeys, List<Column> buildKeys) {
    requireNonNull(probeKeys, "probeKeys is null");
    requireNonNull(buildKeys, "buildKeys is null");
    if (probeKeys.size() != buildKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "SemiJoin has %s probe key(s), but %s build key(s)",
              probeKeys.size(), buildKeys.size()));
    }
    this.inverted = inverted;
    this.probeKeys = copyKeys(probeKeys, "probe");
    this.buildKeys = copyKeys(buildKeys, "build");
  }

  private static List<Column> copyKeys(List<Column> keys, String side) {
    List<Column> copy = new ArrayList<>(keys.size());
    for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
      copy.add(requireNonNull(keys.get(keyIndex), side + " key is null"));
    }
    return Collections.unmodifiableList(copy);
  }

  public boolean isInverted() {
    return inverted;
  }

  public List<Column> getProbeKeys() {
    return probeKeys;
  }

  public List<Column> getBuildKeys() {
    return buildKeys;
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.size() != 2) {
      throw new IllegalArgumentException(
          "SemiJoin requires probe and build inputs, got " + inputSchemas.size());
    }
    StructType probeSchema = requireNonNull(inputSchemas.get(0), "probe schema is null");
    StructType buildSchema = requireNonNull(inputSchemas.get(1), "build schema is null");
    for (Column probeKey : probeKeys) {
      PlanSchemaUtils.resolveField(probeSchema, probeKey, "SemiJoin probe key");
    }
    for (Column buildKey : buildKeys) {
      PlanSchemaUtils.resolveField(buildSchema, buildKey, "SemiJoin build key");
    }
    return probeSchema;
  }
}
