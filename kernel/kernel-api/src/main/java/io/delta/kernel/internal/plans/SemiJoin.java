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

import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.List;

/** Filters probe rows by the presence or absence of matching build keys. */
public final class SemiJoin implements Operator {
  private final boolean inverted;
  private final List<Column> probeKeys;
  private final List<Column> buildKeys;

  public SemiJoin(boolean inverted, List<Column> probeKeys, List<Column> buildKeys) {
    this.probeKeys = PlanValidation.immutableCopy(probeKeys, "probe key is null");
    this.buildKeys = PlanValidation.immutableCopy(buildKeys, "build key is null");
    if (this.probeKeys.isEmpty()) {
      throw new IllegalArgumentException("SemiJoin requires at least one key");
    }
    if (this.probeKeys.size() != this.buildKeys.size()) {
      throw new IllegalArgumentException(
          "SemiJoin probe and build key counts differ: "
              + this.probeKeys.size()
              + " != "
              + this.buildKeys.size());
    }
    this.inverted = inverted;
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
    if (inputSchemas == null || inputSchemas.size() != 2) {
      throw new IllegalArgumentException(
          "SemiJoin requires probe and build inputs, got "
              + (inputSchemas == null ? "null" : inputSchemas.size()));
    }
    StructType probeSchema = inputSchemas.get(0);
    StructType buildSchema = inputSchemas.get(1);
    for (int index = 0; index < probeKeys.size(); index++) {
      DataType probeType =
          PlanValidation.resolveField(probeSchema, probeKeys.get(index), "SemiJoin probe key")
              .getDataType();
      DataType buildType =
          PlanValidation.resolveField(buildSchema, buildKeys.get(index), "SemiJoin build key")
              .getDataType();
      if (!probeType.equals(buildType)) {
        throw new IllegalArgumentException(
            "SemiJoin key "
                + index
                + " has incompatible types "
                + probeType
                + " and "
                + buildType);
      }
    }
    return probeSchema;
  }

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
