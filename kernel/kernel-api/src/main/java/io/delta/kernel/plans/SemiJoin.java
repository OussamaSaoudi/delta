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
package io.delta.kernel.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Filters probe rows by null-safe membership in the build keys. */
public final class SemiJoin extends PlanNode {
  private final PlanNode probe;
  private final PlanNode build;
  private final List<Expression> probeKeys;
  private final List<Expression> buildKeys;
  private final List<DataType> keyTypes;
  private final boolean inverted;
  private final List<PlanNode> children;

  public SemiJoin(
      PlanNode probe,
      PlanNode build,
      List<Expression> probeKeys,
      List<Expression> buildKeys,
      List<DataType> keyTypes,
      boolean inverted) {
    this.probe = requireNonNull(probe, "probe is null");
    this.build = requireNonNull(build, "build is null");
    this.probeKeys = PlanValidation.immutableCopy(probeKeys, "probe key is null");
    this.buildKeys = PlanValidation.immutableCopy(buildKeys, "build key is null");
    this.keyTypes = PlanValidation.immutableCopy(keyTypes, "key type is null");
    this.inverted = inverted;
    this.children = Collections.unmodifiableList(Arrays.asList(probe, build));

    if (this.probeKeys.size() != this.buildKeys.size()
        || this.probeKeys.size() != this.keyTypes.size()) {
      throw new IllegalArgumentException(
          "SemiJoin probe keys, build keys, and key types must have equal arity");
    }
    for (int index = 0; index < this.keyTypes.size(); index++) {
      PlanValidation.validateExpressionReferences(
          probe.outputSchema(), this.probeKeys.get(index), "SemiJoin probe key");
      PlanValidation.validateExpressionReferences(
          build.outputSchema(), this.buildKeys.get(index), "SemiJoin build key");
    }
  }

  public PlanNode probe() {
    return probe;
  }

  public PlanNode build() {
    return build;
  }

  public List<Expression> probeKeys() {
    return probeKeys;
  }

  public List<Expression> buildKeys() {
    return buildKeys;
  }

  public List<DataType> keyTypes() {
    return keyTypes;
  }

  public boolean inverted() {
    return inverted;
  }

  @Override
  public StructType outputSchema() {
    return probe.outputSchema();
  }

  @Override
  public List<PlanNode> children() {
    return children;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemiJoin)) {
      return false;
    }
    SemiJoin that = (SemiJoin) other;
    return inverted == that.inverted
        && probe.equals(that.probe)
        && build.equals(that.build)
        && probeKeys.equals(that.probeKeys)
        && buildKeys.equals(that.buildKeys)
        && keyTypes.equals(that.keyTypes);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(Objects.hash(probe, build, probeKeys, buildKeys, keyTypes, inverted));
  }
}
