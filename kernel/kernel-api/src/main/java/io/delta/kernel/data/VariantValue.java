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
package io.delta.kernel.data;

import io.delta.kernel.annotation.Evolving;
import java.util.Arrays;
import java.util.Objects;

/**
 * One Variant value in its protocol-defined binary representation.
 *
 * <p>The value and metadata buffers follow the Parquet Variant encoding used by Delta. Instances
 * are immutable: the constructor and accessors defensively copy both buffers.
 *
 * <p>Kernel Java currently exposes Variant values for reads and expression evaluation. Physical
 * Variant writes remain unsupported, and {@code Literal} cannot carry Variant values because the
 * plan protocol has no Variant scalar literal.
 *
 * @since 4.1.0
 */
@Evolving
public final class VariantValue {
  private final byte[] value;
  private final byte[] metadata;

  public VariantValue(byte[] value, byte[] metadata) {
    this.value = Objects.requireNonNull(value, "value is null").clone();
    this.metadata = Objects.requireNonNull(metadata, "metadata is null").clone();
  }

  /** Returns a copy of the binary-encoded Variant value. */
  public byte[] getValue() {
    return value.clone();
  }

  /** Returns a copy of the binary-encoded Variant metadata. */
  public byte[] getMetadata() {
    return metadata.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof VariantValue)) {
      return false;
    }
    VariantValue that = (VariantValue) other;
    return Arrays.equals(value, that.value) && Arrays.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(value) + Arrays.hashCode(metadata);
  }

  @Override
  public String toString() {
    return "VariantValue(value="
        + Arrays.toString(value)
        + ", metadata="
        + Arrays.toString(metadata)
        + ")";
  }
}
