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

import io.delta.kernel.types.StructType;
import java.util.List;

/** Reads Parquet files into rows matching a declared output schema. */
public final class ScanParquet extends FileScan {
  public ScanParquet(List<ScanFile> files, List<String> fileConstantColumns, StructType schema) {
    super(files, fileConstantColumns, schema);
  }

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
