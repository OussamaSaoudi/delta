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
package io.delta.kernel.defaults.internal.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.Optional;

/** Internal batch entry point for permissive ParseJson decoding. */
public final class DefaultJsonBatchParser {
  private DefaultJsonBatchParser() {}

  public static ColumnVector parse(ColumnVector input, StructType schema) throws IOException {
    return StrictJsonRowParser.parsePermissiveBatch(input, schema);
  }

  /** Decode strict JsonHandler input directly into column vectors. */
  public static ColumnarBatch parseStrict(
      ColumnVector input, StructType schema, Optional<ColumnVector> selectionVector)
      throws IOException {
    return StrictJsonRowParser.parseStrictBatch(input, schema, selectionVector);
  }
}
