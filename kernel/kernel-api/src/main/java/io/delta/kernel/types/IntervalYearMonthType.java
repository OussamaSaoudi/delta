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
package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;

/**
 * The data type representing ANSI year-month intervals as a signed count of months.
 *
 * @since 4.1.0
 */
@Evolving
public final class IntervalYearMonthType extends BasePrimitiveType {
  public static final IntervalYearMonthType INTERVAL_YEAR_MONTH = new IntervalYearMonthType();

  private IntervalYearMonthType() {
    super("interval year to month");
  }
}
