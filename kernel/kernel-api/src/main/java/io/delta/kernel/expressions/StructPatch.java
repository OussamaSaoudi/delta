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
package io.delta.kernel.expressions;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Applies sparse, ordered edits to the fields of an input struct.
 *
 * <p>Fields not named in {@code fieldTransforms} pass through unchanged. Prepended expressions are
 * emitted first, then input fields are visited in schema order, and appended expressions are
 * emitted last. A field transform may retain its input field and insert expressions after it, or
 * replace the input field with zero or more expressions. Replacing with no expressions drops the
 * field.
 *
 * <p>The optional input path selects a nested input struct. Its null rows remain null in the
 * result. The caller supplies the result field names, types, and nullability through the {@code
 * StructType} passed to the expression evaluator.
 *
 * @since 4.4.0
 */
@Evolving
public final class StructPatch implements Expression {
  /** An edit anchored at one named input field. */
  public static final class FieldTransform {
    private final List<Expression> expressions;
    private final boolean replace;
    private final boolean optional;

    /**
     * @param expressions expressions emitted at or after this input field's position
     * @param replace whether to omit the original input field before emitting {@code expressions}
     * @param optional whether to ignore this entire transform when the input field is absent
     */
    public FieldTransform(List<Expression> expressions, boolean replace, boolean optional) {
      this.expressions = immutableExpressions(expressions, "expressions");
      this.replace = replace;
      this.optional = optional;
    }

    public List<Expression> getExpressions() {
      return expressions;
    }

    public boolean isReplace() {
      return replace;
    }

    public boolean isOptional() {
      return optional;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof FieldTransform)) {
        return false;
      }
      FieldTransform that = (FieldTransform) other;
      return replace == that.replace
          && optional == that.optional
          && expressions.equals(that.expressions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(expressions, replace, optional);
    }
  }

  private final Optional<Column> inputPath;
  private final Map<String, FieldTransform> fieldTransforms;
  private final List<Expression> prependedFields;
  private final List<Expression> appendedFields;
  private final List<Expression> children;

  public StructPatch(
      Optional<Column> inputPath,
      Map<String, FieldTransform> fieldTransforms,
      List<Expression> prependedFields,
      List<Expression> appendedFields) {
    this.inputPath = requireNonNull(inputPath, "inputPath is null");
    requireNonNull(fieldTransforms, "fieldTransforms is null");
    Map<String, FieldTransform> fields = new LinkedHashMap<>();
    fieldTransforms.forEach(
        (name, transform) ->
            fields.put(
                requireNonNull(name, "field transform name is null"),
                requireNonNull(transform, "field transform is null")));
    this.fieldTransforms = Collections.unmodifiableMap(fields);
    this.prependedFields = immutableExpressions(prependedFields, "prependedFields");
    this.appendedFields = immutableExpressions(appendedFields, "appendedFields");

    List<Expression> allChildren = new ArrayList<>(this.prependedFields);
    fields.values().forEach(transform -> allChildren.addAll(transform.getExpressions()));
    allChildren.addAll(this.appendedFields);
    this.children = Collections.unmodifiableList(allChildren);
  }

  public Optional<Column> getInputPath() {
    return inputPath;
  }

  public Map<String, FieldTransform> getFieldTransforms() {
    return fieldTransforms;
  }

  public List<Expression> getPrependedFields() {
    return prependedFields;
  }

  public List<Expression> getAppendedFields() {
    return appendedFields;
  }

  @Override
  public List<Expression> getChildren() {
    return children;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StructPatch)) {
      return false;
    }
    StructPatch that = (StructPatch) other;
    return inputPath.equals(that.inputPath)
        && fieldTransforms.equals(that.fieldTransforms)
        && prependedFields.equals(that.prependedFields)
        && appendedFields.equals(that.appendedFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        StructPatch.class, inputPath, fieldTransforms, prependedFields, appendedFields);
  }

  private static List<Expression> immutableExpressions(List<Expression> expressions, String name) {
    requireNonNull(expressions, name + " is null");
    List<Expression> copy = new ArrayList<>(expressions.size());
    for (int ordinal = 0; ordinal < expressions.size(); ordinal++) {
      copy.add(
          requireNonNull(
              expressions.get(ordinal),
              String.format("%s expression at ordinal %d is null", name, ordinal)));
    }
    return Collections.unmodifiableList(copy);
  }
}
