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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.Aggregate;
import io.delta.kernel.plans.DynamicScan;
import io.delta.kernel.plans.Filter;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.plans.Project;
import io.delta.kernel.plans.ScanJson;
import io.delta.kernel.plans.ScanParquet;
import io.delta.kernel.plans.SemiJoin;
import io.delta.kernel.plans.UnionAll;
import io.delta.kernel.plans.Values;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/** Internal type dispatch for the closed plan-node set. */
final class OperatorDispatcher {
  private static final int OUTPUT_BATCH_SIZE = 1024;

  private OperatorDispatcher() {}

  static CloseableIterator<FilteredColumnarBatch> open(PlanNode node, ExecutionContext context) {
    if (node instanceof Values) {
      return openValues((Values) node);
    }
    if (node instanceof Filter) {
      return openFilter((Filter) node, context);
    }
    if (node instanceof Project) {
      return openProject((Project) node, context);
    }
    if (node instanceof UnionAll) {
      return openUnion((UnionAll) node, context);
    }
    if (node instanceof ScanJson) {
      return ScanOperator.open((ScanJson) node, context.engine());
    }
    if (node instanceof ScanParquet) {
      return ScanOperator.open((ScanParquet) node, context.engine());
    }
    if (node instanceof DynamicScan) {
      return DynamicScanOperator.open((DynamicScan) node, context);
    }
    if (node instanceof Aggregate) {
      return AggregateOperator.open((Aggregate) node, context);
    }
    if (node instanceof SemiJoin) {
      return SemiJoinOperator.open((SemiJoin) node, context);
    }
    throw new UnsupportedOperationException("Unsupported plan node: " + node.getClass().getName());
  }

  private static CloseableIterator<FilteredColumnarBatch> openValues(Values node) {
    return new CloseableIterator<FilteredColumnarBatch>() {
      private int offset;
      private boolean closed;

      @Override
      public boolean hasNext() {
        requireOpen();
        return offset < node.ownedRows().size();
      }

      @Override
      public FilteredColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        int end = Math.min(offset + OUTPUT_BATCH_SIZE, node.ownedRows().size());
        FilteredColumnarBatch result =
            new FilteredColumnarBatch(
                new RowBackedColumnarBatch(
                    node.outputSchema(), node.ownedRows().subList(offset, end)),
                Optional.empty(),
                FilteredColumnarBatch.Lifetime.OWNED);
        offset = end;
        return result;
      }

      @Override
      public void close() {
        closed = true;
      }

      private void requireOpen() {
        if (closed) {
          throw new IllegalStateException("Values iterator is closed");
        }
      }
    };
  }

  private static CloseableIterator<FilteredColumnarBatch> openFilter(
      Filter node, ExecutionContext context) {
    PredicateEvaluator evaluator =
        context.expressions().getPredicateEvaluator(node.input().outputSchema(), node.predicate());
    try {
      return map(context.open(node.input()), evaluator::eval, evaluator);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, evaluator);
      throw failure;
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> openProject(
      Project node, ExecutionContext context) {
    ExpressionEvaluator evaluator =
        context
            .expressions()
            .getEvaluator(node.input().outputSchema(), node.rowExpression(), node.outputSchema());
    try {
      return map(context.open(node.input()), evaluator::eval, evaluator);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, evaluator);
      throw failure;
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> openUnion(
      UnionAll node, ExecutionContext context) {
    List<CloseableIterator<FilteredColumnarBatch>> inputs = new ArrayList<>(node.inputs().size());
    try {
      for (PlanNode input : node.inputs()) {
        inputs.add(context.open(input));
      }
      return new ConcatenatingIterator(inputs);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, inputs.toArray(new AutoCloseable[0]));
      throw failure;
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> map(
      CloseableIterator<FilteredColumnarBatch> input,
      Function<FilteredColumnarBatch, FilteredColumnarBatch> transform,
      AutoCloseable evaluator) {
    return new CloseableIterator<FilteredColumnarBatch>() {
      private boolean closed;

      @Override
      public boolean hasNext() {
        requireOpen();
        boolean hasNext = input.hasNext();
        if (!hasNext) {
          close();
        }
        return hasNext;
      }

      @Override
      public FilteredColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return transform.apply(input.next());
      }

      @Override
      public void close() {
        if (closed) {
          return;
        }
        closed = true;
        Utils.closeCloseables(input, evaluator);
      }

      private void requireOpen() {
        if (closed) {
          throw new IllegalStateException("Operator iterator is closed");
        }
      }
    };
  }

  private static final class ConcatenatingIterator
      implements CloseableIterator<FilteredColumnarBatch> {
    private final List<CloseableIterator<FilteredColumnarBatch>> inputs;
    private int index;
    private boolean closed;

    private ConcatenatingIterator(List<CloseableIterator<FilteredColumnarBatch>> inputs) {
      this.inputs = inputs;
    }

    @Override
    public boolean hasNext() {
      requireOpen();
      while (index < inputs.size()) {
        CloseableIterator<FilteredColumnarBatch> input = inputs.get(index);
        if (input != null && input.hasNext()) {
          return true;
        }
        Utils.closeCloseables(input);
        inputs.set(index++, null);
      }
      close();
      return false;
    }

    @Override
    public FilteredColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return inputs.get(index).next();
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      Utils.closeCloseables(inputs.toArray(new AutoCloseable[0]));
      inputs.clear();
    }

    private void requireOpen() {
      if (closed) {
        throw new IllegalStateException("Union iterator is closed");
      }
    }
  }
}
