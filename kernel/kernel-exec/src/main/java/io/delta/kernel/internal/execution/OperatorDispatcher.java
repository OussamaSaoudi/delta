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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.execution.PlanEngine;
import io.delta.kernel.execution.PlanEngine.BatchEvaluator;
import io.delta.kernel.execution.PlanResultCache;
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.plans.PlanNode;
import io.delta.kernel.plans.PlanNode.Aggregate;
import io.delta.kernel.plans.PlanNode.Cte;
import io.delta.kernel.plans.PlanNode.FileScan;
import io.delta.kernel.plans.PlanNode.Filter;
import io.delta.kernel.plans.PlanNode.Project;
import io.delta.kernel.plans.PlanNode.SemiJoin;
import io.delta.kernel.plans.PlanNode.UnionAll;
import io.delta.kernel.plans.PlanNode.Values;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Recursive type dispatch for one plan execution. */
public final class OperatorDispatcher {
  private static final int OUTPUT_BATCH_SIZE = 1024;

  private final PlanEngine engine;
  private final PlanResultCache cache;
  private final Map<Long, CloseableIterable<ColumnarBatch>> ctes = new LinkedHashMap<>();

  private OperatorDispatcher(PlanEngine engine, PlanResultCache cache) {
    this.engine = requireNonNull(engine, "engine is null");
    this.cache = requireNonNull(cache, "cache is null");
  }

  public static CloseableIterator<ColumnarBatch> execute(
      PlanNode root, PlanEngine engine, PlanResultCache cache) {
    return new OperatorDispatcher(engine, cache).open(root);
  }

  PlanEngine engine() {
    return engine;
  }

  CloseableIterator<ColumnarBatch> open(PlanNode node) {
    if (node instanceof Values) {
      return openValues((Values) node);
    }
    if (node instanceof Filter) {
      return openFilter((Filter) node);
    }
    if (node instanceof Project) {
      return openProject((Project) node);
    }
    if (node instanceof UnionAll) {
      return openUnion((UnionAll) node);
    }
    if (node instanceof FileScan) {
      return openScan((FileScan) node);
    }
    if (node instanceof Aggregate) {
      return AggregateOperator.open((Aggregate) node, this);
    }
    if (node instanceof SemiJoin) {
      return SemiJoinOperator.open((SemiJoin) node, this);
    }
    if (node instanceof Cte) {
      return openCte((Cte) node);
    }
    throw new UnsupportedOperationException("Unsupported plan node: " + node.getClass().getName());
  }

  private CloseableIterator<ColumnarBatch> openScan(FileScan scan) {
    CloseableIterator<ColumnarBatch> prefetched = cache.get(scan);
    return prefetched != null
        ? prefetched
        : requireNonNull(engine.scan(scan), "Scan iterator is null");
  }

  private CloseableIterator<ColumnarBatch> openCte(Cte cte) {
    CloseableIterable<ColumnarBatch> result =
        ctes.computeIfAbsent(
            cte.id(),
            ignored ->
                CloseableIterable.inMemoryIterable(
                    ManagedIterator.map(open(cte.input()), this::retain)));
    return result.iterator();
  }

  private ColumnarBatch retain(ColumnarBatch batch) {
    if (batch.getLifetime() == ColumnarBatch.Lifetime.OWNED) {
      return batch;
    }
    List<Row> rows = new ArrayList<>(batch.getSize());
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      rows.add(engine.retainRow(RowKernels.rowAt(batch, rowId), batch.getSchema()));
    }
    return new RowBackedColumnarBatch(batch.getSchema(), rows, ColumnarBatch.Lifetime.OWNED);
  }

  private static CloseableIterator<ColumnarBatch> openValues(Values node) {
    return new ManagedIterator<ColumnarBatch>() {
      private int offset;

      @Override
      protected boolean hasNextOpen() {
        return offset < node.ownedRows().size();
      }

      @Override
      protected ColumnarBatch nextOpen() {
        int end = Math.min(offset + OUTPUT_BATCH_SIZE, node.ownedRows().size());
        ColumnarBatch result =
            new RowBackedColumnarBatch(
                node.outputSchema(),
                node.ownedRows().subList(offset, end),
                ColumnarBatch.Lifetime.OWNED);
        offset = end;
        return result;
      }
    };
  }

  private CloseableIterator<ColumnarBatch> openFilter(Filter node) {
    BatchEvaluator evaluator = engine.bind(node.input().outputSchema(), node.predicate());
    return ManagedIterator.map(open(node.input()), evaluator::eval, evaluator);
  }

  private CloseableIterator<ColumnarBatch> openProject(Project node) {
    BatchEvaluator evaluator =
        engine.bind(node.input().outputSchema(), node.rowExpression(), node.outputSchema());
    return ManagedIterator.map(open(node.input()), evaluator::eval, evaluator);
  }

  private CloseableIterator<ColumnarBatch> openUnion(UnionAll node) {
    List<CloseableIterator<ColumnarBatch>> inputs = new ArrayList<>(node.inputs().size());
    try {
      for (PlanNode input : node.inputs()) {
        inputs.add(open(input));
      }
      return ManagedIterator.concat(inputs);
    } catch (RuntimeException | Error failure) {
      ManagedIterator.closeAndSuppress(failure, inputs.toArray(new AutoCloseable[0]));
      throw failure;
    }
  }
}
