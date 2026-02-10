/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.examples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.metrics.MetricsReport;

/**
 * Engine wrapper that intercepts all {@link MetricsReport} objects emitted by
 * kernel operations. This follows the same pattern as
 * {@code KernelMetricsProfiler.BenchmarkingEngine} from the JMH benchmark suite.
 *
 * <p>Usage:
 * <pre>
 *   MetricsCapturingEngine engine = new MetricsCapturingEngine(DefaultEngine.create(conf));
 *   // ... perform kernel operations using engine ...
 *   List&lt;MetricsReport&gt; reports = engine.getReports();
 *   engine.clearReports();
 * </pre>
 */
public class MetricsCapturingEngine implements Engine {

    private final Engine delegate;
    private final List<MetricsReport> reports = new ArrayList<>();
    private final List<MetricsReporter> reporters =
            Collections.singletonList(report -> reports.add(report));

    public MetricsCapturingEngine(Engine delegate) {
        this.delegate = delegate;
    }

    @Override
    public ExpressionHandler getExpressionHandler() {
        return delegate.getExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler() {
        return delegate.getJsonHandler();
    }

    @Override
    public FileSystemClient getFileSystemClient() {
        return delegate.getFileSystemClient();
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return delegate.getParquetHandler();
    }

    @Override
    public List<MetricsReporter> getMetricsReporters() {
        return reporters;
    }

    /**
     * Get a copy of all captured metrics reports.
     */
    public List<MetricsReport> getReports() {
        return new ArrayList<>(reports);
    }

    /**
     * Clear all captured metrics reports. Call this before each benchmark iteration
     * to get per-iteration metrics.
     */
    public void clearReports() {
        reports.clear();
    }
}
