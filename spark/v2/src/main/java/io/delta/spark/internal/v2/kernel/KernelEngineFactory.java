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

package io.delta.spark.internal.v2.kernel;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.engine.ParquetHandler;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/** Factory for creating the default Kernel {@link Engine} used by the DSv2 connector. */
public final class KernelEngineFactory {

  private KernelEngineFactory() {}

  /** Builds the backend-appropriate default engine. */
  public static Engine createDefaultEngine(Configuration hadoopConf) {
    return DefaultEngine.create(hadoopConf);
  }

  /** Adds Catalyst expression evaluation while retaining every other engine handler. */
  public static Engine withSparkExpressions(Engine engine) {
    Engine delegate = requireNonNull(engine, "engine is null");
    ExpressionHandler expressions = new SparkExpressionHandler(delegate.getExpressionHandler());
    return new Engine() {
      @Override
      public ExpressionHandler getExpressionHandler() {
        return expressions;
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
        return delegate.getMetricsReporters();
      }
    };
  }
}
