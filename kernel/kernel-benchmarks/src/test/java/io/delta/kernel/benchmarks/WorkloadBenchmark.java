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

package io.delta.kernel.benchmarks;

import static io.delta.kernel.benchmarks.BenchmarkUtils.*;

import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Generic JMH benchmark for all workload types. Automatically loads and runs benchmarks based on
 * JSON workload specifications.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WorkloadBenchmark<T> {

  /** Default implementation of BenchmarkState that supports only the "default" engine. */
  public static class DefaultBenchmarkState extends AbstractBenchmarkState {
    @Override
    protected Engine getEngine(String engineName) {
      if (engineName.equals("default")) {
        return DefaultEngine.create(new Configuration());
      } else {
        throw new IllegalArgumentException("Unsupported engine: " + engineName);
      }
    }
  }

  /**
   * Benchmark method that executes the workload runner specified in the state as a benchmark.
   *
   * @param state The benchmark state containing the workload runner to execute.
   * @param blackhole The Blackhole provided by JMH to consume results and prevent dead code
   *     elimination.
   * @throws Exception If any error occurs during workload execution.
   */
  @Benchmark
  public void benchmarkWorkload(DefaultBenchmarkState state, Blackhole blackhole) throws Exception {
    WorkloadRunner runner = state.getRunner();
    runner.executeAsBenchmark(blackhole);
  }

  /** Helper class to hold parsed command-line arguments. */
  private static class BenchmarkArgs {
    final List<java.nio.file.Path> specDirectories;
    final Optional<String> filter;
    final List<String> jmhArgs;

    BenchmarkArgs(
        List<java.nio.file.Path> specDirectories, Optional<String> filter, List<String> jmhArgs) {
      this.specDirectories = specDirectories;
      this.filter = filter;
      this.jmhArgs = jmhArgs;
    }
  }

  /**
   * Parses command-line arguments to extract benchmark-specific options.
   *
   * <p>Supported custom arguments:
   *
   * <ul>
   *   <li>--with_spec_path <path>: Add a custom directory to search for workload specs
   *   <li>--filter <pattern>: Filter workloads to run by name pattern
   * </ul>
   *
   * <p>All other arguments are passed through to JMH. For async-profiler support, use JMH's
   * built-in profiler flag: -prof async:output=flamegraph
   *
   * @param args The command-line arguments to parse
   * @return Parsed benchmark arguments
   */
  private static BenchmarkArgs parseArgs(String[] args) {
    Optional<String> customSpecPath = Optional.empty();
    Optional<String> filter = Optional.empty();
    List<String> jmhArgs = new ArrayList<>();

    for (int i = 0; i < args.length; i++) {
      if ("--with_spec_path".equals(args[i]) && i + 1 < args.length) {
        customSpecPath = Optional.of(args[i + 1]);
        i++; // Skip the next argument (the path value)
      } else if ("--filter".equals(args[i]) && i + 1 < args.length) {
        filter = Optional.of(args[i + 1]);
        i++; // Skip the next argument (the filter value)
      } else {
        jmhArgs.add(args[i]);
      }
    }

    // Build list of spec directories to load from
    List<java.nio.file.Path> specDirs = new ArrayList<>();
    specDirs.add(WORKLOAD_SPECS_DIR);
    customSpecPath.ifPresent(path -> specDirs.add(java.nio.file.Paths.get(path)));
    System.out.println("Using workload spec directories: " + specDirs);
    return new BenchmarkArgs(specDirs, filter, jmhArgs);
  }

  /**
   * TODO: In the future, this can be extracted so that new benchmarks with custom BenchmarkStates
   * can be easily constructed.
   */
  public static void main(String[] args) throws RunnerException, IOException {
    // Parse command-line arguments
    BenchmarkArgs parsedArgs = parseArgs(args);

    // Get workload specs from all directories
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(parsedArgs.specDirectories);
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths and apply filter if specified
    List<WorkloadSpec> filteredSpecs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      filteredSpecs.addAll(spec.getWorkloadVariants());
    }

    // Convert to JSON strings, applying filter if specified
    java.util.stream.Stream<WorkloadSpec> specStream = filteredSpecs.stream();
    if (parsedArgs.filter.isPresent()) {
      String filterValue = parsedArgs.filter.get();
      specStream = specStream.filter(spec -> spec.getFullName().contains(filterValue));
    }
    String[] workloadSpecsArray = specStream.map(WorkloadSpec::toJsonString).toArray(String[]::new);

    // Configure and run JMH benchmark with the loaded workload specs
    // Start with command-line args to allow users to override defaults
    OptionsBuilder optBuilder = new OptionsBuilder();

    // Parse JMH command-line arguments if provided
    if (!parsedArgs.jmhArgs.isEmpty()) {
      try {
        optBuilder.parent(
            new org.openjdk.jmh.runner.options.CommandLineOptions(
                parsedArgs.jmhArgs.toArray(new String[0])));
      } catch (org.openjdk.jmh.runner.options.CommandLineOptionException e) {
        throw new RunnerException("Failed to parse JMH command-line arguments", e);
      }
    }

    // Apply our defaults (can be overridden by command-line args above)
    Options opt =
        optBuilder
            .include(WorkloadBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .param("workloadSpecJson", workloadSpecsArray)
            // TODO: In the future, this can be extended to support multiple engines.
            .param("engineName", "default")
            // TODO(#5420): Allow configuring forks, warmup, and measurement via command line args.
            .forks(1)
            .warmupIterations(3) // Proper warmup for production benchmarks
            .measurementIterations(5) // Proper measurement iterations for production benchmarks
            .warmupTime(TimeValue.seconds(1))
            .measurementTime(TimeValue.seconds(1))
            .addProfiler(KernelMetricsProfiler.class)
            .build();

    new Runner(opt, new WorkloadOutputFormat()).run();
  }
}
