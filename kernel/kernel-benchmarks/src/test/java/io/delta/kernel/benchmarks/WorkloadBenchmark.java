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
        return DefaultEngine.create(configureHadoopConfiguration());
      } else {
        throw new IllegalArgumentException("Unsupported engine: " + engineName);
      }
    }

    /**
     * Creates and configures a Hadoop Configuration object with S3 credentials from environment
     * variables.
     *
     * <p>Reads the following environment variables if present:
     *
     * <ul>
     *   <li>AWS_ACCESS_KEY_ID → fs.s3a.access.key (and fs.s3.access.key)
     *   <li>AWS_SECRET_ACCESS_KEY → fs.s3a.secret.key (and fs.s3.secret.key)
     *   <li>AWS_SESSION_TOKEN → fs.s3a.session.token (and fs.s3.session.token)
     * </ul>
     *
     * <p>Also sets the S3A implementation and credentials provider based on whether a session token
     * is present. Configures both s3:// and s3a:// schemes to use the same implementation.
     *
     * @return configured Hadoop Configuration object
     */
    private static Configuration configureHadoopConfiguration() {
      Configuration conf = new Configuration();

      // Set S3A implementation for both s3:// and s3a:// schemes
      String s3aImpl = "org.apache.hadoop.fs.s3a.S3AFileSystem";
      conf.set("fs.s3a.impl", s3aImpl);
      conf.set("fs.s3.impl", s3aImpl);

      // Read S3 credentials from standard AWS environment variables
      String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
      String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      String sessionToken = System.getenv("AWS_SESSION_TOKEN");

      // Configure S3 credentials if provided (set for both s3 and s3a schemes)
      if (accessKey != null && !accessKey.trim().isEmpty()) {
        conf.set("fs.s3a.access.key", accessKey);
        conf.set("fs.s3.access.key", accessKey);
      }

      if (secretKey != null && !secretKey.trim().isEmpty()) {
        conf.set("fs.s3a.secret.key", secretKey);
        conf.set("fs.s3.secret.key", secretKey);
      }

      if (sessionToken != null && !sessionToken.trim().isEmpty()) {
        conf.set("fs.s3a.session.token", sessionToken);
        conf.set("fs.s3.session.token", sessionToken);
        // Use TemporaryAWSCredentialsProvider when session token is present
        conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        conf.set(
            "fs.s3.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
      } else if (accessKey != null && secretKey != null) {
        // Use SimpleAWSCredentialsProvider when only access key and secret are present
        conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set(
            "fs.s3.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      }

      return conf;
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

  /**
   * TODO: In the future, this can be extracted so that new benchmarks with custom BenchmarkStates
   * can be easily constructed.
   */
  public static void main(String[] args) throws RunnerException, IOException {
    // Get workload specs from the workloads directory
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths
    List<WorkloadSpec> filteredSpecs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      // TODO(#5420): In the future, we can filter specific workloads using command line args here.
      filteredSpecs.addAll(spec.getWorkloadVariants());
    }

    // Convert paths into a String array for JMH. JMH requires that parameters be of type String[].
    String[] workloadSpecsArray =
        filteredSpecs.stream().map(WorkloadSpec::toJsonString).toArray(String[]::new);

    // Configure and run JMH benchmark with the loaded workload specs
    Options opt =
        new OptionsBuilder()
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
