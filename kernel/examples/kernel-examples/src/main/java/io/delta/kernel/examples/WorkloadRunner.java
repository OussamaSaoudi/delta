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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Standalone benchmark runner for delta-kernel-java.
 *
 * <p>Reads workload specification directories and executes benchmarks using
 * the delta-kernel API. Supports local and cloud (S3, ABFS, GCS) table paths.
 *
 * <p>Directory structure expected:
 * <pre>
 * workload_specs/
 *   &lt;table_name&gt;/
 *     table_info.json          # Contains name, table_root_path
 *     specs/
 *       &lt;case_name&gt;/
 *         spec.json            # Contains type (snapshot_construction, read)
 * </pre>
 *
 * <p>Usage:
 * <pre>
 * java -cp ... io.delta.kernel.examples.WorkloadRunner \
 *   --workload-dir /path/to/specs \
 *   --warmup 2 --iterations 5 \
 *   --output /path/to/results.json
 * </pre>
 *
 * <p>S3 credentials are resolved from (in priority order):
 * <ol>
 *   <li>CLI flags: {@code --s3-access-key} and {@code --s3-secret-key}</li>
 *   <li>Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY</li>
 *   <li>Hadoop config files (core-site.xml)</li>
 *   <li>Default AWS credentials provider chain (IAM roles, instance profiles)</li>
 * </ol>
 */
public class WorkloadRunner {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Engine engine;
    private final int warmupIterations;
    private final int measuredIterations;

    /**
     * Create a runner with the given Hadoop configuration.
     *
     * @param hadoopConf Hadoop configuration (with cloud storage credentials if needed)
     * @param warmupIterations number of warmup iterations (not measured)
     * @param measuredIterations number of measured iterations
     */
    public WorkloadRunner(
            Configuration hadoopConf,
            int warmupIterations,
            int measuredIterations) {
        this.engine = DefaultEngine.create(hadoopConf);
        this.warmupIterations = warmupIterations;
        this.measuredIterations = measuredIterations;
    }

    /**
     * Run benchmarks from a workload_specs directory.
     *
     * @param specsDir the workload_specs directory containing table subdirectories
     * @return list of benchmark results
     */
    public List<BenchmarkResult> runBenchmarks(Path specsDir) throws IOException {
        List<BenchmarkResult> results = new ArrayList<>();

        File[] tableDirs = specsDir.toFile().listFiles(File::isDirectory);
        if (tableDirs == null || tableDirs.length == 0) {
            System.err.println("No table directories found in " + specsDir);
            return results;
        }

        for (File tableDir : tableDirs) {
            Path tableInfoPath = tableDir.toPath().resolve("table_info.json");
            if (!Files.exists(tableInfoPath)) {
                System.err.println("No table_info.json in " + tableDir + ", skipping");
                continue;
            }

            JsonNode tableInfo = MAPPER.readTree(tableInfoPath.toFile());
            String tableName = tableInfo.has("name")
                    ? tableInfo.get("name").asText()
                    : tableDir.getName();
            String tablePath = resolveTablePath(tableDir.toPath(), tableInfo);

            if (tablePath == null) {
                System.err.println("Could not resolve table path for " + tableName);
                results.add(BenchmarkResult.failure(
                        tableName, "unknown", "Could not resolve table path"));
                continue;
            }

            System.out.println("Processing table: " + tableName + " at " + tablePath);

            File specsSubDir = new File(tableDir, "specs");
            if (!specsSubDir.exists() || !specsSubDir.isDirectory()) {
                System.err.println("No specs/ directory in " + tableDir + ", skipping");
                continue;
            }

            File[] caseDirs = specsSubDir.listFiles(File::isDirectory);
            if (caseDirs == null) {
                continue;
            }

            for (File caseDir : caseDirs) {
                Path specPath = caseDir.toPath().resolve("spec.json");
                if (!Files.exists(specPath)) {
                    System.err.println("No spec.json in " + caseDir + ", skipping");
                    continue;
                }

                JsonNode spec = MAPPER.readTree(specPath.toFile());
                String specType = spec.has("type")
                        ? spec.get("type").asText()
                        : "unknown";
                String workloadName = tableName + "/" + caseDir.getName() + "/" + specType;

                System.out.println("Running workload: " + workloadName);

                try {
                    BenchmarkResult result = runWorkload(
                            tablePath, workloadName, specType, spec);
                    results.add(result);
                    printResult(result);
                } catch (Exception e) {
                    System.err.println("Workload failed: " + workloadName + " - " + e.getMessage());
                    results.add(BenchmarkResult.failure(
                            workloadName, specType, e.getMessage()));
                }
            }
        }

        return results;
    }

    private String resolveTablePath(Path tableDir, JsonNode tableInfo) {
        // Prefer explicit table_root_path (used for S3/cloud tables)
        if (tableInfo.has("table_root_path")) {
            return tableInfo.get("table_root_path").asText();
        }

        // Fall back to delta/ directory (local tables via symlink)
        Path deltaDir = tableDir.resolve("delta");
        if (Files.exists(deltaDir)) {
            try {
                return deltaDir.toRealPath().toString();
            } catch (IOException e) {
                return deltaDir.toString();
            }
        }

        return null;
    }

    private BenchmarkResult runWorkload(
            String tablePath, String workloadName, String specType, JsonNode spec) {
        switch (specType) {
            case "snapshot_construction":
                return runSnapshotConstruction(tablePath, workloadName);
            case "read":
                String operationType = spec.has("operation_type")
                        ? spec.get("operation_type").asText()
                        : "read_metadata";
                return runRead(tablePath, workloadName, operationType);
            default:
                return BenchmarkResult.failure(
                        workloadName, specType, "Unsupported spec type: " + specType);
        }
    }

    /**
     * Benchmark snapshot construction (log replay) time.
     */
    private BenchmarkResult runSnapshotConstruction(String tablePath, String workloadName) {
        System.out.println("  Benchmark: snapshot_construction for " + tablePath);

        // Warmup
        for (int i = 0; i < warmupIterations; i++) {
            System.out.println("  Warmup " + (i + 1) + "/" + warmupIterations);
            TableManager.loadSnapshot(tablePath).build(engine);
        }

        // Measured iterations
        List<Long> durations = new ArrayList<>();
        for (int i = 0; i < measuredIterations; i++) {
            long startNanos = System.nanoTime();
            Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            durations.add(elapsedMs);
            System.out.println("  Measured " + (i + 1) + "/" + measuredIterations
                    + ": " + elapsedMs + "ms (schema fields: "
                    + snapshot.getSchema().length() + ")");
        }

        return BenchmarkResult.success(workloadName, "snapshot_construction", durations);
    }

    /**
     * Benchmark read operations (metadata or data).
     */
    private BenchmarkResult runRead(
            String tablePath, String workloadName, String operationType) {
        System.out.println("  Benchmark: read/" + operationType + " for " + tablePath);

        // Warmup
        for (int i = 0; i < warmupIterations; i++) {
            System.out.println("  Warmup " + (i + 1) + "/" + warmupIterations);
            executeRead(tablePath, operationType);
        }

        // Measured iterations
        List<Long> durations = new ArrayList<>();
        Map<String, Double> customMetrics = new HashMap<>();

        for (int i = 0; i < measuredIterations; i++) {
            long startNanos = System.nanoTime();
            ReadResult readResult = executeRead(tablePath, operationType);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            durations.add(elapsedMs);
            System.out.println("  Measured " + (i + 1) + "/" + measuredIterations
                    + ": " + elapsedMs + "ms");

            // Capture metrics from last iteration
            if (i == measuredIterations - 1) {
                customMetrics.put("schema_field_count", (double) readResult.schemaFieldCount);
                if (readResult.scanFileCount >= 0) {
                    customMetrics.put("scan_file_count", (double) readResult.scanFileCount);
                }
            }
        }

        BenchmarkResult result = BenchmarkResult.success(workloadName, "read", durations);
        result.customMetrics = customMetrics;
        return result;
    }

    private ReadResult executeRead(String tablePath, String operationType) {
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
        int schemaFieldCount = snapshot.getSchema().length();

        if ("read_metadata".equals(operationType)) {
            return new ReadResult(schemaFieldCount, -1);
        }

        // For read_data, build a scan and enumerate the scan files.
        long scanFileCount = 0;
        Scan scan = snapshot.getScanBuilder().build();

        try (CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine)) {
            while (fileIter.hasNext()) {
                FilteredColumnarBatch batch = fileIter.next();
                try (CloseableIterator<Row> rows = batch.getRows()) {
                    while (rows.hasNext()) {
                        rows.next();
                        scanFileCount++;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading scan files", e);
        }

        return new ReadResult(schemaFieldCount, scanFileCount);
    }

    private static class ReadResult {
        final int schemaFieldCount;
        final long scanFileCount;

        ReadResult(int schemaFieldCount, long scanFileCount) {
            this.schemaFieldCount = schemaFieldCount;
            this.scanFileCount = scanFileCount;
        }
    }

    private static void printResult(BenchmarkResult result) {
        String status = result.success ? "PASS" : "FAIL";
        System.out.printf("  => %s | %s | avg=%.2fms min=%dms max=%dms stddev=%.2fms%n",
                status, result.workloadName,
                result.avgDurationMs, result.minDurationMs, result.maxDurationMs,
                result.stdDevDurationMs);
    }

    // =========================================================================
    // CLI entry point
    // =========================================================================

    public static void main(String[] args) throws Exception {
        String workloadDir = null;
        int warmup = 1;
        int iterations = 3;
        String outputFile = null;
        String s3AccessKey = null;
        String s3SecretKey = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--workload-dir":
                    workloadDir = args[++i];
                    break;
                case "--warmup":
                    warmup = Integer.parseInt(args[++i]);
                    break;
                case "--iterations":
                    iterations = Integer.parseInt(args[++i]);
                    break;
                case "--output":
                    outputFile = args[++i];
                    break;
                case "--s3-access-key":
                    s3AccessKey = args[++i];
                    break;
                case "--s3-secret-key":
                    s3SecretKey = args[++i];
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        if (workloadDir == null) {
            printUsage();
            System.exit(1);
        }

        Configuration hadoopConf = buildHadoopConfiguration(s3AccessKey, s3SecretKey);
        WorkloadRunner runner = new WorkloadRunner(hadoopConf, warmup, iterations);

        System.out.println("=".repeat(70));
        System.out.println("Delta Kernel Workload Runner");
        System.out.println("  Workload dir: " + workloadDir);
        System.out.println("  Warmup: " + warmup + ", Measured: " + iterations);
        System.out.println("=".repeat(70));

        List<BenchmarkResult> results = runner.runBenchmarks(
                java.nio.file.Paths.get(workloadDir));

        // Print summary
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("Summary");
        System.out.println("=".repeat(70));
        int passed = 0;
        int failed = 0;
        for (BenchmarkResult r : results) {
            String status = r.success ? "PASS" : "FAIL";
            if (r.success) {
                passed++;
            } else {
                failed++;
            }
            System.out.printf("  %-50s %s  avg=%.2fms%n",
                    r.workloadName, status, r.avgDurationMs);
        }
        System.out.println("Total: " + results.size()
                + " | Passed: " + passed + " | Failed: " + failed);
        System.out.println("=".repeat(70));

        // Serialize results to JSON
        ArrayNode resultsArray = MAPPER.createArrayNode();
        for (BenchmarkResult result : results) {
            resultsArray.add(result.toJson());
        }
        String json = MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(resultsArray);

        if (outputFile != null) {
            Files.write(java.nio.file.Paths.get(outputFile), json.getBytes());
            System.out.println("Results written to: " + outputFile);
        } else {
            System.out.println(json);
        }
    }

    /**
     * Build Hadoop Configuration with cloud storage credentials.
     *
     * <p>Credential sources (in priority order):
     * <ol>
     *   <li>Explicit access key / secret key (from CLI flags)</li>
     *   <li>Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)</li>
     *   <li>Hadoop config files (core-site.xml picked up automatically)</li>
     *   <li>Default AWS credentials provider chain (IAM roles, instance profiles)</li>
     * </ol>
     */
    static Configuration buildHadoopConfiguration(
            String s3AccessKey, String s3SecretKey) {
        Configuration conf = new Configuration();

        // Determine S3 credentials: CLI flags > env vars > default chain
        String accessKey = s3AccessKey;
        String secretKey = s3SecretKey;

        if (accessKey == null) {
            accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        }
        if (secretKey == null) {
            secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }

        if (accessKey != null && secretKey != null) {
            conf.set("fs.s3a.access.key", accessKey);
            conf.set("fs.s3a.secret.key", secretKey);
            System.out.println("Using explicit S3 credentials");

            String sessionToken = System.getenv("AWS_SESSION_TOKEN");
            if (sessionToken != null) {
                conf.set("fs.s3a.session.token", sessionToken);
                conf.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
                System.out.println("Using temporary session token");
            }
        } else {
            System.out.println("No explicit S3 credentials; "
                    + "using default AWS credentials provider chain");
        }

        String region = System.getenv("AWS_REGION");
        if (region != null) {
            conf.set("fs.s3a.endpoint.region", region);
        }

        // S3A filesystem settings
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.fast.upload", "true");

        return conf;
    }

    private static void printUsage() {
        System.err.println("Usage: WorkloadRunner --workload-dir <path>"
                + " [--warmup <n>] [--iterations <n>]"
                + " [--output <path>]"
                + " [--s3-access-key <key>] [--s3-secret-key <key>]");
    }
}
