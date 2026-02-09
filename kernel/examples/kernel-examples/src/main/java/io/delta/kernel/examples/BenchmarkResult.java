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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Result from a single benchmark workload execution.
 */
public class BenchmarkResult {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public String workloadName;
    public String specType;
    public List<Long> durationsMs;
    public long minDurationMs;
    public long maxDurationMs;
    public double avgDurationMs;
    public double stdDevDurationMs;
    public boolean success;
    public String errorMessage;
    public Map<String, Double> customMetrics;

    public static BenchmarkResult success(
            String workloadName, String specType, List<Long> durations) {
        BenchmarkResult r = new BenchmarkResult();
        r.workloadName = workloadName;
        r.specType = specType;
        r.durationsMs = durations;
        r.success = true;
        r.customMetrics = new HashMap<>();

        if (!durations.isEmpty()) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long sum = 0;
            for (long d : durations) {
                min = Math.min(min, d);
                max = Math.max(max, d);
                sum += d;
            }
            r.minDurationMs = min;
            r.maxDurationMs = max;
            r.avgDurationMs = (double) sum / durations.size();
            r.stdDevDurationMs = calculateStdDev(durations, r.avgDurationMs);
        }

        return r;
    }

    public static BenchmarkResult failure(
            String workloadName, String specType, String errorMessage) {
        BenchmarkResult r = new BenchmarkResult();
        r.workloadName = workloadName;
        r.specType = specType;
        r.durationsMs = Collections.emptyList();
        r.success = false;
        r.errorMessage = errorMessage;
        r.customMetrics = new HashMap<>();
        return r;
    }

    private static double calculateStdDev(List<Long> values, double mean) {
        if (values.size() <= 1) {
            return 0.0;
        }
        double sumSquaredDiffs = 0.0;
        for (long v : values) {
            double diff = v - mean;
            sumSquaredDiffs += diff * diff;
        }
        return Math.sqrt(sumSquaredDiffs / (values.size() - 1));
    }

    /**
     * Convert to JSON node for serialization.
     */
    public ObjectNode toJson() {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("workloadName", workloadName);
        node.put("specType", specType);
        node.put("success", success);
        node.put("minDurationMs", minDurationMs);
        node.put("maxDurationMs", maxDurationMs);
        node.put("avgDurationMs", avgDurationMs);
        node.put("stdDevDurationMs", stdDevDurationMs);

        if (errorMessage != null) {
            node.put("errorMessage", errorMessage);
        }

        ArrayNode durationsNode = node.putArray("durationsMs");
        for (long d : durationsMs) {
            durationsNode.add(d);
        }

        if (customMetrics != null && !customMetrics.isEmpty()) {
            ObjectNode metricsNode = node.putObject("customMetrics");
            for (Map.Entry<String, Double> entry : customMetrics.entrySet()) {
                metricsNode.put(entry.getKey(), entry.getValue());
            }
        }

        return node;
    }
}
