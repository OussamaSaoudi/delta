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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import io.delta.storage.commit.uniform.UniformMetadata;

/**
 * REST-based implementation of {@link UCClient} for kernel benchmarks.
 *
 * <p>This client uses {@link HttpURLConnection} (zero extra dependencies) to talk to the
 * Unity Catalog REST API. It is <strong>read-only</strong> — {@link #commit} throws
 * {@link UnsupportedOperationException}.
 *
 * <p>The {@link #getCommits} implementation uses query parameters instead of a request body
 * because {@link HttpURLConnection} silently converts GET to POST when
 * {@code setDoOutput(true)} is called, and the UC getCommits endpoint only accepts GET.
 */
public class UCRestClient implements UCClient {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String baseUri;
    private final String token;
    private final String orgId;

    public UCRestClient(String ucEndpoint, String ucToken) {
        this(ucEndpoint, ucToken, null);
    }

    public UCRestClient(String ucEndpoint, String ucToken, String orgId) {
        if (ucEndpoint == null || ucEndpoint.isEmpty()) {
            throw new IllegalArgumentException("UC endpoint must not be null or empty");
        }
        if (ucToken == null || ucToken.isEmpty()) {
            throw new IllegalArgumentException("UC token must not be null or empty");
        }
        this.baseUri = ucEndpoint.replaceAll("/+$", "");
        this.token = ucToken;
        this.orgId = orgId;
    }

    /**
     * Create a client from environment variables.
     * Reads DATABRICKS_HOST / UC_ENDPOINT and DATABRICKS_TOKEN / UC_TOKEN.
     */
    public static UCRestClient fromEnvironment() {
        String endpoint = System.getenv("DATABRICKS_HOST");
        if (endpoint == null) {
            endpoint = System.getenv("UC_ENDPOINT");
        }
        String token = System.getenv("DATABRICKS_TOKEN");
        if (token == null) {
            token = System.getenv("UC_TOKEN");
        }
        if (endpoint == null || token == null) {
            throw new IllegalStateException(
                    "UC credentials not found. Set DATABRICKS_HOST and DATABRICKS_TOKEN.");
        }
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }
        String orgId = System.getenv("DATABRICKS_ORG_ID");
        return new UCRestClient(endpoint, token, orgId);
    }

    @Override
    public String getMetastoreId() throws IOException {
        String url = baseUri + "/api/2.1/unity-catalog/metastores/summary";
        HttpURLConnection conn = openConnection(url, "GET");
        try {
            int status = conn.getResponseCode();
            String body = readResponse(conn);
            if (status != 200) {
                throw new IOException(
                        "Failed to get metastore summary: HTTP " + status + " - " + body);
            }
            MetastoreSummary summary = MAPPER.readValue(body, MetastoreSummary.class);
            return summary.metastoreId;
        } finally {
            conn.disconnect();
        }
    }

    @Override
    public GetCommitsResponse getCommits(
            String tableId,
            URI tableUri,
            Optional<Long> startVersion,
            Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException {

        // Normalize s3a:// to s3:// for the UC API (UC uses native cloud schemes)
        String tableUriForApi = tableUri.toString();
        if (tableUriForApi.startsWith("s3a://")) {
            tableUriForApi = "s3://" + tableUriForApi.substring("s3a://".length());
        }

        // Build URL with query params (HttpURLConnection can't do GET-with-body)
        StringBuilder url = new StringBuilder(baseUri);
        url.append("/api/2.1/unity-catalog/delta/preview/commits");
        url.append("?table_id=").append(URLEncoder.encode(tableId, "UTF-8"));
        url.append("&table_uri=").append(URLEncoder.encode(tableUriForApi, "UTF-8"));
        url.append("&start_version=").append(startVersion.orElse(0L));
        endVersion.ifPresent(v -> {
            try {
                url.append("&end_version=").append(v);
            } catch (Exception e) {
                // StringBuilder.append doesn't throw, but lambda requires try-catch
            }
        });

        HttpURLConnection conn = openConnection(url.toString(), "GET");
        try {
            int status = conn.getResponseCode();
            String body = readResponse(conn);
            if (status != 200) {
                throw new IOException(String.format(
                        "getCommits failed for table '%s': HTTP %d - %s",
                        tableId, status, body));
            }

            RestGetCommitsResponse response =
                    MAPPER.readValue(body, RestGetCommitsResponse.class);

            // Construct Commit objects with Hadoop FileStatus.
            // Use the ORIGINAL tableUri (s3a://) for Hadoop-readable paths.
            Path commitDir = new Path(new Path(new Path(tableUri), "_delta_log"), "_commits");
            List<Commit> commits = new ArrayList<>();
            if (response.commits != null) {
                for (CommitInfo ci : response.commits) {
                    FileStatus fs = new FileStatus(
                            ci.fileSize != null ? ci.fileSize : 0L,
                            false /* isdir */,
                            0 /* block_replication */,
                            0 /* blocksize */,
                            ci.fileModificationTimestamp != null
                                    ? ci.fileModificationTimestamp : 0L,
                            new Path(commitDir, ci.fileName));
                    commits.add(new Commit(
                            ci.version,
                            fs,
                            ci.timestamp != null ? ci.timestamp : 0L));
                }
            }

            long latestVersion = response.latestTableVersion != null
                    ? response.latestTableVersion : -1L;
            return new GetCommitsResponse(commits, latestVersion);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Not supported — this client is read-only for benchmarks.
     */
    @Override
    public void commit(
            String tableId,
            URI tableUri,
            Optional<Commit> commit,
            Optional<Long> lastKnownBackfilledVersion,
            boolean disown,
            Optional<AbstractMetadata> newMetadata,
            Optional<AbstractProtocol> newProtocol,
            Optional<UniformMetadata> uniform)
            throws IOException, CommitFailedException, UCCommitCoordinatorException {
        throw new UnsupportedOperationException(
                "UCRestClient is read-only; commit() is not supported for benchmarks");
    }

    @Override
    public void close() throws IOException {
        // No persistent resources to close
    }

    // =========================================================================
    // HTTP helpers
    // =========================================================================

    private HttpURLConnection openConnection(String url, String method) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Accept", "application/json");
        if (orgId != null && !orgId.isEmpty()) {
            conn.setRequestProperty("X-Databricks-Org-Id", orgId);
        }
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        return conn;
    }

    private String readResponse(HttpURLConnection conn) throws IOException {
        java.io.InputStream is = conn.getResponseCode() >= 400
                ? conn.getErrorStream()
                : conn.getInputStream();
        if (is == null) {
            return "";
        }
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    // =========================================================================
    // Response POJOs
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class MetastoreSummary {
        @JsonProperty("metastore_id")
        public String metastoreId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RestGetCommitsResponse {
        @JsonProperty("commits")
        public List<CommitInfo> commits;

        @JsonProperty("latest_table_version")
        public Long latestTableVersion;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CommitInfo {
        @JsonProperty("version")
        public Long version;

        @JsonProperty("timestamp")
        public Long timestamp;

        @JsonProperty("file_name")
        public String fileName;

        @JsonProperty("file_size")
        public Long fileSize;

        @JsonProperty("file_modification_timestamp")
        public Long fileModificationTimestamp;
    }
}
