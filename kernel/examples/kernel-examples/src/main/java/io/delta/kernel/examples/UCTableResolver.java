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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

/**
 * Resolves UC-managed table metadata and obtains temporary cloud storage credentials.
 *
 * <p>This client handles two UC REST API operations:
 * <ul>
 *   <li>Table info resolution: three-part name to storage location + table ID</li>
 *   <li>Temporary credential vending: table ID to temporary cloud storage credentials</li>
 * </ul>
 *
 * <p>Uses {@link HttpURLConnection} for HTTP requests (no extra dependencies).
 */
public class UCTableResolver implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String baseUri;
    private final String token;
    private final String orgId;  // Optional workspace org ID for multi-workspace deployments

    public UCTableResolver(String ucEndpoint, String ucToken) {
        this(ucEndpoint, ucToken, null);
    }

    public UCTableResolver(String ucEndpoint, String ucToken, String orgId) {
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
     * Create a resolver from environment variables.
     * Reads from DATABRICKS_HOST and DATABRICKS_TOKEN (or UC_ENDPOINT and UC_TOKEN).
     */
    public static UCTableResolver fromEnvironment() {
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
                    "UC credentials not found in environment. " +
                    "Set DATABRICKS_HOST and DATABRICKS_TOKEN (or UC_ENDPOINT and UC_TOKEN).");
        }
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }
        String orgId = System.getenv("DATABRICKS_ORG_ID");
        return new UCTableResolver(endpoint, token, orgId);
    }

    /**
     * Resolve a three-part UC table name to its storage location and table ID.
     * Calls: GET /api/2.1/unity-catalog/tables/{full_name}
     */
    public UCTableInfo resolveTable(String fullTableName) throws IOException {
        String encodedName = URLEncoder.encode(fullTableName, StandardCharsets.UTF_8.name());
        String url = baseUri + "/api/2.1/unity-catalog/tables/" + encodedName;

        HttpURLConnection conn = openConnection(url, "GET");
        try {
            int status = conn.getResponseCode();
            String body = readResponse(conn);
            if (status != 200) {
                throw new IOException(String.format(
                        "Failed to resolve UC table '%s': HTTP %d - %s",
                        fullTableName, status, body));
            }
            return MAPPER.readValue(body, UCTableInfo.class);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Get temporary cloud storage credentials for reading a UC table.
     * Calls: POST /api/2.1/unity-catalog/temporary-table-credentials
     */
    public TempCredentials getTemporaryCredentials(String tableId) throws IOException {
        String url = baseUri + "/api/2.1/unity-catalog/temporary-table-credentials";

        String requestBody = MAPPER.writeValueAsString(
                new TempCredentialRequest(tableId, "READ"));

        HttpURLConnection conn = openConnection(url, "POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try {
            try (OutputStream os = conn.getOutputStream()) {
                os.write(requestBody.getBytes(StandardCharsets.UTF_8));
            }

            int status = conn.getResponseCode();
            String body = readResponse(conn);
            if (status != 200) {
                throw new IOException(String.format(
                        "Failed to get temp credentials for table '%s': HTTP %d - %s",
                        tableId, status, body));
            }
            return MAPPER.readValue(body, TempCredentials.class);
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Get the latest table version ratified by UC (the maxCatalogVersion).
     * Calls: GET /api/2.1/unity-catalog/delta/preview/commits
     *
     * Note: This endpoint requires GET with a body, but HttpURLConnection silently
     * converts GET to POST when setDoOutput(true) is called. So we pass parameters
     * as query parameters instead, which the UC API also accepts.
     */
    public long getLatestTableVersion(String tableId, String tableUri) throws IOException {
        String encodedTableId = URLEncoder.encode(tableId, StandardCharsets.UTF_8.name());
        String encodedTableUri = URLEncoder.encode(tableUri, StandardCharsets.UTF_8.name());
        String url = baseUri + "/api/2.1/unity-catalog/delta/preview/commits"
                + "?table_id=" + encodedTableId
                + "&table_uri=" + encodedTableUri
                + "&start_version=0";

        HttpURLConnection conn = openConnection(url, "GET");
        try {
            int status = conn.getResponseCode();
            String body = readResponse(conn);
            if (status != 200) {
                throw new IOException(String.format(
                        "Failed to get commits for table '%s': HTTP %d - %s",
                        tableId, status, body));
            }
            GetCommitsResponse response = MAPPER.readValue(body, GetCommitsResponse.class);
            return response.latestTableVersion;
        } finally {
            conn.disconnect();
        }
    }

    @Override
    public void close() {
        // No resources to close with HttpURLConnection
    }

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
    // Data classes
    // =========================================================================

    private static class GetCommitsRequest {
        @JsonProperty("table_id")
        public String tableId;
        @JsonProperty("table_uri")
        public String tableUri;
        @JsonProperty("start_version")
        public long startVersion;

        GetCommitsRequest(String tableId, String tableUri, long startVersion) {
            this.tableId = tableId;
            this.tableUri = tableUri;
            this.startVersion = startVersion;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class GetCommitsResponse {
        @JsonProperty("latest_table_version")
        public long latestTableVersion;
    }

    private static class TempCredentialRequest {
        @JsonProperty("table_id")
        public String tableId;
        @JsonProperty("operation")
        public String operation;

        TempCredentialRequest(String tableId, String operation) {
            this.tableId = tableId;
            this.operation = operation;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UCTableInfo {
        @JsonProperty("table_id")
        public String tableId;

        @JsonProperty("storage_location")
        public String storageLocation;

        @JsonProperty("table_type")
        public String tableType;

        @JsonProperty("data_source_format")
        public String dataSourceFormat;

        @JsonProperty("name")
        public String name;

        @JsonProperty("catalog_name")
        public String catalogName;

        @JsonProperty("schema_name")
        public String schemaName;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TempCredentials {
        @JsonProperty("aws_temp_credentials")
        public AwsTempCredentials awsTempCredentials;

        @JsonProperty("expiration_time")
        public long expirationTime;

        public boolean isExpired(long bufferMs) {
            return System.currentTimeMillis() + bufferMs >= expirationTime;
        }

        public boolean isExpired() {
            return isExpired(60_000);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AwsTempCredentials {
        @JsonProperty("access_key_id")
        public String accessKeyId;

        @JsonProperty("secret_access_key")
        public String secretAccessKey;

        @JsonProperty("session_token")
        public String sessionToken;
    }
}
