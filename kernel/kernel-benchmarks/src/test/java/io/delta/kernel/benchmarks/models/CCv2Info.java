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

package io.delta.kernel.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.unity.InMemoryUCClient;
import io.delta.unity.UCCatalogManagedCommitter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import scala.collection.mutable.ArrayBuffer;

/**
 * Represents CCv2 (Coordinated Commits v2) configuration for a Delta table used in benchmarks.
 *
 * <p>This class contains information about staged commits that need to be registered with the Unity
 * Catalog commit coordinator for CCv2 tables. The staged commits are stored in the
 * `_delta_log/_staged_commits/` directory.
 *
 * <p>Example JSON structure:
 *
 * <pre>{@code
 * {
 *   "max_committed_version": 0,
 *   "log_tail": [
 *     {
 *       "version": 1,
 *       "staged_commit_name": "00000000-0000-0000-0000-000000000001.json",
 *       "commit_timestamp": 1697049600000
 *     }
 *   ]
 * }
 * }</pre>
 */
public class CCv2Info {

  /**
   * Represents a single staged commit in the CCv2 log tail.
   *
   * <p>Each staged commit corresponds to a UUID-based JSON file in the
   * `_delta_log/_staged_commits/` directory that contains the Delta log actions for that version.
   */
  public static class StagedCommit {
    /** The version number of this commit. */
    @JsonProperty("version")
    private long version;

    /**
     * The filename of the staged commit, relative to `_delta_log/_staged_commits/`.
     *
     * <p>Example: "00000000-0000-0000-0000-000000000000.json"
     */
    @JsonProperty("staged_commit_name")
    private String stagedCommitName;

    /**
     * The InCommitTimestamp of this commit. This should only be used if InCommitTimestamp is
     * enabled. Otherwise, this field may be null and the commit timestamp is the file modification
     * time of the staged commit file.
     */
    @JsonProperty("in_commit_timestamp")
    private Long inCommitTimestamp;

    /** Default constructor for Jackson deserialization. */
    public StagedCommit() {}

    /**
     * Constructor for creating a StagedCommit.
     *
     * @param version the version number of the commit
     * @param stagedCommitName the filename of the staged commit
     */
    public StagedCommit(long version, String stagedCommitName) {
      this.version = version;
      this.stagedCommitName = stagedCommitName;
    }

    /** @return the version number of this commit */
    public long getVersion() {
      return version;
    }

    /** @return the filename of the staged commit */
    public String getStagedCommitName() {
      return stagedCommitName;
    }

    /** @return the commit timestamp of this commit */
    public Long getInCommitTimestamp() {
      return inCommitTimestamp;
    }

    /**
     * Resolves the full path to the staged commit file.
     *
     * @param tableRoot the root path of the Delta table
     * @return the full path to the staged commit file
     */
    public String getFullPath(String tableRoot) {
      return Paths.get(tableRoot, "_delta_log", "_staged_commits", stagedCommitName)
          .toUri() // Format this as a URI
          .toString();
    }

    @Override
    public String toString() {
      return String.format(
          "StagedCommit{version=%d, stagedCommitName='%s'}", version, stagedCommitName);
    }

    public Commit toCommit(Engine engine, String tableRoot) throws IOException {
      String stagedCommitPath = getFullPath(tableRoot);
      FileStatus fileStatus = engine.getFileSystemClient().getFileStatus(stagedCommitPath);
      long commitTimestamp =
          inCommitTimestamp != null ? inCommitTimestamp : fileStatus.getModificationTime();
      // convert to hadoop FileStatus

      org.apache.hadoop.fs.FileStatus hadoopFileStatus =
          UCCatalogManagedCommitter.kernelFileStatusToHadoopFileStatus(fileStatus);
      return new Commit(version, hadoopFileStatus, commitTimestamp);
    }
  }

  /**
   * The list of staged commits that make up the log tail for this CCv2 table.
   *
   * <p>These commits have been ratified by the Unity Catalog coordinator but may not yet be
   * backfilled to the regular Delta log.
   */
  @JsonProperty("log_tail")
  private List<StagedCommit> logTail;

  @JsonProperty("last_backfilled_version")
  private long lastBackfilledVersion;

  /**
   * The Unity Catalog table ID for this CCv2 table.
   *
   * <p>This ID must match the ucTableId in the table's metadata configuration and is used when
   * registering commits with the Unity Catalog coordinator.
   */
  @JsonProperty("uc_table_id")
  private String ucTableId;

  /** Default constructor for Jackson deserialization. */
  public CCv2Info() {}

  /**
   * Constructor for creating CCv2Info. The log tail contains the list of staged commits. Each
   * staged commit contains the version and the staged commit filename. Since version info is
   * included, the logTail list does not need to be ordered.
   *
   * @param logTail the list of staged commits
   */
  public CCv2Info(List<StagedCommit> logTail) {
    this.logTail = logTail;
  }

  /**
   * Gets the list of staged commits in the log tail.
   *
   * @return the list of staged commits, or an empty list if none
   */
  public List<StagedCommit> getLogTail() {
    return logTail != null ? logTail : Collections.emptyList();
  }

  /* * @return the last ratified version of the delta table */
  public long getLastBackfilledVersion() {
    return lastBackfilledVersion;
  }

  /**
   * Gets the Unity Catalog table ID for this CCv2 table.
   *
   * @return the UC table ID
   */
  public String getUcTableId() {
    return ucTableId;
  }

  /**
   * Creates and initializes an InMemoryUCClient for this CCv2 table.
   *
   * <p>This method sets up the UCClient with the correct maxRatifiedVersion (from
   * lastBackfilledVersion) and pre-populates it with all staged commits from the log_tail.
   *
   * @param engine the Engine to use for reading staged commit files
   * @param tableRoot the root path of the Delta table
   * @return an initialized InMemoryUCClient ready for use with UCCatalogManagedClient
   * @throws IOException if there's an error reading staged commit files
   */
  public InMemoryUCClient createUCClient(Engine engine, String tableRoot) throws IOException {
    InMemoryUCClient ucClient = new InMemoryUCClient("benchmark-metastore");

    // Create TableData with maxRatifiedVersion pre-set to lastBackfilledVersion
    // This avoids reading backfilled commits from disk
    InMemoryUCClient.TableData tableData =
        new InMemoryUCClient.TableData(this.lastBackfilledVersion, new ArrayBuffer<Commit>());

    // Add only staged commits from the log_tail to TableData
    for (StagedCommit stagedCommit : this.logTail) {
      Commit commit = stagedCommit.toCommit(engine, tableRoot);
      tableData.appendCommit(commit, Optional.empty(), Optional.empty());
    }

    // Register the pre-initialized table with UCClient
    ucClient.createTableIfNotExistsOrThrow(this.ucTableId, tableData);

    return ucClient;
  }

  /**
   * Loads a CCv2Info instance from a JSON file.
   *
   * @param jsonPath the path to the JSON file containing the CCv2Info
   * @return the CCv2Info instance parsed from the file
   * @throws IOException if there is an error reading or parsing the file
   */
  public static CCv2Info fromJsonPath(String jsonPath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(jsonPath), CCv2Info.class);
  }

  @Override
  public String toString() {
    return String.format("CCv2Info{logTail=%s}", logTail);
  }
}
