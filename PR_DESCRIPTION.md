<!--
Thanks for sending a pull request!  Here are some tips for you:
  1. If this is your first time, please read our contributor guidelines: https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md
  2. If the PR is unfinished, add '[WIP]' in your PR title, e.g., '[WIP] Your PR title ...'.
  3. Be sure to keep the PR description updated to reflect all changes.
  4. Please write your PR title to summarize what this PR proposes.
  5. If possible, provide a concise example to reproduce the issue for a faster review.
  6. If applicable, include the corresponding issue number in the PR title and link it in the body.
-->

#### Which Delta project/connector is this regarding?

<!--
Please add the component selected below to the beginning of the pull request title

For example: [Spark] Title of my pull request
-->

- [ ] Spark
- [ ] Standalone
- [ ] Flink
- [x] Kernel
- [ ] Other (fill in here)

## Description

This PR adds support for Coordinated Commits v2 (CCv2) tables to the Delta Kernel benchmarking framework, enabling performance testing of tables using staged commits with Unity Catalog coordination.

### Key Changes

**New Models:**
* **`CCv2Info` class**: Encapsulates CCv2 table configuration including staged commits mapping, UC table ID, and last backfilled version. Provides `createUCClient()` method to initialize an `InMemoryUCClient` with pre-populated staged commits.
* **Extended `TableInfo` class**: Added `ccv2_enabled` flag to indicate CCv2 tables and lazy loading of `CCv2Info` from `ccv2_info.json`.

**Workload Runner Enhancements:**
* **`WorkloadRunner.loadSnapshot()`**: New method that provides a unified interface for loading snapshots from both CCv2 tables (using `UCCatalogManagedClient` production code path) and regular filesystem tables (using `TableManager`).
* **`ReadMetadataRunner`**: Updated to use the new `loadSnapshot()` method for consistent snapshot loading.
* **`WriteRunner`**: 
  - Uses `loadSnapshot()` for snapshot initialization
  - Enhanced cleanup logic to handle both `_delta_log/` and `_delta_log/_staged_commits/` directories for CCv2 tables
  - Improved error handling in cleanup

**Test Infrastructure:**
* Added complete `basic_ccv2` test table with:
  - 2 backfilled commits (versions 0-1) in `_delta_log/`
  - 2 staged commits (versions 2-3) in `_delta_log/_staged_commits/`
  - `ccv2_info.json` with UC table ID and staged commit mappings
  - Two workload specs: `read_with_staged` and `write_with_staged`
* Updated `build.sbt` to include necessary Unity Catalog dependencies

## How was this patch tested?

* Added `basic_ccv2` test table with complete CCv2 structure (backfilled and staged commits)
* Added read workload spec (`read_with_staged`) to test reading from CCv2 tables with staged commits
* Added write workload spec (`write_with_staged`) to test writing to CCv2 tables and verifying staged commit handling

## Does this PR introduce _any_ user-facing changes?

<!--
If yes, please clarify the previous behavior and the change this PR proposes - provide the console output, description and/or an example to show the behavior difference if possible.

If possible, please also clarify if this is a user-facing change compared to the released Delta Lake versions or within the unreleased branches such as master.

If no, write 'No'.
-->

No.


