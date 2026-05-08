# NEAR Artifact Bundle

This directory contains the analysis artifact for the NEAR sharding study. The contents are organized as query templates, exported intermediate files, workload-specific slices, shard-era folders, and figure-generation scripts used to summarize the results.

The repository is centered on the public BigQuery dataset `bigquery-public-data.crypto_near_mainnet_us`. Many files preserve their original working names from the analysis pipeline.

## Important Note About File Formats

Several files with a `.csv` extension are not comma-separated tables. Many of them contain SQL queries or query fragments that were saved with their original filenames. Before treating any file as structured CSV input, open it and verify whether it is:

- a SQL query template,
- an exported table,
- a manually curated helper file, or
- a downstream summary generated for figures.

## Top-Level Layout

## Root files

The root directory contains broad query templates and summary files for network-wide counts and interaction classes, including:

- transaction, action, receipt, account, and contract count queries,
- graph-style interaction breakdowns such as `AMG`, `CDG`, `CUG`, `DPG`, and `TFG`,
- hot user and hot contract analyses, and
- distinct user/contract counts associated with hot entities.

Representative files include `txCount.csv`, `actionTypeCount.csv`, `hotUser.csv`, and `AMG.csv`.

## Shard-era folders

These folders partition the analysis by the active number of shards and the corresponding time range:

| Folder | Period |
| --- | --- |
| `1 Shard (Begin - 16 Nov 2021)` | Genesis through 16 Nov 2021 |
| `4 Shard (17 Nov 2021 - 11 Mar 2024)` | 17 Nov 2021 through 11 Mar 2024 |
| `5 Shard (12 Mar 2024 - 20 Mar 2024)` | 12 Mar 2024 through 20 Mar 2024 |
| `6 Shard (21 Mar 2024 - 16 Mar 2025)` | 21 Mar 2024 through 16 Mar 2025 |
| `7 Shard (17 Mar 2025 - 18 Mar 2025)` | 17 Mar 2025 through 18 Mar 2025 |
| `8 Shard (19 Mar 2025 - 17 Aug 2025)` | 19 Mar 2025 through 17 Aug 2025 |
| `9 Shard (18 Aug 2025 - 31 Dec 2025)` | 18 Aug 2025 through 31 Dec 2025 |

Each shard folder contains the queries or outputs used for that era, such as:

- action type counts,
- per-shard user activity,
- deployed and distinct contract counts,
- contract usage metrics,
- gas usage summaries,
- hot users and hot contracts, and
- top sender, receiver, deployer, and contract-user tables.

Most shard folders also include a small `readMe.txt` or `ReadMe.txt` file documenting the shard split, segment names, shard IDs, and the BigQuery date predicate used for that period.

## Workload

The `Workload/` folder groups the artifact by load regime rather than by shard era. It includes low, moderate, and heavy workload windows, along with per-second workload series and hotspot analyses. The accompanying `readME.txt` identifies the time windows used for each workload category.

Representative contents include:

- `txPerSecond.csv`,
- `actionPerSecond.csv`,
- `receiptLatencyPerSecond.csv`,
- `hotUser Low.csv`, `hotUser Moderate.csv`, `hotUser Heavy.csv`, and
- `gasUsagePerHotContract *.csv`.

## Figure

The `Figure/` folder contains Python utilities used to derive summary tables and metrics for figures. Current scripts include:

- `throughput_by_section.py`,
- `extract_partition_metrics.py`,
- `s2s_top5_metrics_table.py`, and
- `queue_correlations.py`.

Some of these scripts expect an Excel workbook in the same directory, for example `Fig_all_new.xlsx`, and use Python packages such as `pandas` and `openpyxl`.

## Previous

The `Previous/` folder stores earlier or supporting exports, including transaction-rate summaries, block-time summaries, validator and producer views, token flow summaries, contract activity reports, and related historical query outputs.

## Reproducing Figure Summaries

If the expected workbook and Python environment are available, run the scripts from the `Figure/` directory.

Example:

```bash
cd Figure
python throughput_by_section.py --sheet tps latency --output throughput_by_section_summary.csv
```

For partition-balance metrics:

```bash
cd Figure
python extract_partition_metrics.py
```

## Conventions and Caveats

- Filename casing is not fully uniform. Both `readMe.txt` and `ReadMe.txt` appear in this artifact.
- Some files are true CSV exports, while others are SQL saved with a `.csv` suffix.
- The folder names should be treated as the primary source for the intended shard-era grouping.
- Some internal notes may use query predicates that should be checked against the final folder label before publication.

## Recommended Entry Points

If you are new to this artifact, start with:

1. the shard folder corresponding to the period you want to study,
2. `Workload/readME.txt` for load-based slices,
3. `Figure/` for derived summaries used in plots, and
4. the root query files for network-wide totals and interaction classifications.