# Federal spending 2024 vs 2025

Big Data course final project. We compare federal contract and grant
spending before and after the January 20, 2025 presidential transition,
using 2022-2023 as a baseline for normal year-to-year variation.

## Data
- Source: USAspending Award Data Archive, prime award transactions.
- Runs on the NYU course Dataproc cluster (master node nyu-dataproc-m;
  laptop has too little disk). We can't create clusters or buckets there.
  download.py fetches one fiscal-year zip at a time to local disk, streams
  its CSVs into HDFS, converts them with to_parquet.convert(), deletes the
  raw files, and writes Parquet to HDFS $OUT (default
  /user/<you>/fedspend/parquet) as
  $OUT/{contracts,assistance}/fy=<file>/year=<calendar year>/.
  API response shape verified 2026-11-23.
- Column names in to_parquet.py were guessed; check them against the
  real CSV headers and fix any reported as NOT FOUND.
- All analysis reads from the Parquet in $OUT, never from the raw CSVs.
  The fy= partition is just the source file; filter and group by year /
  action_date.

## Rules
- Use PySpark for all processing.
- Compare by action_date (calendar), not fiscal year.
- Negative federal_action_obligation means money was pulled back.
- Analyze loans separately from grants and contracts.
- data/ is large: never commit it, and check free disk before downloading.
