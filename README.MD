# PySpark Data Pipeline Project

## Overview
This project demonstrates a simple PySpark ETL pipeline using sample datasets.

- `userdata.parquet`
- `products-1000.csv`

### Why this project
This repository is a hands-on example for showcasing end-to-end PySpark data engineering:

1. Reading mixed sources (Parquet + CSV) from local storage.
2. Data transformation (timestamp+date parsing, ID enrichment, categorical joins).
3. Quality-focused cleansing (remove duplicates, normalize product categories).
4. Production output formats (CSV + parquet aggregations).
5. Performance tuning (coalesce, partitioning, job-friendly in-memory operations).

It is ideal for learning how to build maintainable data pipeline stages and for highlighting PySpark APIs in a real workflow.

## Step 1: Generate usersales
Run `gen_usersales.py` to combine the two source datasets and create `usersales.csv`.

## Step 2: Add a date_id column
Add a new integer-based date ID field to `usersales` for easier date-based grouping.

## Step 3: Productionize the pipeline (cleanup + join)
Convert and clean raw sales data:

- Input:
  - `usersalesdtid.csv`
  - `pcategory.csv`
  - `products-1000.csv`
- Output:
  - `usersalesprod.csv`

## Step 4: Aggregate data to Parquet
Build aggregated datasets for analytics:

- Input:
  - `usersalesprod.csv`
  - `pcategory.csv`
- Output:
  - `salesbymonth.parquet`
  - `salesbycategory.parquet`

## Notes
- Keep source filenames consistent.
- Validate formats and schema before each pipeline stage.
- Use Spark optimizations (`coalesce`, partitioning) for production writes.
