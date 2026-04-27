# ETL Pipeline — Logistics Delivery Data

> Technical study: end-to-end ETL pipeline applied to delivery operations data, exploring partitioned writes to object storage in Parquet format.

A focused exercise in building a multi-stage ETL pipeline with realistic business transformations. The use case is delivery logistics — a domain familiar from prior experience at Loggi — and the goal is to practice patterns commonly found in production data pipelines: schema enforcement, idempotent loads, partitioning strategy, and tested transformations.

## What this project explores

- **Multi-source extraction** with schema validation
- **Business transformations** including status normalization, UTC-aware date parsing, and Haversine distance calculation
- **Partitioned loads** to Cloudflare R2 in Parquet format
- **Test coverage** for transformation logic with `pytest`

## Stack

`Python` · `Pandas` · `NumPy` · `boto3` · `Cloudflare R2` · `Parquet` · `pytest`

## Architecture

```
sources/  →  extract.py  →  transform.py  →  load.py  →  R2 (partitioned Parquet)
                                  ↓
                              tests/
```

## Project layout

```
etl-pipeline/
├── etl/
│   ├── extract.py        # Source readers
│   ├── transform.py      # Business rules and normalization
│   └── load.py           # R2 partitioned writer
├── tests/                # Transformation tests
├── config/               # Pipeline configuration
└── main.py               # Pipeline entrypoint
```

## How to run

```bash
pip install -r requirements.txt
python main.py
```

Configure R2 credentials in `config/` before running. See `config/example.yaml` for the expected structure.

## Status

This is a study repository — code is functional but not deployed in production. Volume-tested with ~12k synthetic records; production-grade volumes were not benchmarked.

## Author

Murillo Sezerino — Analytics Engineer · Data Engineer
[murillosezerino.com](https://murillosezerino.com) · [LinkedIn](https://linkedin.com/in/murillosezerino)
