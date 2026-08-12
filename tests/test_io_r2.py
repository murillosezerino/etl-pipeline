"""
test_io_r2.py — Testa a fronteira de I/O (boto3 / object storage) com moto,
sem credenciais reais. Cobre upload/leitura de CSV e Parquet, listagem e o
watermark, que antes eram o ponto cego dos testes.
"""

import boto3
import pandas as pd
from moto import mock_aws

from etl.extract import R2Extractor
from etl.load import R2Loader
from etl.state import WatermarkStore

REGION = "us-east-1"


def _df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["A1", "A2"],
        "status": ["entregue", "pendente"],
        "weight_kg": [1.5, 2.0],
    })


def _loader(bucket: str) -> R2Loader:
    return R2Loader(bucket=bucket, endpoint=None, access_key="x", secret_key="x", region=REGION)


def _extractor(bucket: str) -> R2Extractor:
    return R2Extractor(bucket=bucket, endpoint=None, access_key="x", secret_key="x", region=REGION)


@mock_aws
def test_csv_and_parquet_roundtrip_and_listing():
    boto3.client("s3", region_name=REGION).create_bucket(Bucket="raw")
    loader = _loader("raw")

    loader.upload_csv(_df(), "raw/deliveries/dt=2024-06-01/deliveries.csv")
    loader.upload_parquet(_df(), "processed/year=2024/month=06/day=01/part.parquet")

    extractor = _extractor("raw")

    keys = extractor.list_files("raw/deliveries/")
    assert "raw/deliveries/dt=2024-06-01/deliveries.csv" in keys

    csv_back = extractor.read_csv("raw/deliveries/dt=2024-06-01/deliveries.csv")
    assert len(csv_back) == 2
    assert set(csv_back["order_id"]) == {"A1", "A2"}

    pq_back = extractor.read_parquet("processed/year=2024/month=06/day=01/part.parquet")
    assert len(pq_back) == 2


@mock_aws
def test_watermark_store_read_write():
    boto3.client("s3", region_name=REGION).create_bucket(Bucket="proc")
    store = WatermarkStore(_loader("proc").s3, "proc")

    assert store.read() is None  # NoSuchKey -> primeira execucao (full load)

    store.write("2024-06-01T00:00:00+00:00")
    assert store.read() == "2024-06-01T00:00:00+00:00"
