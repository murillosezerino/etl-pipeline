"""
load.py — Carga de dados transformados no Cloudflare R2 em formato Parquet.
"""

import logging
import boto3
import pandas as pd
from io import BytesIO

logger = logging.getLogger(__name__)


class R2Loader:
    """Carrega DataFrames no Cloudflare R2 como arquivos Parquet."""

    def __init__(self, bucket: str, endpoint: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def upload_parquet(self, df: pd.DataFrame, key: str) -> None:
        logger.info(f"Carregando {len(df):,} registros -> {self.bucket}/{key}")
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        buffer.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        logger.info("Upload concluido.")

    def upload_csv(self, df: pd.DataFrame, key: str) -> None:
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())

    @staticmethod
    def partitioned_key(base: str, year: int, month: int, day: int, filename: str) -> str:
        return f"{base}/year={year}/month={month:02d}/day={day:02d}/{filename}"