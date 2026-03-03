"""
extract.py — Extracao de dados do Cloudflare R2 (S3-compativel).
"""

import logging
import pandas as pd
import boto3
from io import BytesIO

logger = logging.getLogger(__name__)


class R2Extractor:
    """Le arquivos CSV ou Parquet do Cloudflare R2."""

    def __init__(self, bucket: str, endpoint: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def read_csv(self, key: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Extraindo CSV: {self.bucket}/{key}")
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj["Body"], **kwargs)

    def read_parquet(self, key: str) -> pd.DataFrame:
        logger.info(f"Extraindo Parquet: {self.bucket}/{key}")
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))

    def list_files(self, prefix: str = "") -> list:
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [item["Key"] for item in response.get("Contents", [])]