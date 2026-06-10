"""
state.py — Carga incremental por watermark.

A logica pura (filtrar novos, calcular proximo watermark) fica em funcoes
testaveis sem dependencia de rede. A persistencia do watermark no R2 fica
em WatermarkStore, usada pelo orquestrador.

Watermark = maior created_at ja processado. Numa execucao, processamos apenas
os registros mais novos que o watermark e, ao final, avancamos o marcador.
"""

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def filter_new(df: pd.DataFrame, last_watermark, ts_col: str = "created_at") -> pd.DataFrame:
    """Retorna apenas os registros com ts_col maior que o watermark anterior.

    Se last_watermark for None (primeira execucao), retorna o df inteiro.
    """
    if last_watermark is None:
        return df
    last = pd.to_datetime(last_watermark, utc=True)
    mask = df[ts_col] > last
    return df[mask].reset_index(drop=True)


def next_watermark(df: pd.DataFrame, ts_col: str = "created_at"):
    """Maior valor de ts_col no df, em ISO 8601. None se o df estiver vazio."""
    if df.empty:
        return None
    return df[ts_col].max().isoformat()


class WatermarkStore:
    """Le e grava o watermark como um pequeno JSON no R2 (S3-compativel)."""

    def __init__(self, s3_client, bucket: str, key: str = "_state/watermark.json"):
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key

    def read(self):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            data = json.loads(obj["Body"].read())
            return data.get("watermark")
        except self.s3.exceptions.NoSuchKey:
            logger.info("Nenhum watermark anterior. Primeira execucao (full load).")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Falha ao ler watermark, assumindo full load: {exc}")
            return None

    def write(self, watermark) -> None:
        if watermark is None:
            return
        body = json.dumps({"watermark": watermark}).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=body)
        logger.info(f"Watermark avancado para {watermark}")
