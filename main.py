"""
main.py — Orquestrador do pipeline ETL com Cloudflare R2.

Fluxo:
  gera mock -> upload raw -> extract -> transform -> gate de qualidade
  -> filtro incremental (watermark) -> load particionado (Parquet) -> avanca watermark
"""

import logging
import re

from config.settings import (
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_RAW_BUCKET, R2_PROCESSED_BUCKET, LOG_LEVEL,
)
from etl.extract import R2Extractor
from etl.transform import DeliveryTransformer
from etl.load import R2Loader
from etl.quality import DeliveryQualityChecker
from etl.state import WatermarkStore, filter_new, next_watermark
from etl.mock_data import generate_deliveries

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _batch_tag(watermark: str) -> str:
    """Transforma um watermark ISO em um sufixo de arquivo seguro."""
    return re.sub(r"[^0-9]", "", watermark)[:14] or "full"


def run() -> None:
    logger.info("=" * 55)
    logger.info("Iniciando pipeline ETL - Cloudflare R2")
    logger.info("=" * 55)

    # 0. GERAR MOCK
    logger.info("[0] Gerando dados mock...")
    df_mock = generate_deliveries(12000)

    # 1. UPLOAD RAW
    logger.info("[1] Upload dos dados raw para R2...")
    loader_raw = R2Loader(
        bucket=R2_RAW_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    loader_raw.upload_csv(df_mock, "raw/deliveries/latest.csv")

    # 2. EXTRACT
    logger.info("[2] Extraindo dados do R2...")
    extractor = R2Extractor(
        bucket=R2_RAW_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    df_raw = extractor.read_csv("raw/deliveries/latest.csv")
    logger.info(f"  Extraido: {len(df_raw):,} registros")

    # 3. TRANSFORM
    logger.info("[3] Transformando dados...")
    df_clean = DeliveryTransformer.run_all(df_raw)

    # 4. GATE DE QUALIDADE
    logger.info("[4] Rodando checagens de qualidade...")
    report = DeliveryQualityChecker(df_clean).run_all()
    report.raise_for_status()  # para o pipeline se algo critico falhou
    logger.info("  Qualidade aprovada.")

    # 5. CARGA INCREMENTAL
    logger.info("[5] Carga incremental no R2...")
    loader_processed = R2Loader(
        bucket=R2_PROCESSED_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    store = WatermarkStore(loader_processed.s3, R2_PROCESSED_BUCKET)
    last = store.read()
    df_new = filter_new(df_clean, last, ts_col="created_at")
    logger.info(f"  Watermark anterior: {last} | novos registros: {len(df_new):,}")

    if df_new.empty:
        logger.info("  Nada novo para carregar. Pipeline encerrado.")
        return

    new_mark = next_watermark(df_new, ts_col="created_at")
    tag = _batch_tag(new_mark)

    partitions = df_new.groupby(["year", "month", "day"])
    for (year, month, day), group in partitions:
        key = R2Loader.partitioned_key(
            "deliveries", int(year), int(month), int(day), f"part-{tag}.parquet"
        )
        loader_processed.upload_parquet(
            group.drop(columns=["year", "month", "day"]), key
        )

    # 6. AVANCAR WATERMARK
    store.write(new_mark)

    logger.info("=" * 55)
    logger.info(f"Pipeline concluido: {len(df_new):,} novos registros em {partitions.ngroups} particoes")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()
