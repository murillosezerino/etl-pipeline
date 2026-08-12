"""
main.py — Pipeline ETL com Cloudflare R2 (seeding DESACOPLADO em seed_source.py).

Fluxo:
  descobre lotes raw (list_files) -> extract -> transform -> gate de qualidade
  -> filtro incremental (watermark) -> load particionado (Parquet)
  -> verify read-after-write (read_parquet) -> avanca watermark

Rode primeiro:  python -m etl.seed_source     (aterrissa um lote datado no raw)
Depois:         python main.py                 (processa os lotes ainda nao vistos)
"""

import logging
import re

import pandas as pd

from config.settings import (
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_RAW_BUCKET, R2_PROCESSED_BUCKET, LOG_LEVEL,
)
from etl.extract import R2Extractor
from etl.transform import DeliveryTransformer
from etl.load import R2Loader
from etl.quality import DeliveryQualityChecker
from etl.state import WatermarkStore, filter_new, next_watermark

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
    logger.info("Pipeline ETL - Cloudflare R2")
    logger.info("=" * 55)

    extractor = R2Extractor(
        bucket=R2_RAW_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )

    # 1. DESCOBRIR lotes raw datados (raw/deliveries/dt=YYYY-MM-DD/...)
    logger.info("[1] Descobrindo lotes raw no R2...")
    keys = [k for k in extractor.list_files("raw/deliveries/") if k.endswith(".csv")]
    if not keys:
        logger.info("Nenhum lote raw encontrado. Rode: python -m etl.seed_source")
        return
    logger.info(f"  {len(keys)} lote(s) encontrado(s).")

    # 2. EXTRACT + concat de todos os lotes descobertos
    df_raw = pd.concat([extractor.read_csv(k) for k in keys], ignore_index=True)
    logger.info(f"[2] Extraido: {len(df_raw):,} registros de {len(keys)} lote(s)")

    # 3. TRANSFORM
    df_clean = DeliveryTransformer.run_all(df_raw)

    # 4. GATE DE QUALIDADE
    report = DeliveryQualityChecker(df_clean).run_all()
    report.raise_for_status()  # para o pipeline se algo critico falhou
    logger.info("[4] Qualidade aprovada.")

    # 5. CARGA INCREMENTAL (watermark)
    loader = R2Loader(
        bucket=R2_PROCESSED_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    store = WatermarkStore(loader.s3, R2_PROCESSED_BUCKET)
    last = store.read()
    df_new = filter_new(df_clean, last, ts_col="created_at")
    logger.info(f"[5] Watermark anterior: {last} | novos registros: {len(df_new):,}")

    if df_new.empty:
        logger.info("Nada novo para carregar. Pipeline encerrado (idempotente).")
        return

    new_mark = next_watermark(df_new, ts_col="created_at")
    tag = _batch_tag(new_mark)

    written = []
    for (year, month, day), group in df_new.groupby(["year", "month", "day"]):
        key = R2Loader.partitioned_key(
            "deliveries", int(year), int(month), int(day), f"part-{tag}.parquet"
        )
        loader.upload_parquet(group.drop(columns=["year", "month", "day"]), key)
        written.append((key, len(group)))

    # 6. VERIFY read-after-write: relê uma particao gravada e confere a contagem
    verifier = R2Extractor(
        bucket=R2_PROCESSED_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    key0, n0 = written[0]
    check = verifier.read_parquet(key0)
    if len(check) != n0:
        raise RuntimeError(f"Verificacao falhou em {key0}: {len(check)} != {n0}")
    logger.info(f"[6] Verify OK: {key0} relido com {len(check):,} registros")

    # 7. AVANCAR WATERMARK
    store.write(new_mark)

    logger.info("=" * 55)
    logger.info(f"Concluido: {len(df_new):,} novos registros em {len(written)} particoes")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()
