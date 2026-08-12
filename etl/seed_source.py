"""
seed_source.py — Simula um sistema upstream aterrissando um LOTE DATADO de
entregas raw no R2, em raw/deliveries/dt=YYYY-MM-DD/deliveries.csv.

Desacopla o seeding do pipeline: o main.py apenas DESCOBRE e processa os lotes
(via list_files), sem gerar dado. Rode este script para criar um novo lote
antes de rodar o main. Assim o incremental por watermark é real entre lotes.
"""

import argparse
import logging
from datetime import date

from config.settings import (
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_RAW_BUCKET,
)
from etl.load import R2Loader
from etl.mock_data import generate_deliveries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)


def seed(batch_date: str, n: int = 12000) -> str:
    df = generate_deliveries(n)
    loader = R2Loader(
        bucket=R2_RAW_BUCKET, endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY,
    )
    key = f"raw/deliveries/dt={batch_date}/deliveries.csv"
    loader.upload_csv(df, key)
    logger.info(f"Lote aterrissado: {key} ({len(df):,} registros)")
    return key


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aterrissa um lote raw datado no R2.")
    parser.add_argument("--date", default=date.today().isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--n", type=int, default=12000)
    args = parser.parse_args()
    seed(args.date, args.n)
