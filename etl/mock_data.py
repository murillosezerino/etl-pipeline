"""
mock_data.py — Gera dados mock realistas de entregas para teste do pipeline.
Cria 12.000 registros com cenarios de erro propositais (duplicatas, coordenadas invalidas, status variados).
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker("pt_BR")
np.random.seed(42)
random.seed(42)

# Cidades brasileiras com coordenadas reais
CIDADES = [
    {"cidade": "São Paulo",      "lat": -23.5505, "lng": -46.6333},
    {"cidade": "Rio de Janeiro", "lat": -22.9068, "lng": -43.1729},
    {"cidade": "Belo Horizonte", "lat": -19.9191, "lng": -43.9386},
    {"cidade": "Curitiba",       "lat": -25.4290, "lng": -49.2671},
    {"cidade": "Porto Alegre",   "lat": -30.0346, "lng": -51.2177},
    {"cidade": "Salvador",       "lat": -12.9714, "lng": -38.5014},
    {"cidade": "Fortaleza",      "lat": -3.7172,  "lng": -38.5433},
    {"cidade": "Manaus",         "lat": -3.1190,  "lng": -60.0217},
]

STATUS_PT = ["entregue", "cancelado", "em_transito", "pendente", "devolvido"]
STATUS_WEIGHTS = [0.70, 0.10, 0.10, 0.07, 0.03]


def _random_coord(base: float, spread: float = 0.5) -> float:
    return base + np.random.uniform(-spread, spread)


def generate_deliveries(n: int = 12000) -> pd.DataFrame:
    records = []

    for i in range(n):
        origin = random.choice(CIDADES)
        dest   = random.choice(CIDADES)

        created = fake.date_time_between(start_date="-6m", end_date="now")
        status  = random.choices(STATUS_PT, weights=STATUS_WEIGHTS)[0]

        if status == "entregue":
            delivered = created + timedelta(hours=random.uniform(1, 48))
        elif status == "devolvido":
            delivered = created + timedelta(hours=random.uniform(24, 96))
        else:
            delivered = None

        records.append({
            "order_id":    f"ORD-{i+1:06d}",
            "driver_id":   f"DRV-{random.randint(1, 500):04d}",
            "status":      status,
            "created_at":  created,
            "delivered_at": delivered,
            "origin_city": origin["cidade"],
            "origin_lat":  _random_coord(origin["lat"]),
            "origin_lng":  _random_coord(origin["lng"]),
            "dest_city":   dest["cidade"],
            "dest_lat":    _random_coord(dest["lat"]),
            "dest_lng":    _random_coord(dest["lng"]),
            "weight_kg":   round(random.uniform(0.1, 30.0), 2),
        })

    df = pd.DataFrame(records)

    # Injetar duplicatas propositais (2% dos registros)
    n_dupes = int(n * 0.02)
    dupes = df.sample(n_dupes).copy()
    df = pd.concat([df, dupes], ignore_index=True)

    # Injetar coordenadas invalidas (1% dos registros)
    n_invalid = int(n * 0.01)
    idx_invalid = df.sample(n_invalid).index
    df.loc[idx_invalid, "origin_lat"] = 999.0

    print(f"Mock gerado: {len(df):,} registros ({n_dupes} duplicatas, {n_invalid} coords invalidas)")
    return df


if __name__ == "__main__":
    df = generate_deliveries(12000)
    df.to_csv("mock_deliveries.csv", index=False)
    print("Arquivo salvo: mock_deliveries.csv")