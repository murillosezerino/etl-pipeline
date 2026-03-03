"""
transform.py — Transformacoes e regras de negocio sobre dados de entregas.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DeliveryTransformer:
    """
    Pipeline de transformacao de registros de entrega.

    Uso rapido:
        df_clean = DeliveryTransformer.run_all(df_raw)
    """

    STATUS_MAP = {
        "entregue":    "delivered",
        "cancelado":   "cancelled",
        "em_transito": "in_transit",
        "pendente":    "pending",
        "devolvido":   "returned",
    }

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        logger.info(f"Transformer iniciado com {len(self.df):,} registros")

    def remove_duplicates(self) -> "DeliveryTransformer":
        before = len(self.df)
        self.df.drop_duplicates(subset=["order_id"], keep="last", inplace=True)
        removed = before - len(self.df)
        if removed:
            logger.warning(f"{removed:,} duplicatas removidas")
        return self

    def normalize_status(self) -> "DeliveryTransformer":
        self.df["status"] = (
            self.df["status"]
            .str.strip()
            .str.lower()
            .map(self.STATUS_MAP)
            .fillna("unknown")
        )
        return self

    def parse_dates(self) -> "DeliveryTransformer":
        for col in ["created_at", "delivered_at"]:
            self.df[col] = pd.to_datetime(self.df[col], errors="coerce", utc=True)
        return self

    def add_lead_time(self) -> "DeliveryTransformer":
        self.df["lead_time_hours"] = (
            (self.df["delivered_at"] - self.df["created_at"])
            .dt.total_seconds() / 3600
        )
        return self

    def filter_valid_coordinates(self) -> "DeliveryTransformer":
        mask = (
            self.df["origin_lat"].between(-90, 90)
            & self.df["origin_lng"].between(-180, 180)
            & self.df["dest_lat"].between(-90, 90)
            & self.df["dest_lng"].between(-180, 180)
        )
        invalid = (~mask).sum()
        if invalid:
            logger.warning(f"{invalid:,} registros com coordenadas invalidas removidos")
        self.df = self.df[mask].reset_index(drop=True)
        return self

    def add_distance_km(self) -> "DeliveryTransformer":
        R = 6371
        lat1 = np.radians(self.df["origin_lat"])
        lat2 = np.radians(self.df["dest_lat"])
        dlat = lat2 - lat1
        dlng = np.radians(self.df["dest_lng"] - self.df["origin_lng"])
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2) ** 2
        self.df["distance_km"] = R * 2 * np.arcsin(np.sqrt(a))
        return self

    def add_partition_columns(self) -> "DeliveryTransformer":
        self.df["year"]  = self.df["created_at"].dt.year
        self.df["month"] = self.df["created_at"].dt.month
        self.df["day"]   = self.df["created_at"].dt.day
        return self

    def build(self) -> pd.DataFrame:
        logger.info(f"Transformacao concluida: {len(self.df):,} registros finais")
        return self.df

    @classmethod
    def run_all(cls, df: pd.DataFrame) -> pd.DataFrame:
        return (
            cls(df)
            .remove_duplicates()
            .normalize_status()
            .parse_dates()
            .add_lead_time()
            .filter_valid_coordinates()
            .add_distance_km()
            .add_partition_columns()
            .build()
        )