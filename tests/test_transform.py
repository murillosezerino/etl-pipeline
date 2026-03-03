"""
test_transform.py — Testes unitarios das transformacoes do pipeline.
"""

import pytest
import pandas as pd
import numpy as np
from etl.transform import DeliveryTransformer


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id":    ["A1", "A2", "A3", "A1"],
        "driver_id":   ["D1", "D2", "D3", "D1"],
        "status":      ["entregue", "cancelado", "em_transito", "entregue"],
        "created_at":  ["2024-03-01 08:00:00", "2024-03-01 09:00:00",
                        "2024-03-02 10:00:00", "2024-03-01 08:00:00"],
        "delivered_at": ["2024-03-01 10:00:00", None,
                         "2024-03-02 14:00:00", "2024-03-01 10:00:00"],
        "origin_lat":  [-23.5, -23.6, -23.7, -23.5],
        "origin_lng":  [-46.6, -46.7, -46.8, -46.6],
        "dest_lat":    [-23.6, -23.7, -23.8, -23.6],
        "dest_lng":    [-46.7, -46.8, -46.9, -46.7],
        "weight_kg":   [1.5, 2.0, 0.8, 1.5],
    })


def test_remove_duplicates(sample_df):
    t = DeliveryTransformer(sample_df).remove_duplicates()
    assert len(t.df) == 3
    assert t.df["order_id"].nunique() == 3


def test_normalize_status(sample_df):
    t = DeliveryTransformer(sample_df).normalize_status()
    valid = {"delivered", "cancelled", "in_transit", "pending", "returned", "unknown"}
    assert set(t.df["status"].unique()).issubset(valid)


def test_parse_dates(sample_df):
    t = DeliveryTransformer(sample_df).parse_dates()
    assert pd.api.types.is_datetime64_any_dtype(t.df["created_at"])
    assert pd.api.types.is_datetime64_any_dtype(t.df["delivered_at"])


def test_add_lead_time(sample_df):
    t = (DeliveryTransformer(sample_df)
         .remove_duplicates()
         .parse_dates()
         .add_lead_time())
    row = t.df[t.df["order_id"] == "A1"].iloc[0]
    assert row["lead_time_hours"] == pytest.approx(2.0, abs=0.01)


def test_add_distance_km(sample_df):
    t = (DeliveryTransformer(sample_df)
         .remove_duplicates()
         .parse_dates()
         .filter_valid_coordinates()
         .add_distance_km())
    assert (t.df["distance_km"] > 0).all()


def test_add_partition_columns(sample_df):
    t = (DeliveryTransformer(sample_df)
         .remove_duplicates()
         .parse_dates()
         .add_partition_columns())
    assert {"year", "month", "day"}.issubset(t.df.columns)


def test_run_all(sample_df):
    df = DeliveryTransformer.run_all(sample_df)
    assert len(df) == 3
    assert "lead_time_hours" in df.columns
    assert "distance_km" in df.columns
    assert "year" in df.columns