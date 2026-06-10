"""
test_quality.py — Testes da camada de qualidade de dados.
"""

import pandas as pd
import pytest

from etl.quality import DeliveryQualityChecker, DataQualityError


def _good_df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id":   ["A1", "A2", "A3"],
        "status":     ["delivered", "cancelled", "in_transit"],
        "created_at": pd.to_datetime(
            ["2024-03-01", "2024-03-02", "2024-03-03"], utc=True
        ),
        "origin_lat": [-23.5, -23.6, -23.7],
        "dest_lat":   [-23.6, -23.7, -23.8],
        "distance_km": [10.0, 20.0, 5.0],
        "lead_time_hours": [2.0, None, None],
        "year":  [2024, 2024, 2024],
        "month": [3, 3, 3],
        "day":   [1, 2, 3],
    })


def test_good_data_passes():
    report = DeliveryQualityChecker(_good_df()).run_all()
    assert report.passed
    assert report.failures == []


def test_empty_data_fails():
    report = DeliveryQualityChecker(_good_df().iloc[0:0]).run_all()
    assert not report.passed
    assert any(f.name == "not_empty" for f in report.failures)


def test_duplicate_order_id_fails():
    df = _good_df()
    df.loc[2, "order_id"] = "A1"
    report = DeliveryQualityChecker(df).run_all()
    assert any(f.name == "unique_order_id" for f in report.failures)


def test_invalid_status_fails():
    df = _good_df()
    df.loc[0, "status"] = "teleported"
    report = DeliveryQualityChecker(df).run_all()
    assert any(f.name == "status_domain" for f in report.failures)


def test_negative_distance_fails():
    df = _good_df()
    df.loc[0, "distance_km"] = -1.0
    report = DeliveryQualityChecker(df).run_all()
    assert any(f.name == "distance_non_negative" for f in report.failures)


def test_null_in_critical_column_fails():
    df = _good_df()
    df.loc[1, "distance_km"] = None
    report = DeliveryQualityChecker(df).run_all()
    assert any(f.name == "no_nulls_critical" for f in report.failures)


def test_lead_time_out_of_bounds_fails():
    df = _good_df()
    df.loc[0, "lead_time_hours"] = -5.0
    report = DeliveryQualityChecker(df).run_all()
    assert any(f.name == "lead_time_bounds" for f in report.failures)


def test_raise_for_status_raises():
    report = DeliveryQualityChecker(_good_df().iloc[0:0]).run_all()
    with pytest.raises(DataQualityError):
        report.raise_for_status()
