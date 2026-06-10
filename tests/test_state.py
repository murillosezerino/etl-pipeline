"""
test_state.py — Testes da logica de carga incremental por watermark.
"""

import pandas as pd

from etl.state import filter_new, next_watermark


def _df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["A1", "A2", "A3"],
        "created_at": pd.to_datetime(
            ["2024-03-01", "2024-03-02", "2024-03-03"], utc=True
        ),
    })


def test_filter_new_without_watermark_returns_all():
    df = _df()
    out = filter_new(df, None)
    assert len(out) == 3


def test_filter_new_returns_only_newer():
    df = _df()
    out = filter_new(df, "2024-03-01T00:00:00+00:00")
    assert len(out) == 2
    assert set(out["order_id"]) == {"A2", "A3"}


def test_filter_new_all_old_returns_empty():
    df = _df()
    out = filter_new(df, "2024-03-10T00:00:00+00:00")
    assert out.empty


def test_next_watermark_returns_max():
    df = _df()
    mark = next_watermark(df)
    assert mark.startswith("2024-03-03")


def test_next_watermark_empty_is_none():
    df = _df().iloc[0:0]
    assert next_watermark(df) is None


def test_incremental_cycle():
    df = _df()
    mark = next_watermark(filter_new(df, None))
    second = filter_new(df, mark)
    assert second.empty
