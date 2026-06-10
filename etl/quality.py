"""
quality.py — Camada de qualidade de dados.

Roda um conjunto de checagens sobre o DataFrame transformado antes da carga.
Funciona como um gate: se uma checagem critica falha, o pipeline para em vez
de gravar dado ruim no destino.

Uso rapido:
    report = DeliveryQualityChecker(df_clean).run_all()
    report.raise_for_status()   # levanta DataQualityError se algo critico falhou
"""

import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Levantada quando uma ou mais checagens criticas falham."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class DataQualityReport:
    results: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failures(self) -> list:
        return [r for r in self.results if not r.passed]

    def raise_for_status(self) -> None:
        if not self.passed:
            lines = "; ".join(f"{r.name}: {r.detail}" for r in self.failures)
            raise DataQualityError(f"{len(self.failures)} checagem(ns) falharam -> {lines}")

    def log(self) -> None:
        for r in self.results:
            mark = "OK " if r.passed else "FALHOU"
            logger.info(f"[{mark}] {r.name} {('- ' + r.detail) if r.detail else ''}")


class DeliveryQualityChecker:
    """Checagens de qualidade para o dataset de entregas ja transformado."""

    CRITICAL_COLUMNS = [
        "order_id", "status", "created_at",
        "origin_lat", "dest_lat", "distance_km",
        "year", "month", "day",
    ]
    ALLOWED_STATUS = {
        "delivered", "cancelled", "in_transit", "pending", "returned", "unknown",
    }
    MAX_LEAD_TIME_HOURS = 24 * 30  # 30 dias e um teto sensato para entrega

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results: list = []

    def _add(self, name: str, passed: bool, detail: str = "") -> None:
        self.results.append(CheckResult(name, passed, detail))

    def check_not_empty(self) -> "DeliveryQualityChecker":
        n = len(self.df)
        self._add("not_empty", n > 0, f"{n} linhas")
        return self

    def check_required_columns(self) -> "DeliveryQualityChecker":
        missing = [c for c in self.CRITICAL_COLUMNS if c not in self.df.columns]
        self._add("required_columns", not missing, f"faltando: {missing}" if missing else "todas presentes")
        return self

    def check_unique_order_id(self) -> "DeliveryQualityChecker":
        if "order_id" not in self.df.columns:
            self._add("unique_order_id", False, "coluna ausente")
            return self
        dupes = int(self.df["order_id"].duplicated().sum())
        self._add("unique_order_id", dupes == 0, f"{dupes} duplicatas")
        return self

    def check_no_nulls_critical(self) -> "DeliveryQualityChecker":
        cols = [c for c in self.CRITICAL_COLUMNS if c in self.df.columns]
        nulls = self.df[cols].isnull().sum()
        offenders = nulls[nulls > 0].to_dict()
        self._add("no_nulls_critical", not offenders, f"nulos: {offenders}" if offenders else "sem nulos")
        return self

    def check_status_domain(self) -> "DeliveryQualityChecker":
        if "status" not in self.df.columns:
            self._add("status_domain", False, "coluna ausente")
            return self
        invalid = set(self.df["status"].dropna().unique()) - self.ALLOWED_STATUS
        self._add("status_domain", not invalid, f"fora do dominio: {invalid}" if invalid else "ok")
        return self

    def check_distance_non_negative(self) -> "DeliveryQualityChecker":
        if "distance_km" not in self.df.columns:
            self._add("distance_non_negative", False, "coluna ausente")
            return self
        bad = int((self.df["distance_km"] < 0).sum())
        self._add("distance_non_negative", bad == 0, f"{bad} negativas")
        return self

    def check_lead_time_bounds(self) -> "DeliveryQualityChecker":
        if "lead_time_hours" not in self.df.columns:
            self._add("lead_time_bounds", True, "coluna ausente, ignorado")
            return self
        s = self.df["lead_time_hours"].dropna()
        bad = int(((s < 0) | (s > self.MAX_LEAD_TIME_HOURS)).sum())
        self._add("lead_time_bounds", bad == 0, f"{bad} fora de [0, {self.MAX_LEAD_TIME_HOURS}]")
        return self

    def run_all(self) -> DataQualityReport:
        (self.check_not_empty()
             .check_required_columns()
             .check_unique_order_id()
             .check_no_nulls_critical()
             .check_status_domain()
             .check_distance_non_negative()
             .check_lead_time_bounds())
        report = DataQualityReport(self.results)
        report.log()
        return report
