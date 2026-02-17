# stress.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np


@dataclass
class StressResult:
    series_id: str
    latest_value: float
    z: float
    pctile: float
    delta_7d_pct: float
    score: float
    triggered: bool
    reasons: List[str]


def _percentile_of_score(baseline: np.ndarray, x: float) -> float:
    if baseline.size == 0:
        return float("nan")
    return float((baseline <= x).mean())


def compute_stress(
    series_id: str,
    values: List[float],
    triggers: Dict,
    weights: Dict,
) -> StressResult:
    """
    values: ordered oldest->newest, includes baseline and latest.
    """
    arr = np.asarray(values, dtype=float)
    latest = float(arr[-1])

    base = arr[:-1] if arr.size > 1 else arr
    base = base[~np.isnan(base)]

    mu = float(np.nanmean(base)) if base.size else float("nan")
    sd = float(np.nanstd(base, ddof=1)) if base.size > 2 else float("nan")
    z = (latest - mu) / sd if sd and sd > 0 else float("nan")

    pctile = _percentile_of_score(base, latest)

    # 7d pct change (if enough points; assumes daily freq—works “okay” as a first pass)
    delta_7d_pct = float("nan")
    if arr.size >= 8 and arr[-8] != 0:
        delta_7d_pct = float((arr[-1] - arr[-8]) / abs(arr[-8]) * 100.0)

    # Convert into a 0..100-ish score
    z_component = (
        min(1.0, abs(z) / max(0.001, float(triggers.get("z_abs", 3.0))))
        if z == z
        else 0.0
    )
    pct_component = (
        0.0
        if pctile != pctile
        else max(
            0.0,
            (pctile - float(triggers.get("pctile", 0.95)))
            / (1.0 - float(triggers.get("pctile", 0.95))),
        )
    )
    delta_thr = float(triggers.get("delta_7d_pct", 50))
    delta_component = (
        0.0
        if delta_7d_pct != delta_7d_pct
        else min(1.0, abs(delta_7d_pct) / max(1e-6, delta_thr))
    )

    score = 100.0 * (
        float(weights.get("z_component", 0.6)) * z_component
        + float(weights.get("pctile_component", 0.2)) * pct_component
        + float(weights.get("delta_component", 0.2)) * delta_component
    )

    reasons: List[str] = []
    triggered = False

    if z == z and abs(z) >= float(triggers.get("z_abs", 3.0)):
        triggered = True
        reasons.append(f"|z|={z:.2f} ≥ {float(triggers.get('z_abs')):.2f}")

    if pctile == pctile and pctile >= float(triggers.get("pctile", 0.95)):
        triggered = True
        reasons.append(f"pctile={pctile:.3f} ≥ {float(triggers.get('pctile')):.3f}")

    if delta_7d_pct == delta_7d_pct and abs(delta_7d_pct) >= float(
        triggers.get("delta_7d_pct", 50)
    ):
        triggered = True
        reasons.append(
            f"|Δ7d|={delta_7d_pct:.1f}% ≥ {float(triggers.get('delta_7d_pct')):.1f}%"
        )

    return StressResult(
        series_id=series_id,
        latest_value=latest,
        z=z,
        pctile=pctile,
        delta_7d_pct=delta_7d_pct,
        score=score,
        triggered=triggered,
        reasons=reasons,
    )
