# plotter.py
from __future__ import annotations

import datetime as dt
from typing import List, Tuple

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def plot_series_with_bands(
    series_label: str,
    rows: List[Tuple[dt.date, float]],
    out_path: str,
    band_sigma: float = 2.0,
) -> str:
    dates = [d for d, _ in rows]
    vals = np.asarray([v for _, v in rows], dtype=float)

    base = vals[:-1] if vals.size > 1 else vals
    mu = float(np.nanmean(base))
    sd = float(np.nanstd(base, ddof=1)) if base.size > 2 else 0.0

    upper = mu + band_sigma * sd
    lower = mu - band_sigma * sd

    plt.figure()
    plt.plot(dates, vals, marker="o", linewidth=1)
    plt.axhline(mu, linestyle="--", linewidth=1)
    if sd > 0:
        plt.axhline(upper, linestyle=":", linewidth=1)
        plt.axhline(lower, linestyle=":", linewidth=1)

    # highlight last
    plt.scatter([dates[-1]], [vals[-1]], s=80)

    plt.title(series_label)
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path
