# api.py (only the relevant diffs; keep your existing endpoints)
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from helpers import make_engine, make_session_factory
from models import Alert, Observation
from sqlalchemy import distinct, select
from stress import compute_stress

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


cfg = load_config()
engine = make_engine(cfg["app"]["db_url"])
SessionLocal = make_session_factory(engine)
SERIES_LABELS = {s["id"]: s.get("label", s["id"]) for s in cfg.get("series", [])}

app = FastAPI(title="NYFed Stress Dashboard", version="0.2.0")

# Serve /static/index.html, /static/app.js, etc.
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _parse_date(s: Optional[str], default: dt.date) -> dt.date:
    if not s:
        return default
    return dt.date.fromisoformat(s)


@app.get("/")
def dashboard():
    path = STATIC_DIR / "index.html"
    if not path.exists():
        raise HTTPException(status_code=500, detail="static/index.html not found")
    return FileResponse(str(path), media_type="text/html")


# ---- keep your existing JSON endpoints below (unchanged) ----


@app.get("/api/series")
def list_series():
    with SessionLocal() as session:
        stmt = select(distinct(Observation.series_id)).order_by(
            Observation.series_id.asc()
        )
        ids = [r[0] for r in session.execute(stmt).all()]
    return {
        "series_ids": ids,
        "series_labels": {sid: SERIES_LABELS.get(sid, sid) for sid in ids},
    }


@app.get("/api/series/{series_id}")
def get_series(
    series_id: str,
    start: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    today = dt.date.today()
    start_d = _parse_date(start, today - dt.timedelta(days=365))
    end_d = _parse_date(end, today)

    with SessionLocal() as session:
        stmt = (
            select(Observation.obs_date, Observation.value)
            .where(Observation.series_id == series_id)
            .where(Observation.obs_date >= start_d)
            .where(Observation.obs_date <= end_d)
            .order_by(Observation.obs_date.asc())
        )
        rows = session.execute(stmt).all()

    if not rows:
        raise HTTPException(status_code=404, detail="series not found or empty")

    return {
        "series_id": series_id,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "observations": [{"date": d.isoformat(), "value": float(v)} for d, v in rows],
    }


@app.get("/api/alerts")
def get_alerts(limit: int = Query(50, ge=1, le=500)):
    with SessionLocal() as session:
        stmt = select(Alert).order_by(Alert.alert_ts.desc()).limit(limit)
        rows = session.execute(stmt).scalars().all()
    return {
        "alerts": [
            {
                "alert_ts": a.alert_ts.isoformat(timespec="seconds"),
                "series_id": a.series_id,
                "series_label": SERIES_LABELS.get(a.series_id, a.series_id),
                "level": a.level,
                "message": a.message,
            }
            for a in rows
        ]
    }


@app.get("/api/stress/latest")
def latest_stress(lookback_days: int = Query(365, ge=30, le=5000)):
    weights = cfg.get("stress_score", {}).get("weights", {})
    today = dt.date.today()
    start = today - dt.timedelta(days=lookback_days + 10)

    results = []
    with SessionLocal() as session:
        for s in cfg["series"]:
            series_id = s["id"]
            triggers = s.get("triggers", {})

            stmt = (
                select(Observation.obs_date, Observation.value)
                .where(Observation.series_id == series_id)
                .where(Observation.obs_date >= start)
                .where(Observation.obs_date <= today)
                .order_by(Observation.obs_date.asc())
            )
            rows = session.execute(stmt).all()
            if len(rows) < 10:
                continue

            values = [float(v) for _, v in rows]
            res = compute_stress(
                series_id, values=values, triggers=triggers, weights=weights
            )
            results.append(
                {
                    "series_id": series_id,
                    "series_label": SERIES_LABELS.get(series_id, series_id),
                    "latest_value": res.latest_value,
                    "z": res.z,
                    "pctile": res.pctile,
                    "delta_7d_pct": res.delta_7d_pct,
                    "score": res.score,
                    "triggered": res.triggered,
                    "reasons": res.reasons,
                }
            )

    system_score = max([r["score"] for r in results], default=0.0)
    return {"asof": today.isoformat(), "system_score": system_score, "results": results}
