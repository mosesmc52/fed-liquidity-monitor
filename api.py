# api.py
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

import numpy as np
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response
from helpers import make_engine, make_session_factory
from models import Alert, Observation
from plotter import plot_series_with_bands
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session
from stress import compute_stress


def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


cfg = load_config()
engine = make_engine(cfg["app"]["db_url"])
SessionLocal = make_session_factory(engine)

app = FastAPI(title="NYFed Stress Dashboard", version="0.1.0")


def _parse_date(s: Optional[str], default: dt.date) -> dt.date:
    if not s:
        return default
    return dt.date.fromisoformat(s)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    # Minimal no-dependency dashboard (plain HTML+JS)
    return HTMLResponse(
        """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>NYFed Stress Dashboard</title>
    <style>
      body { font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial; margin: 24px; }
      .row { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
      .card { border: 1px solid #ddd; border-radius: 12px; padding: 12px 14px; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; font-size: 14px; }
      .bad { font-weight: 700; }
      img { max-width: 100%; border: 1px solid #eee; border-radius: 12px; }
      .muted { color: #666; font-size: 13px; }
    </style>
  </head>
  <body>
    <h2>NYFed Stress Dashboard</h2>
    <div class="row">
      <div class="card">
        <div class="muted">Series</div>
        <select id="seriesSelect"></select>
      </div>
      <div class="card">
        <div class="muted">Lookback (days)</div>
        <input id="lookback" type="number" value="365" min="30" max="5000"/>
      </div>
      <button id="refresh">Refresh</button>
    </div>

    <h3>Latest Stress</h3>
    <div class="card">
      <table id="stressTable">
        <thead>
          <tr>
            <th>Series</th><th>Latest</th><th>z</th><th>pctile</th><th>Δ7d%</th><th>score</th><th>triggered</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>

    <h3>Plot “proof”</h3>
    <div class="muted">Bands are baseline mean and ±2σ. Last point is highlighted.</div>
    <p><img id="plot" src="" alt="plot"/></p>

    <h3>Recent Alerts</h3>
    <div class="card">
      <table id="alertsTable">
        <thead><tr><th>Time</th><th>Series</th><th>Level</th><th>Message</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <script>
      async function getJSON(url) {
        const r = await fetch(url);
        if (!r.ok) throw new Error("HTTP " + r.status);
        return await r.json();
      }

      function fmt(x, d=2) {
        if (x === null || x === undefined) return "";
        if (Number.isNaN(x)) return "";
        return Number(x).toFixed(d);
      }

      async function loadSeriesList() {
        const series = await getJSON("/api/series");
        const sel = document.getElementById("seriesSelect");
        sel.innerHTML = "";
        for (const s of series.series_ids) {
          const opt = document.createElement("option");
          opt.value = s; opt.textContent = s;
          sel.appendChild(opt);
        }
      }

      async function loadStress() {
        const lookback = Number(document.getElementById("lookback").value || 365);
        const stress = await getJSON("/api/stress/latest?lookback_days=" + lookback);
        const tbody = document.querySelector("#stressTable tbody");
        tbody.innerHTML = "";
        for (const row of stress.results) {
          const tr = document.createElement("tr");
          if (row.triggered) tr.className = "bad";
          tr.innerHTML = `
            <td>${row.series_id}</td>
            <td>${fmt(row.latest_value, 4)}</td>
            <td>${fmt(row.z, 2)}</td>
            <td>${fmt(row.pctile, 3)}</td>
            <td>${fmt(row.delta_7d_pct, 1)}</td>
            <td>${fmt(row.score, 1)}</td>
            <td>${row.triggered ? "YES" : "no"}</td>
          `;
          tbody.appendChild(tr);
        }
      }

      async function loadPlot() {
        const seriesId = document.getElementById("seriesSelect").value;
        const lookback = Number(document.getElementById("lookback").value || 365);
        const img = document.getElementById("plot");
        img.src = `/api/plot/${encodeURIComponent(seriesId)}.png?lookback_days=${lookback}&_=${Date.now()}`;
      }

      async function loadAlerts() {
        const alerts = await getJSON("/api/alerts?limit=25");
        const tbody = document.querySelector("#alertsTable tbody");
        tbody.innerHTML = "";
        for (const a of alerts.alerts) {
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${a.alert_ts}</td><td>${a.series_id}</td><td>${a.level}</td><td>${a.message}</td>`;
          tbody.appendChild(tr);
        }
      }

      async function refreshAll() {
        await loadStress();
        await loadPlot();
        await loadAlerts();
      }

      document.getElementById("refresh").addEventListener("click", refreshAll);
      document.getElementById("seriesSelect").addEventListener("change", loadPlot);

      (async function init() {
        await loadSeriesList();
        await refreshAll();
      })();
    </script>
  </body>
</html>
        """
    )


@app.get("/api/series")
def list_series():
    with SessionLocal() as session:
        stmt = select(distinct(Observation.series_id)).order_by(
            Observation.series_id.asc()
        )
        ids = [r[0] for r in session.execute(stmt).all()]
    return {"series_ids": ids}


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
                "level": a.level,
                "message": a.message,
            }
            for a in rows
        ]
    }


@app.get("/api/stress/latest")
def latest_stress(lookback_days: int = Query(365, ge=30, le=5000)):
    # Compute “latest stress” on demand from DB using config thresholds.
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
                    "latest_value": res.latest_value,
                    "z": res.z,
                    "pctile": res.pctile,
                    "delta_7d_pct": res.delta_7d_pct,
                    "score": res.score,
                    "triggered": res.triggered,
                    "reasons": res.reasons,
                }
            )

    # Useful summary fields for the dashboard
    system_score = max([r["score"] for r in results], default=0.0)
    return {"asof": today.isoformat(), "system_score": system_score, "results": results}


@app.get("/api/plot/{series_id}.png")
def plot_series(series_id: str, lookback_days: int = Query(365, ge=30, le=5000)):
    today = dt.date.today()
    start = today - dt.timedelta(days=lookback_days + 10)

    with SessionLocal() as session:
        stmt = (
            select(Observation.obs_date, Observation.value)
            .where(Observation.series_id == series_id)
            .where(Observation.obs_date >= start)
            .where(Observation.obs_date <= today)
            .order_by(Observation.obs_date.asc())
        )
        rows = session.execute(stmt).all()

    if len(rows) < 10:
        raise HTTPException(status_code=404, detail="not enough data to plot")

    # Build plot in a temp file then return bytes
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, f"{series_id}.png")
        plot_series_with_bands(
            series_label=series_id, rows=[(d, float(v)) for d, v in rows], out_path=path
        )
        with open(path, "rb") as f:
            png = f.read()

    return Response(content=png, media_type="image/png")
