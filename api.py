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
SERIES_LABELS = {s["id"]: s.get("label", s["id"]) for s in cfg.get("series", [])}

app = FastAPI(title="NYFed Stress Dashboard", version="0.1.0")


def _parse_date(s: Optional[str], default: dt.date) -> dt.date:
    if not s:
        return default
    return dt.date.fromisoformat(s)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Liquidity Stress Monitor</title>
    <style>
      :root {
        --bg: #f4f6ef;
        --panel: #ffffff;
        --ink: #1f2933;
        --muted: #5f6c7b;
        --line: #d8e0d1;
        --accent: #006d5b;
        --accent-soft: #d9efe9;
        --warn: #9a3412;
        --warn-soft: #fee2d5;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background:
          radial-gradient(circle at 20% 0%, #ebf4e4 0, transparent 42%),
          radial-gradient(circle at 95% 10%, #dcedf7 0, transparent 38%),
          var(--bg);
        color: var(--ink);
        font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
      }
      .wrap { max-width: 1100px; margin: 0 auto; padding: 20px 16px 40px; }
      .hero {
        background: linear-gradient(120deg, #1f5f52 0%, #285b79 100%);
        color: #fff;
        border-radius: 18px;
        padding: 20px;
        margin-bottom: 16px;
      }
      .hero h1 { margin: 0 0 6px; font-size: 26px; line-height: 1.2; }
      .hero p { margin: 0; opacity: 0.92; font-size: 14px; }
      .grid { display: grid; gap: 12px; grid-template-columns: repeat(12, 1fr); }
      .card {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px;
      }
      .controls { grid-column: span 12; display: grid; gap: 10px; grid-template-columns: 1fr 1fr auto; align-items: end; }
      .kpi { grid-column: span 6; }
      .plot { grid-column: span 8; }
      .summary { grid-column: span 4; }
      .table-card { grid-column: span 12; }
      label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 6px; }
      select, input, button {
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 10px 11px;
        font-size: 14px;
      }
      button {
        width: auto;
        min-width: 120px;
        background: var(--accent);
        color: #fff;
        border: 0;
        cursor: pointer;
      }
      button:hover { filter: brightness(1.05); }
      .kpi .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; }
      .kpi .value { font-size: 28px; font-weight: 650; }
      .pill {
        display: inline-block;
        margin-top: 8px;
        padding: 4px 9px;
        border-radius: 999px;
        font-size: 12px;
      }
      .ok { background: var(--accent-soft); color: #005546; }
      .warn { background: var(--warn-soft); color: var(--warn); }
      .section-title { margin: 0 0 8px; font-size: 16px; }
      .muted { color: var(--muted); font-size: 13px; }
      .plot-frame {
        min-height: 250px;
        border: 1px solid var(--line);
        border-radius: 12px;
        overflow: hidden;
        background: #fbfdf9;
      }
      #plot { width: 100%; height: 100%; object-fit: contain; display: block; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; border-bottom: 1px solid #edf1e8; padding: 10px 6px; font-size: 13px; vertical-align: top; }
      th { color: var(--muted); font-weight: 600; }
      tr.alert-row td { background: #fff9f7; }
      #globalError { margin-top: 8px; color: var(--warn); font-size: 13px; min-height: 18px; }
      @media (max-width: 900px) {
        .kpi, .plot, .summary, .table-card { grid-column: span 12; }
        .controls { grid-template-columns: 1fr; }
        button { width: 100%; }
      }
    </style>
  </head>
  <body>
    <main class="wrap">
      <section class="hero">
        <h1>NY Fed Liquidity Stress Monitor</h1>
        <p>A plain-language view of funding market pressure indicators and recent alerts.</p>
      </section>

      <section class="grid">
        <div class="card controls">
          <div>
            <label for="seriesSelect">Indicator</label>
            <select id="seriesSelect"></select>
          </div>
          <div>
            <label for="lookback">History window (days)</label>
            <input id="lookback" type="number" value="365" min="30" max="5000"/>
          </div>
          <button id="refresh">Update View</button>
          <div id="globalError"></div>
        </div>

        <article class="card kpi">
          <div class="label">Date</div>
          <div class="value" id="asOf">-</div>
          <span class="pill ok" id="statusPill">No active signal</span>
        </article>

        <article class="card kpi">
          <div class="label">System Stress Score</div>
          <div class="value" id="systemScore">-</div>
          <span class="pill ok" id="scorePill">Normal range</span>
        </article>

        <article class="card summary">
          <h2 class="section-title" id="explainerTitle">Indicator Explainer</h2>
          <p class="muted"><strong>What it is:</strong> <span id="explainerWhat">Select an indicator to see a plain-English explanation.</span></p>
          <p class="muted"><strong>Why it matters:</strong> <span id="explainerWhy">This helps translate market data into funding stress context.</span></p>
          <p class="muted"><strong>When it's a danger:</strong> <span id="explainerDanger">Watch for sharp jumps, persistent extremes, and repeated alerts.</span></p>
        </article>

        <article class="card plot">
          <h2 class="section-title">Selected Indicator Trend</h2>
          <p class="muted">Center line is average, dotted bands are typical range. Last dot is newest value.</p>
          <div class="plot-frame">
            <img id="plot" src="" alt="Selected indicator chart"/>
          </div>
        </article>

        <article class="card table-card">
          <h2 class="section-title">Latest Indicator Readings</h2>
          <table id="stressTable">
            <thead>
              <tr>
                <th>Indicator</th><th>Latest</th><th>Z-score</th><th>Percentile</th><th>7d change %</th><th>Score</th><th>Status</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </article>

        <article class="card table-card">
          <h2 class="section-title">Recent Alerts</h2>
          <table id="alertsTable">
            <thead><tr><th>Time</th><th>Indicator</th><th>Level</th><th>Message</th></tr></thead>
            <tbody></tbody>
          </table>
        </article>
      </section>
    </main>

    <script>
      async function getJSON(url) {
        const r = await fetch(url);
        if (!r.ok) throw new Error("HTTP " + r.status);
        return await r.json();
      }

      function fmt(x, d = 2, compact = false) {
        if (x === null || x === undefined || Number.isNaN(x)) return "";
        const n = Number(x);
        if (!Number.isFinite(n)) return "";
        const abs = Math.abs(n);

        if (compact && abs >= 1_000_000_000) {
          return `${(n / 1_000_000_000).toLocaleString("en-US", {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
          })}B`;
        }
        if (compact && abs >= 1_000_000) {
          return `${(n / 1_000_000).toLocaleString("en-US", {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
          })}M`;
        }

        return n.toLocaleString("en-US", {
          minimumFractionDigits: d,
          maximumFractionDigits: d
        });
      }

      function setError(msg = "") {
        document.getElementById("globalError").textContent = msg;
      }

      function fmtDate(value) {
        if (!value) return "-";
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return value;
        return d.toLocaleDateString("en-US", {
          month: "long",
          day: "numeric",
          year: "numeric"
        });
      }

      const EXPLAINERS = {
        repo_ops_total: {
          title: "Total Repo Operations",
          what: "Think of repo as short-term cash lending using safe collateral. This indicator tracks how much cash is being taken up in those operations.",
          why: "It gives an early read on day-to-day funding pressure. Rising usage can mean institutions need more liquidity support.",
          danger: "Concern rises when usage jumps quickly, stays elevated for days, or spikes alongside other stress indicators."
        },
        sofr: {
          title: "Secured Overnight Financing Rate (SOFR)",
          what: "SOFR is the broad overnight borrowing rate backed by Treasury collateral. It is a core benchmark for U.S. dollar funding.",
          why: "Because so many loans and derivatives reference SOFR, sudden moves can signal wider funding strain and affect borrowing costs quickly.",
          danger: "Concern rises when SOFR moves abruptly versus recent norms, especially if volatility persists or diverges from policy expectations."
        },
        usd_swaps_outstanding: {
          title: "USD Liquidity Swaps Outstanding",
          what: "This reflects dollar liquidity swap usage between central banks and the Fed to ease offshore dollar funding pressure.",
          why: "It helps show whether global institutions are struggling to access dollars in private markets.",
          danger: "Concern rises when outstanding usage ramps up fast and remains high, suggesting persistent cross-border dollar stress."
        }
      };

      function updateExplainer(seriesId) {
        const info = EXPLAINERS[seriesId] || {
          title: "Indicator Explainer",
          what: "This indicator tracks a funding condition linked to market liquidity.",
          why: "It helps show whether short-term financing conditions are stable or tightening.",
          danger: "Concern rises when values move sharply and stay far from their recent range."
        };
        document.getElementById("explainerTitle").textContent = info.title;
        document.getElementById("explainerWhat").textContent = info.what;
        document.getElementById("explainerWhy").textContent = info.why;
        document.getElementById("explainerDanger").textContent = info.danger;
      }

      function setKPIs(stress) {
        document.getElementById("asOf").textContent = fmtDate(stress.asof);
        document.getElementById("systemScore").textContent = fmt(stress.system_score, 1);

        const hasSignal = stress.results.some(r => r.triggered);
        const statusPill = document.getElementById("statusPill");
        const scorePill = document.getElementById("scorePill");
        if (hasSignal) {
          statusPill.textContent = "Active signal";
          statusPill.className = "pill warn";
          scorePill.textContent = "Watch closely";
          scorePill.className = "pill warn";
        } else {
          statusPill.textContent = "No active signal";
          statusPill.className = "pill ok";
          scorePill.textContent = "Normal range";
          scorePill.className = "pill ok";
        }
      }

      async function loadSeriesList() {
        const series = await getJSON("/api/series");
        const sel = document.getElementById("seriesSelect");
        const labels = series.series_labels || {};
        sel.innerHTML = "";
        for (const s of series.series_ids) {
          const opt = document.createElement("option");
          opt.value = s;
          opt.textContent = labels[s] || s;
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
          if (row.triggered) tr.className = "alert-row";
          tr.innerHTML = `
            <td>${row.series_label || row.series_id}</td>
            <td>${fmt(row.latest_value, 4, true)}</td>
            <td>${fmt(row.z, 2)}</td>
            <td>${fmt(row.pctile, 3)}</td>
            <td>${fmt(row.delta_7d_pct, 1)}</td>
            <td>${fmt(row.score, 1)}</td>
            <td>${row.triggered ? "Alert" : "Normal"}</td>
          `;
          tbody.appendChild(tr);
        }
        setKPIs(stress);
      }

      async function loadPlot() {
        const sel = document.getElementById("seriesSelect");
        if (!sel.value) return;
        updateExplainer(sel.value);
        const lookback = Number(document.getElementById("lookback").value || 365);
        const img = document.getElementById("plot");
        img.src = `/api/plot/${encodeURIComponent(sel.value)}.png?lookback_days=${lookback}&_=${Date.now()}`;
      }

      async function loadAlerts() {
        const alerts = await getJSON("/api/alerts?limit=25");
        const tbody = document.querySelector("#alertsTable tbody");
        tbody.innerHTML = "";
        for (const a of alerts.alerts) {
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${fmtDate(a.alert_ts)}</td><td>${a.series_label || a.series_id}</td><td>${a.level}</td><td>${a.message}</td>`;
          tbody.appendChild(tr);
        }
      }

      async function refreshAll() {
        setError("");
        try {
          await loadStress();
          await loadPlot();
          await loadAlerts();
        } catch (err) {
          setError("Could not refresh data. Please try again.");
        }
      }

      document.getElementById("refresh").addEventListener("click", refreshAll);
      document.getElementById("seriesSelect").addEventListener("change", loadPlot);

      (async function init() {
        try {
          await loadSeriesList();
          updateExplainer(document.getElementById("seriesSelect").value);
          await refreshAll();
        } catch (err) {
          setError("Dashboard could not load initial data.");
        }
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
