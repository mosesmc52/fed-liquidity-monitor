// static/app.js

const SERIES_IDS = {
  SOFR: "sofr",
  EFFR: "effr",
  REPO_VOL: "repo_ops_total", // <-- change to match your DB series_id
};

const EXPLAINERS = {
  spread: {
    tag: "SOFR − EFFR",
    title: "SOFR − EFFR Spread",
    what:
      "SOFR is a secured overnight funding rate (Treasury-collateralized repo). EFFR is an unsecured overnight rate (fed funds). The spread is the difference between secured and unsecured funding costs.",
    why:
      "A widening spread often indicates secured funding pressure (collateral scarcity, balance-sheet constraints, or repo market stress). It’s a useful early-warning signal because it reflects plumbing-level funding conditions.",
    watch:
      "Watch for sharp upward moves, persistence over multiple days, and confirmation by rising repo usage or other liquidity facilities.",
    hint:
      "Rates can move for policy reasons; focus on deviations from recent norms and whether multiple indicators corroborate the move."
  },
  repo: {
    tag: "Repo Operations",
    title: "Total Repo Operations",
    what:
      "Repo operations represent short-term cash borrowing against high-quality collateral (typically Treasuries). Total usage reflects how much liquidity the market is demanding through repo channels.",
    why:
      "Quantity signals (usage/volume) often rise before rate signals. Sustained increases can suggest institutions are leaning more on secured funding—sometimes due to stress, sometimes due to technical factors.",
    watch:
      "Watch for sudden spikes, sustained elevation, or repeated high readings—especially alongside widening spreads or rising volatility.",
    hint:
      "Repo can spike around quarter-end or tax dates. Compare against seasonality and look for multi-indicator confirmation."
  },
  overview: {
    tag: "Overview",
    title: "Indicator Explainer",
    what: "Click “Explain” under a KPI to see a plain-language description of that indicator.",
    why: "This panel helps translate market plumbing signals into actionable intuition.",
    watch: "Spikes, persistence, and confirmation across indicators reduce false alarms.",
    hint: "Combine rate-based and quantity-based signals to improve robustness."
  }
};


async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("HTTP " + r.status);
  return await r.json();
}

function setError(msg = "") {
  document.getElementById("globalError").textContent = msg;
}

function fmt(x, d = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

function fmtPct01(p, d = 0) {
  if (p === null || p === undefined || Number.isNaN(p)) return "—";
  const n = Number(p);
  if (!Number.isFinite(n)) return "—";
  return (n * 100).toFixed(d) + "%";
}

function fmtSignedBn(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n >= 0 ? "+" : "−";
  const abs = Math.abs(n);
  // assume repo values are in dollars or millions; your DB could store any unit.
  // We'll format as "B" if it's large.
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(0)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(0)}M`;
  return `${sign}${abs.toFixed(0)}`;
}

function mean(arr) {
  const xs = arr.filter(v => Number.isFinite(v));
  if (!xs.length) return NaN;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function std(arr) {
  const xs = arr.filter(v => Number.isFinite(v));
  if (xs.length < 2) return NaN;
  const m = mean(xs);
  const v = xs.reduce((acc, x) => acc + (x - m) * (x - m), 0) / (xs.length - 1);
  return Math.sqrt(v);
}

function fmtDateTimeISO(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" });
}

function fmtDateISO(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function sigmoid(x) {
  return 1 / (1 + Math.exp(-x));
}

// “Tail-ish” probability from z-score; UI approximation (swap later for real model endpoint)
function tailProbFromZ(z, k) {
  const t = Math.abs(z) - k;
  // steeper slope makes it behave like a tail trigger
  return Math.max(0, Math.min(1, sigmoid(2.2 * t)));
}

function alignByDate(seriesA, seriesB) {
  const mapB = new Map(seriesB.map(o => [o.date, o.value]));
  const out = [];
  for (const a of seriesA) {
    if (mapB.has(a.date)) out.push({ date: a.date, a: a.value, b: mapB.get(a.date) });
  }
  return out;
}

function toXY(obs) {
  return obs.map(o => [new Date(o.date).getTime(), o.value]);
}

/** ---------- Charts ---------- */
let gaugeChart = null;
let stressChart = null;
let comboChart = null;
let seriesChart = null;

function ensureCharts() {
  if (!gaugeChart) {
    gaugeChart = new ApexCharts(document.querySelector("#gauge"), {
      chart: { type: "radialBar", height: 140, sparkline: { enabled: true } },
      series: [0],
      labels: ["Stress"],
      plotOptions: {
        radialBar: {
          hollow: { size: "55%" },
          track: { strokeWidth: "100%" },
          dataLabels: {
            name: { show: false },
            value: { show: false },
          }
        }
      }
    });
    gaugeChart.render();
  }

  if (!stressChart) {
    stressChart = new ApexCharts(document.querySelector("#chartStress"), {
      chart: { type: "area", height: 290, toolbar: { show: false }, zoom: { enabled: false } },
      series: [{ name: "Combined Stress Probability", data: [] }],
      xaxis: { type: "datetime" },
      yaxis: { min: 0, max: 1, tickAmount: 4 },
      stroke: { width: 2 },
      fill: { opacity: 0.22 },
      grid: { borderColor: "#e5e7eb" },
      annotations: {
        yaxis: [
          { y: 0.66, y2: 1.0, borderColor: "#000", fillColor: "#f3f4f6", opacity: 0.35, label: { text: "High", style: { fontSize: "12px" } } },
          { y: 0.33, y2: 0.66, borderColor: "#000", fillColor: "#f3f4f6", opacity: 0.18, label: { text: "Moderate", style: { fontSize: "12px" } } },
        ]
      },
      tooltip: { x: { format: "yyyy-MM-dd" } }
    });
    stressChart.render();
  }

  if (!comboChart) {
    comboChart = new ApexCharts(document.querySelector("#chartCombo"), {
      chart: { type: "line", height: 290, stacked: false, toolbar: { show: false }, zoom: { enabled: false } },
      series: [
        { name: "SOFR - EFFR Spread", type: "line", data: [] },
        { name: "Repo Volume", type: "bar", data: [] },
      ],
      xaxis: { type: "datetime" },
      yaxis: [
        { title: { text: "Spread (%)" }, decimalsInFloat: 4 },
        { opposite: true, title: { text: "Repo Volume" } }
      ],
      stroke: { width: [2, 0] },
      fill: { opacity: [1, 0.65] },
      grid: { borderColor: "#e5e7eb" },
      tooltip: { x: { format: "yyyy-MM-dd" } }
    });
    comboChart.render();
  }

  if (!seriesChart) {
    seriesChart = new ApexCharts(document.querySelector("#chartSeries"), {
      chart: { type: "line", height: 290, toolbar: { show: false }, zoom: { enabled: false } },
      series: [],
      xaxis: { type: "datetime" },
      stroke: { width: [0, 2] },
      fill: { opacity: [0.18, 1] },
      grid: { borderColor: "#e5e7eb" },
      tooltip: { x: { format: "yyyy-MM-dd" } }
    });
    seriesChart.render();
  }
}

/** ---------- API helpers ---------- */
async function fetchSeries(seriesId, lookbackDays) {
  const end = new Date();
  const start = new Date(end.getTime() - (lookbackDays * 24 * 3600 * 1000));
  const url = `/api/series/${encodeURIComponent(seriesId)}?start=${start.toISOString().slice(0,10)}&end=${end.toISOString().slice(0,10)}`;
  return await getJSON(url);
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

async function loadStressTable() {
  const lookback = Number(document.getElementById("lookback").value || 365);
  const stress = await getJSON("/api/stress/latest?lookback_days=" + lookback);

  // last updated
  document.getElementById("lastUpdated").textContent = fmtDateTimeISO(stress.asof);

  const tbody = document.querySelector("#stressTable tbody");
  tbody.innerHTML = "";
  for (const row of stress.results || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.series_label || row.series_id}</td>
      <td>${fmt(row.latest_value, 4)}</td>
      <td>${fmt(row.z, 2)}</td>
      <td>${fmt(row.pctile, 3)}</td>
      <td>${fmt(row.delta_7d_pct, 1)}</td>
      <td>${fmt(row.score, 1)}</td>
      <td>${row.triggered ? "Alert" : "Normal"}</td>
    `;
    tbody.appendChild(tr);
  }

  return stress;
}

async function loadAlerts() {
  const alerts = await getJSON("/api/alerts?limit=25");
  const tbody = document.querySelector("#alertsTable tbody");
  tbody.innerHTML = "";
  for (const a of alerts.alerts || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${fmtDateISO(a.alert_ts)}</td><td>${a.series_label || a.series_id}</td><td>${a.level}</td><td>${a.message}</td>`;
    tbody.appendChild(tr);
  }
}

/** ---------- Model logic (Beginner+) ---------- */
function computeBeginnerPlus(alignedSpreadRepo, k) {
  // alignedSpreadRepo: [{date, spread, repoLogChg5, repoDelta1, repoVal, spreadVal}]
  const spreadVals = alignedSpreadRepo.map(o => o.spread);
  const repoVals = alignedSpreadRepo.map(o => o.repoLogChg5).filter(v => v !== null);

  const mS = mean(spreadVals), sS = std(spreadVals) || 1e-9;
  const mR = mean(repoVals), sR = std(repoVals) || 1e-9;

  const points = alignedSpreadRepo.map(o => {
    const zSpread = (o.spread - mS) / sS;
    const pSpread = tailProbFromZ(zSpread, k);

    let pRepo = 0;
    if (o.repoLogChg5 !== null) {
      const zRepo = (o.repoLogChg5 - mR) / sR;
      pRepo = tailProbFromZ(zRepo, k);
    }

    const pStress = 1 - (1 - pSpread) * (1 - pRepo);
    return { date: o.date, pStress, pSpread, pRepo, zSpread };
  });

  const latest = points.length ? points[points.length - 1] : null;
  return { points, latest };
}

function riskLabel(p) {
  if (p >= 0.80) return { label: "High", cls: "danger" };
  if (p >= 0.50) return { label: "Elevated", cls: "warn" };
  return { label: "Normal", cls: "ok" };
}

function setExplainer(key) {
  const info = EXPLAINERS[key] || EXPLAINERS.overview;
  document.getElementById("explainerTag").textContent = info.tag;
  document.getElementById("explainerTitle").textContent = info.title;
  document.getElementById("explainerWhat").textContent = info.what;
  document.getElementById("explainerWhy").textContent = info.why;
  document.getElementById("explainerWatch").textContent = info.watch;
  document.getElementById("explainerHint").textContent = info.hint;
}


/** ---------- Rendering ---------- */
async function renderSelectedSeriesBand(seriesId, lookback) {
  const payload = await fetchSeries(seriesId, lookback);
  const obs = payload.observations || [];
  if (obs.length < 10) return;

  const values = obs.map(o => o.value);
  const m = mean(values);
  const s = std(values);
  const lo = m - 2*s;
  const hi = m + 2*s;

  const band = obs.map(o => [new Date(o.date).getTime(), lo, hi]);
  const line = toXY(obs);

  await seriesChart.updateOptions({
    series: [
      { name: "Typical range (±2σ)", type: "rangeArea", data: band.map(p => ({ x: p[0], y: [p[1], p[2]] })) },
      { name: payload.series_id, type: "line", data: line.map(p => ({ x: p[0], y: p[1] })) }
    ],
  }, true, true);
}

async function renderCorePanels(lookback) {
  const k = Number(document.getElementById("kTail").value || 2.5);

  // Fetch core series
  const [sofr, effr, repo] = await Promise.all([
    fetchSeries(SERIES_IDS.SOFR, lookback),
    fetchSeries(SERIES_IDS.EFFR, lookback),
    fetchSeries(SERIES_IDS.REPO_VOL, lookback),
  ]);

  const sofrObs = sofr.observations || [];
  const effrObs = effr.observations || [];
  const repoObs = repo.observations || [];

  // Spread series by date
  const spread = alignByDate(sofrObs, effrObs).map(o => ({ date: o.date, value: (o.a - o.b) }));

  // Repo: compute 1D delta and 5D log-change
  const repoLog = repoObs.map(o => ({ date: o.date, value: Math.log1p(o.value) }));
  const repoDelta1 = repoObs.map((o, i) => {
    const prev = i >= 1 ? repoObs[i - 1].value : null;
    return { date: o.date, value: (prev === null ? null : (o.value - prev)) };
  });
  const repoLogChg5 = repoLog.map((o, i) => {
    const prev = i >= 5 ? repoLog[i - 5].value : null;
    return { date: o.date, value: (prev === null ? null : (o.value - prev)) };
  });

  // Align spread with repo values
  const aligned = alignByDate(spread, repoObs).map(o => ({
    date: o.date,
    spread: o.a,
    repoVal: o.b,
  }));

  // For model features, align spread with repoLogChg5 and repoDelta1
  const mapRepoLogChg5 = new Map(repoLogChg5.filter(x => x.value !== null).map(x => [x.date, x.value]));
  const mapRepoDelta1 = new Map(repoDelta1.filter(x => x.value !== null).map(x => [x.date, x.value]));

  const alignedForModel = aligned.map(o => ({
    date: o.date,
    spread: o.spread,
    repoVal: o.repoVal,
    repoDelta1: mapRepoDelta1.has(o.date) ? mapRepoDelta1.get(o.date) : null,
    repoLogChg5: mapRepoLogChg5.has(o.date) ? mapRepoLogChg5.get(o.date) : null
  }));

  // Compute beginner+ probabilities
  const { points, latest } = computeBeginnerPlus(alignedForModel, k);

  // KPIs
  if (latest) {
    document.getElementById("stressPct").textContent = fmtPct01(latest.pStress, 0);
    await gaugeChart.updateSeries([Math.round(latest.pStress * 100)], true);

    // Spread KPI: show as percent points (rates are already in percent; spread is percent points)
    document.getElementById("spreadNow").textContent = fmt(latest ? alignedForModel[alignedForModel.length - 1].spread : null, 2) + "%";
    document.getElementById("spreadSub").textContent = `Tail k=${k.toFixed(1)} | p_outlier=${fmtPct01(latest.pSpread, 0)}`;

    // Repo KPI: 1-day delta
    const repoDeltaNow = alignedForModel.length ? alignedForModel[alignedForModel.length - 1].repoDelta1 : null;
    document.getElementById("repoDeltaNow").textContent = fmtSignedBn(repoDeltaNow);
    document.getElementById("repoSub").textContent = `5d logΔ feature | p_outlier=${fmtPct01(latest.pRepo, 0)}`;

    // Analysis cards
    // Spread vol (20d)
    const spreadVals = alignedForModel.map(o => o.spread);
    const vol20 = std(spreadVals.slice(-20));
    document.getElementById("spreadVol").textContent = (Number.isFinite(vol20) ? fmt(vol20, 3) + "%" : "—");
    document.getElementById("pSpread").textContent = fmtPct01(latest.pSpread, 0);

    const spreadRisk = riskLabel(latest.pSpread);
    document.getElementById("spreadRisk").textContent = spreadRisk.label;

    // Repo 5d change: show latest value
    const repoChg5Now = alignedForModel.length ? alignedForModel[alignedForModel.length - 1].repoLogChg5 : null;
    document.getElementById("repoChg5").textContent = repoChg5Now === null ? "—" : fmt(repoChg5Now, 3);
    document.getElementById("pRepo").textContent = fmtPct01(latest.pRepo, 0);

    const repoRisk = riskLabel(latest.pRepo);
    document.getElementById("repoRisk").textContent = repoRisk.label;

    // Alert banner
    const banner = document.getElementById("alertBanner");
    const text = document.getElementById("alertText");
    const sys = riskLabel(latest.pStress);

    banner.className = `card alertBanner ${sys.cls === "danger" ? "danger" : (sys.cls === "warn" ? "warn" : "ok")}`;
    if (sys.cls === "danger") {
      text.innerHTML = `<strong>ALERT:</strong> Elevated Liquidity Stress Detected. Monitor conditions closely and review funding positions.`;
    } else if (sys.cls === "warn") {
      text.innerHTML = `<strong>WATCH:</strong> Stress probability elevated. Track spread and repo usage for persistence.`;
    } else {
      text.innerHTML = `<strong>OK:</strong> No elevated liquidity stress detected.`;
    }
  }

  // Stress chart series
  const stressSeries = points.map(p => ({ x: new Date(p.date).getTime(), y: p.pStress }));
  await stressChart.updateSeries([{ name: "Combined Stress Probability", data: stressSeries }], true);

  // Combo chart series (spread line + repo bars)
  const spreadSeries = aligned.map(o => ({ x: new Date(o.date).getTime(), y: o.spread }));
  const repoSeries = aligned.map(o => ({ x: new Date(o.date).getTime(), y: o.repoVal }));
  await comboChart.updateOptions({ series: [
    { name: "SOFR - EFFR Spread", type: "line", data: spreadSeries },
    { name: "Repo Volume", type: "bar", data: repoSeries },
  ]}, true, true);
}

async function refreshAll() {
  setError("");
  ensureCharts();

  const sel = document.getElementById("seriesSelect");
  const lookback = Number(document.getElementById("lookback").value || 365);

  try {
    await loadStressTable();
    await loadAlerts();

    // Wireframe panels
    await renderCorePanels(lookback);

    // Selected indicator band chart
    if (sel.value) {
      await renderSelectedSeriesBand(sel.value, lookback);
    }
  } catch (err) {
    console.error(err);
    setError("Could not refresh. Verify your series IDs (sofr/effr/repo) exist in DB.");
  }
}

document.getElementById("refresh").addEventListener("click", refreshAll);
document.getElementById("seriesSelect").addEventListener("change", refreshAll);
document.getElementById("explainSpreadBtn")?.addEventListener("click", () => setExplainer("spread"));
document.getElementById("explainRepoBtn")?.addEventListener("click", () => setExplainer("repo"));


(async function init() {
  try {
    await loadSeriesList();
    await refreshAll();
    setExplainer("overview");
  } catch (err) {
    console.error(err);
    setError("Dashboard could not load. Check /api/series and static file mounting.");
  }
})();
