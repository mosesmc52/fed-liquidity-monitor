# monitor.py
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List

import yaml
from config_utils import expand_env_vars
from notify import notify_console, notify_email_ses, notify_slack
from nyfed_client import FetchSpec, NYFedClient
from plotter import plot_series_with_bands
from store import Store
from stress import compute_stress


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    cfg = load_config("config.yml")
    cfg = expand_env_vars(cfg)

    db_url = cfg["app"]["db_url"]
    lookback_days = int(cfg["app"]["lookback_days"])
    weights = cfg.get("stress_score", {}).get("weights", {})
    alert_score = float(cfg.get("stress_score", {}).get("alert_score", 70))

    base_url = cfg["app"]["base_url"]

    store = Store(db_url=db_url)
    client = NYFedClient(base_url=base_url)

    today = dt.date.today()
    baseline_start = today - dt.timedelta(days=lookback_days + 10)

    plots_dir = Path("plots")
    plots_dir.mkdir(exist_ok=True)

    results = []
    any_triggered = False
    system_score = 0.0

    for s in cfg["series"]:
        series_id = s["id"]
        label = s["label"]
        spec = FetchSpec(**s["fetch"])
        triggers = s.get("triggers", {})

        # Pull only missing data if available
        last = store.latest_date(series_id)
        fetch_start = max(
            baseline_start, (last + dt.timedelta(days=1)) if last else baseline_start
        )
        fetch_end = today

        if fetch_start <= fetch_end:
            fresh_rows = client.fetch_series(
                spec, start_date=fetch_start, end_date=fetch_end
            )
            if fresh_rows:
                store.upsert_observations(series_id, fresh_rows)

        # Load baseline window + latest for scoring/plotting
        rows = store.load_series(series_id, baseline_start, today)
        if len(rows) < 10:
            continue

        values = [v for _, v in rows]
        res = compute_stress(
            series_id, values=values, triggers=triggers, weights=weights
        )
        results.append((label, res))

        system_score = max(system_score, res.score)  # simple aggregator (max)
        if res.triggered:
            any_triggered = True

            out_png = str(plots_dir / f"{series_id}_{today.isoformat()}.png")
            plot_series_with_bands(label, rows, out_png)

            msg = (
                f"{label}\n"
                f"latest={res.latest_value:.4g}  z={res.z:.2f}  pctile={res.pctile:.3f}  Δ7d={res.delta_7d_pct:.1f}%\n"
                f"reasons: {', '.join(res.reasons)}\n"
                f"plot: {out_png}"
            )

            store.insert_alert(dt.datetime.now(), series_id, "ALERT", msg)

            notify_console("NYFed Stress Alert", msg)

            notify_cfg = cfg.get("notify", {})
            if notify_cfg.get("enabled", False):
                channels = notify_cfg.get("channels", [])

                # Slack
                slack_cfg = notify_cfg.get("slack", {})
                if "slack" in channels and slack_cfg.get("enabled", False):
                    notify_slack(
                        slack_cfg.get("webhook_url", ""), "NYFed Stress Alert", msg
                    )

                # Email (SES)
                email_cfg = notify_cfg.get("email", {})
                if (
                    "email" in channels
                    and email_cfg.get("enabled", False)
                    and email_cfg.get("provider") == "ses"
                ):
                    ses_cfg = email_cfg.get("ses", {})
                    notify_email_ses(
                        region=ses_cfg["region"],
                        access_key=ses_cfg["access_key"],
                        secret_key=ses_cfg["secret_key"],
                        from_address=ses_cfg["from_address"],
                        to_addrs=email_cfg.get("to_addrs", []),
                        subject="NYFed Stress Alert",
                        body_text=msg,
                        image_paths=[Path(out_png)] if out_png else None,
                    )

    # System-wide alert (optional)
    if system_score >= alert_score:
        notify_console(
            "SYSTEM STRESS SCORE ALERT",
            f"system_score={system_score:.1f} >= {alert_score:.1f}",
        )


if __name__ == "__main__":
    main()
