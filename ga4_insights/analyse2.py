from __future__ import annotations
from pathlib import Path
from io import StringIO
import csv
import pandas as pd

# seaborn + matplotlib for visuals
import matplotlib
matplotlib.use("Agg")  # safe in headless runs
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="whitegrid", context="talk")

# ---- Settings ----
LAST_DAYS = 90

# ---- Paths ----
RAW = Path(__file__).parent / "data" / "raw" / "ga4_export.csv"
OUT_DIR = Path(__file__).parent / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CHART_DIR = OUT_DIR / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_CSV    = OUT_DIR / "ga4_clean.csv"
CHANNELS_CSV = OUT_DIR / f"channels_last_{LAST_DAYS}d.csv"
LANDING_CSV  = OUT_DIR / f"landing_pages_last_{LAST_DAYS}d.csv"
REPORT       = Path(__file__).parent / "report.md"


# ---------- Helpers ----------
def _read_text_lines(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return [ln for ln in text if not ln.startswith("#")]  # drop GA4 comments

def _parse_dates_robust(s: pd.Series) -> pd.Series:
    out = []
    for v in s.astype(str):
        v = v.strip()
        if not v:
            out.append(pd.NaT)
        elif v.isdigit() and len(v) == 8:  # YYYYMMDD
            out.append(pd.to_datetime(v, format="%Y%m%d", errors="coerce"))
        else:
            out.append(pd.to_datetime(v, errors="coerce"))
    return pd.to_datetime(out)

def _fmt_int(x):
    try: return f"{int(x):,}"
    except Exception: return "–"

def _safe_series(df: pd.DataFrame, name: str):
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").fillna(0)
    return pd.Series(0, index=df.index, dtype="float")


# ---------- Parse GA4 CSV (two-row header with device split) ----------
def parse_device_header_csv(path: Path) -> pd.DataFrame:
    rows = _read_text_lines(path)
    while rows and not rows[0].strip():
        rows.pop(0)
    if len(rows) < 3:
        raise SystemExit("CSV does not look like a GA4 Explore export with device header.")

    device_row = next(csv.reader([rows[0]]))
    header_row = next(csv.reader([rows[1]]))

    try:
        idx = device_row.index("Device category")
    except ValueError:
        idx = next(i for i, v in enumerate(device_row) if v.strip().lower() == "device category")

    device_labels = [c.strip() for c in device_row[idx + 1 :]]  # e.g. mobile, desktop, tablet, Totals

    header_cells = header_row[:]
    for i, lab in enumerate(reversed(device_labels), start=1):
        header_cells[-i] = f"Active users ({lab})"

    data_lines = [ln for ln in rows[2:] if not ln.startswith(",")]  # drop totals line
    csv_text = ",".join(header_cells) + "\n" + "\n".join(data_lines)
    df = pd.read_csv(StringIO(csv_text))
    return df


# ---------- Load & clean ----------
def load_and_clean(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(
            f"Could not find CSV at: {path}\n"
            "Export from GA4 (Explore) and save it there as ga4_export.csv."
        )
    raw = parse_device_header_csv(path)

    df = pd.DataFrame({
        "date": _parse_dates_robust(raw["Date"]),
        "landing_page": raw["Landing page"].astype(str).str.strip(),
        "country": raw.get("Country", ""),
        "event_name": raw.get("Event name", ""),
        "channel_group": raw.get("Session default channel group", ""),
        "active_users_mobile": _safe_series(raw, "Active users (mobile)"),
        "active_users_desktop": _safe_series(raw, "Active users (desktop)"),
        "active_users_tablet": _safe_series(raw, "Active users (tablet)"),
        "active_users_total": _safe_series(raw, "Active users (Totals)"),
    })

    # Deduplicate by key (Explore often repeats totals per event)
    key = ["date", "landing_page", "channel_group", "country"]
    dedup = (df.groupby(key, dropna=False)
               .agg(
                   active_users_total=("active_users_total", "max"),
                   active_users_mobile=("active_users_mobile", "max"),
                   active_users_desktop=("active_users_desktop", "max"),
                   active_users_tablet=("active_users_tablet", "max"),
               )
               .reset_index())
    return dedup


def _window_mask(df: pd.DataFrame, days: int) -> pd.Series:
    if "date" not in df.columns or df["date"].isna().all():
        return pd.Series([True] * len(df), index=df.index)
    maxd = df["date"].max().normalize()
    start = maxd - pd.Timedelta(days=days - 1)
    start = max(start, df["date"].min())
    return (df["date"] >= start) & (df["date"] <= maxd)

def _wow_windows(df: pd.DataFrame):
    if "date" not in df.columns or df["date"].isna().all():
        cur = pd.Series([True] * len(df), index=df.index)
        prev = pd.Series([False] * len(df), index=df.index)
        return cur, prev
    end = df["date"].max().normalize()
    cur_start = end - pd.Timedelta(days=6)
    prev_start = cur_start - pd.Timedelta(days=7)
    prev_end = cur_start - pd.Timedelta(days=1)
    cur = (df["date"] >= cur_start) & (df["date"] <= end)
    prev = (df["date"] >= prev_start) & (df["date"] <= prev_end)
    return cur, prev


# ---------- Charts (seaborn) ----------
def _savefig(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()

def make_charts(df: pd.DataFrame, last_days: int) -> list[str]:
    rels: list[str] = []
    m = _window_mask(df, last_days)
    dwin = df[m].copy()
    if dwin.empty:
        return rels

    # 1) Daily active users (line)
    daily = dwin.groupby("date", as_index=False)["active_users_total"].sum().sort_values("date")
    if not daily.empty:
        plt.figure(figsize=(10, 4))
        sns.lineplot(data=daily, x="date", y="active_users_total")
        plt.title(f"Daily active users — last {last_days} days")
        plt.xlabel("Date"); plt.ylabel("Active users")
        p = CHART_DIR / f"daily_active_users_last_{last_days}d.png"
        _savefig(p); rels.append(f"data/processed/charts/{p.name}")

    # 2) Channels (bar, top 10)
    ch = (dwin.groupby("channel_group", as_index=False)["active_users_total"]
              .sum().sort_values("active_users_total", ascending=False).head(10))
    if not ch.empty:
        plt.figure(figsize=(10, 4))
        sns.barplot(data=ch, x="channel_group", y="active_users_total", errorbar=None)
        plt.title("Channels by active users (top 10)")
        plt.xlabel("Channel"); plt.ylabel("Active users"); plt.xticks(rotation=45, ha="right")
        p = CHART_DIR / f"channels_top10_last_{last_days}d.png"
        _savefig(p); rels.append(f"data/processed/charts/{p.name}")

    # 3) Landing pages (barh, top 10)
    lp = (dwin.groupby("landing_page", as_index=False)["active_users_total"]
              .sum().sort_values("active_users_total", ascending=False).head(10))
    if not lp.empty:
        plt.figure(figsize=(10, 6))
        lp_sorted = lp.sort_values("active_users_total")
        sns.barplot(data=lp_sorted, y="landing_page", x="active_users_total", errorbar=None)
        plt.title("Top landing pages — active users (top 10)")
        plt.xlabel("Active users"); plt.ylabel("Landing page")
        p = CHART_DIR / f"landing_pages_top10_last_{last_days}d.png"
        _savefig(p); rels.append(f"data/processed/charts/{p.name}")

    # 4) Device split by channel (grouped bars, top 10 channels)
    dev_long = dwin.melt(
        id_vars=["channel_group"],
        value_vars=["active_users_mobile","active_users_desktop","active_users_tablet"],
        var_name="device", value_name="active_users"
    )
    dev_long["device"] = (dev_long["device"]
                          .str.replace("active_users_", "")
                          .str.replace("_", " ")
                          .str.title())
    totals = (dev_long.groupby("channel_group")["active_users"]
              .sum().sort_values(ascending=False).head(10).index)
    top = dev_long[dev_long["channel_group"].isin(totals)]
    if not top.empty:
        plt.figure(figsize=(10, 5))
        sns.barplot(data=top, x="channel_group", y="active_users", hue="device", errorbar=None)
        plt.title("Device split by channel (top 10 channels)")
        plt.xlabel("Channel"); plt.ylabel("Active users"); plt.xticks(rotation=45, ha="right")
        plt.legend(title=None, loc="best")
        p = CHART_DIR / f"device_split_by_channel_last_{last_days}d.png"
        _savefig(p); rels.append(f"data/processed/charts/{p.name}")

    return rels


# ---------- Report ----------
def summarise(df: pd.DataFrame, last_days: int = LAST_DAYS) -> str:
    m = _window_mask(df, last_days)
    dwin = df[m].copy()

    if "date" in dwin.columns and not dwin["date"].isna().all():
        end = dwin["date"].max().date()
        start = dwin["date"].min().date()
    else:
        start = end = None

    by_ch = (dwin.groupby("channel_group", dropna=False)[["active_users_total"]]
                  .sum().sort_values("active_users_total", ascending=False).reset_index())

    by_lp = (dwin.groupby("landing_page", dropna=False)[["active_users_total"]]
                  .sum().sort_values("active_users_total", ascending=False).head(15).reset_index())

    cur_m, prev_m = _wow_windows(df)
    cur_lp = df[cur_m].groupby("landing_page", dropna=False)["active_users_total"].sum()
    prev_lp = df[prev_m].groupby("landing_page", dropna=False)["active_users_total"].sum()
    movers = ((cur_lp - prev_lp).to_frame("delta_active_users")
              .join(cur_lp.rename("active_users_cur"), how="left")
              .join(prev_lp.rename("active_users_prev"), how="left")
              .fillna(0).sort_values("delta_active_users", ascending=False))
    risers = movers.head(5).reset_index()
    fallers = movers.tail(5).reset_index()

    # Save summary CSVs
    by_ch.to_csv(CHANNELS_CSV, index=False)
    by_lp.to_csv(LANDING_CSV, index=False)

    # Build Markdown
    lines: list[str] = []
    lines.append(f"# GA4 Landing Page & Channel Insights — last {last_days} days")
    if start and end:
        lines.append(f"_Data window: **{start} → {end}** (from CSV)_")
    lines.append("")
    total_active = int(dwin["active_users_total"].sum()) if not dwin.empty else 0
    lines.append(f"**Overview** • Active users (sum): **{_fmt_int(total_active)}**")
    lines.append("")

    # Visuals
    for p in make_charts(df, last_days):
        lines.append(f"![]({p})")
    lines.append("")

    # Tables
    lines.append("## Channels — Active users")
    if not by_ch.empty:
        lines.append("| Channel | Active users |")
        lines.append("|---|---:|")
        for r in by_ch.itertuples():
            ch = r.channel_group if (pd.notna(r.channel_group) and r.channel_group) else "Unassigned"
            lines.append(f"| {ch} | {_fmt_int(r.active_users_total)} |")
    else:
        lines.append("_No data in window._")
    lines.append("")

    lines.append("## Top landing pages — Active users")
    if not by_lp.empty:
        lines.append("| Landing page | Active users |")
        lines.append("|---|---:|")
        for r in by_lp.itertuples():
            lines.append(f"| {r.landing_page} | {_fmt_int(r.active_users_total)} |")
    else:
        lines.append("_No data in window._")

    return "\n".join(lines)


# ---------- Main ----------
if __name__ == "__main__":
    df = load_and_clean(RAW)
    df.to_csv(CLEAN_CSV, index=False)
    md = summarise(df, LAST_DAYS)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Saved cleaned CSV → {CLEAN_CSV}")
    print(f"Wrote report → {REPORT}")
    print(f"Charts → {CHART_DIR}")
    print(f"Also wrote → {CHANNELS_CSV} and → {LANDING_CSV}")
