from __future__ import annotations

from pathlib import Path
from io import StringIO
import csv
import pandas as pd

RAW = Path(__file__).parent / "data" / "raw" / "ga4_export.csv"
OUT_DIR = Path(__file__).parent / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_CSV = OUT_DIR / "ga4_clean.csv"
REPORT = Path(__file__).parent / "report.md"


def _read_text_lines(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace").splitlines()
    # drop GA4 metadata comment lines like "# ----"
    return [ln for ln in text if not ln.startswith("#")]


def _parse_dates_robust(s: pd.Series) -> pd.Series:
    """Handle GA4 Explore dates like '20250721' as well as normal strings."""
    out = []
    for v in s.astype(str):
        v = v.strip()
        if not v:
            out.append(pd.NaT)
        elif v.isdigit() and len(v) == 8:  # YYYYMMDD
            try:
                out.append(pd.to_datetime(v, format="%Y%m%d"))
            except Exception:
                out.append(pd.NaT)
        else:
            out.append(pd.to_datetime(v, errors="coerce"))
    return pd.to_datetime(out)


def parse_device_header_csv(path: Path) -> pd.DataFrame:
    """
    Handles GA4 Explore CSV with a first line like:
      ,,,,Device category,mobile,desktop,tablet,Totals
    and a second header line with repeated 'Active users' columns.
    """
    rows = _read_text_lines(path)
    # drop leading blanks
    while rows and not rows[0].strip():
        rows.pop(0)

    if len(rows) < 3:
        raise SystemExit(
            "CSV does not look like a GA4 Explore export with device header."
        )

    # Parse first two lines using csv.reader (respects quoting)
    device_row = next(csv.reader([rows[0]]))
    header_row = next(csv.reader([rows[1]]))

    # Where is "Device category" on the first header row?
    try:
        idx = device_row.index("Device category")
    except ValueError:
        idx = next(
            i for i, v in enumerate(device_row) if v.strip().lower() == "device category"
        )

    device_labels = [c.strip() for c in device_row[idx + 1 :]]  # e.g. mobile, desktop…

    # Rename trailing "Active users" columns to include device labels
    header_cells = header_row[:]
    for i, lab in enumerate(reversed(device_labels), start=1):
        header_cells[-i] = f"Active users ({lab})"

    # Keep only true data lines (skip the totals line that begins with commas)
    data_lines = [ln for ln in rows[2:] if not ln.startswith(",")]

    csv_text = ",".join(header_cells) + "\n" + "\n".join(data_lines)
    df = pd.read_csv(StringIO(csv_text))
    return df


def load_and_clean(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(
            f"Could not find CSV at: {path}\n"
            "Export from GA4 (Explore) and save it there as ga4_export.csv."
        )

    raw = parse_device_header_csv(path)

    df = pd.DataFrame(
        {
            "date": _parse_dates_robust(raw["Date"]),
            "landing_page": raw["Landing page"].astype(str).str.strip(),
            "country": raw.get("Country", ""),
            "event_name": raw.get("Event name", ""),
            "channel_group": raw.get("Session default channel group", ""),
            "active_users_mobile": pd.to_numeric(
                raw.get("Active users (mobile)"), errors="coerce"
            ),
            "active_users_desktop": pd.to_numeric(
                raw.get("Active users (desktop)"), errors="coerce"
            ),
            "active_users_tablet": pd.to_numeric(
                raw.get("Active users (tablet)"), errors="coerce"
            ),
            "active_users_total": pd.to_numeric(
                raw.get("Active users (Totals)"), errors="coerce"
            ),
        }
    )

    # Deduplicate by key fields (ignoring event_name) — GA4 Explore repeats totals per event.
    key = ["date", "landing_page", "channel_group", "country"]
    dedup = (
        df.groupby(key, dropna=False)
        .agg(
            active_users_total=("active_users_total", "max"),
            active_users_mobile=("active_users_mobile", "max"),
            active_users_desktop=("active_users_desktop", "max"),
            active_users_tablet=("active_users_tablet", "max"),
        )
        .reset_index()
    )
    return dedup


def _last_28d_mask(df: pd.DataFrame) -> pd.Series:
    if "date" not in df.columns or df["date"].isna().all():
        return pd.Series([True] * len(df), index=df.index)
    end = df["date"].max().normalize()
    start = end - pd.Timedelta(days=27)
    return (df["date"] >= start) & (df["date"] <= end)


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


def summarise(df: pd.DataFrame) -> str:
    m28 = _last_28d_mask(df)
    d28 = df[m28].copy()

    # Channels summary (Active users total)
    by_ch = (
        d28.groupby("channel_group", dropna=False)["active_users_total"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    # Landing pages (top 10)
    by_lp = (
        d28.groupby("landing_page", dropna=False)["active_users_total"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    # Week-over-Week movers by landing page
    cur_m, prev_m = _wow_windows(df)
    cur_lp = df[cur_m].groupby("landing_page", dropna=False)["active_users_total"].sum()
    prev_lp = df[prev_m].groupby("landing_page", dropna=False)["active_users_total"].sum()
    movers = (
        (cur_lp - prev_lp)
        .to_frame("delta_active_users")
        .join(cur_lp.rename("active_users_cur"), how="left")
        .join(prev_lp.rename("active_users_prev"), how="left")
        .fillna(0)
        .sort_values("delta_active_users", ascending=False)
    )
    risers = movers.head(5).reset_index()
    fallers = movers.tail(5).reset_index()

    # Build Markdown
    lines: list[str] = []
    lines.append("# GA4 Landing Page & Channel Insights (Active users, from CSV)")
    if "date" in df.columns and not df["date"].isna().all():
        lines.append(f"_Data through **{df['date'].max().date()}**_")
    lines.append("")
    lines.append("## Channels — last 28 days")
    lines.append("")
    if not by_ch.empty:
        lines.append("| Channel | Active users |")
        lines.append("|---|---:|")
        for r in by_ch.itertuples():
            ch = r.channel_group if (pd.notna(r.channel_group) and r.channel_group) else "Unassigned"
            lines.append(f"| {ch} | {int(r.active_users_total):,} |")
    else:
        lines.append("_No data in window._")

    lines.append("")
    lines.append("## Top 10 Landing Pages — last 28 days (by active users)")
    lines.append("")
    if not by_lp.empty:
        lines.append("| Landing page | Active users |")
        lines.append("|---|---:|")
        for r in by_lp.itertuples():
            lines.append(f"| {r.landing_page} | {int(r.active_users_total):,} |")
    else:
        lines.append("_No data in window._")

    lines.append("")
    lines.append("## Week-over-Week Movers — Landing pages (Top 5 / Bottom 5)")
    lines.append("")
    if not movers.empty:
        lines.append("**Top risers**")
        lines.append("")
        lines.append("| Landing page | Δ Active users | Last 7d | Prev 7d |")
        lines.append("|---|---:|---:|---:|")
        for r in risers.itertuples():
            lines.append(
                f"| {r.landing_page} | {int(r.delta_active_users):+,.0f} | "
                f"{int(r.active_users_cur):,} | {int(r.active_users_prev):,} |"
            )
        lines.append("")
        lines.append("**Top fallers**")
        lines.append("")
        lines.append("| Landing page | Δ Active users | Last 7d | Prev 7d |")
        lines.append("|---|---:|---:|---:|")
        for r in fallers.itertuples():
            lines.append(
                f"| {r.landing_page} | {int(r.delta_active_users):+,.0f} | "
                f"{int(r.active_users_cur):,} | {int(r.active_users_prev):,} |"
            )
    else:
        lines.append("_Not enough data to compute movers._")

    return "\n".join(lines)


if __name__ == "__main__":
    df = load_and_clean(RAW)
    df.to_csv(CLEAN_CSV, index=False)
    md = summarise(df)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Saved cleaned CSV → {CLEAN_CSV}")
    print(f"Wrote report → {REPORT}")
