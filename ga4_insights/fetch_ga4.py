import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

# load PROPERTY_ID from .env beside this script
load_dotenv(dotenv_path=Path(__file__).parent / ".env")
PROPERTY_ID = os.getenv("PROPERTY_ID")
CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def friendly_checks():
    problems = []
    if not PROPERTY_ID:
        problems.append("Missing PROPERTY_ID in ga4_insights\\.env (expected PROPERTY_ID=338961329)")
    if not CREDS or not Path(CREDS).exists():
        problems.append("GOOGLE_APPLICATION_CREDENTIALS is not set or the file path is wrong.")
    if problems:
        print("\nSETUP ISSUE(S):")
        for p in problems: print(" - " + p)
        print("\nFix these and run again.")
        raise SystemExit(1)

def fetch_ga4(property_id: str, start_date="30daysAgo", end_date="today") -> pd.DataFrame:
    client = BetaAnalyticsDataClient()
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionDefaultChannelGroup"),
            Dimension(name="landingPage"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="engagedSessions"),
            Metric(name="engagementRate"),
        ],
        limit=250000,
    )
    resp = client.run_report(req)
    rows = []
    for r in resp.rows:
        rows.append({
            "date": r.dimension_values[0].value,
            "channel_group": r.dimension_values[1].value,
            "landing_page": r.dimension_values[2].value,
            "sessions": int(r.metric_values[0].value or 0),
            "active_users": int(r.metric_values[1].value or 0),
            "engaged_sessions": int(r.metric_values[2].value or 0),
            "engagement_rate": float(r.metric_values[3].value or 0.0),
        })
    return pd.DataFrame(rows)

def main():
    friendly_checks()
    print("Fetching GA4 data… (property 338961329)")
    df = fetch_ga4("338961329")
    out_dir = Path(__file__).parent / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ga4_landing_page_channel.csv"
    df.to_csv(out_file, index=False)
    print(f"Done. Rows: {len(df)}")
    print(f"Saved to: {out_file}")

if __name__ == "__main__":
    main()
