# Copilot Instructions for `sandbox` (GA4 Insights)

## Project Overview
- This is a playground for analytics experiments, focused on Google Analytics 4 (GA4) data analysis and reporting.
- Main workflows: fetch GA4 data, clean/process, analyze, and generate visual reports.
- All code and data are in `ga4_insights/`.

## Key Components
- `fetch_ga4.py`: Fetches GA4 data using Google Analytics Data API. Requires `.env` with `PROPERTY_ID` and `GOOGLE_APPLICATION_CREDENTIALS`.
- `analyse.py` / `analyse2.py`: Cleans, processes, and analyzes GA4 data. Generates CSVs and charts in `data/processed/`.
- `report.md`: Markdown report with summary tables and chart embeds.
- `requirements.txt`: Python dependencies (pandas, google-analytics-data, python-dotenv, seaborn).

## Data Flow
1. **Fetch**: Run `fetch_ga4.py` to download raw GA4 data to `data/raw/ga4_export.csv`.
2. **Analyze**: Run `analyse.py` or `analyse2.py` to process data and generate outputs:
   - Cleaned CSVs: `ga4_clean.csv`, `channels_last_90d.csv`, `landing_pages_last_90d.csv`
   - Charts: PNGs in `data/processed/charts/`
   - Report: `report.md`

## Developer Workflows
- **Setup**: Install dependencies with `pip install -r requirements.txt` in `ga4_insights/`.
- **Environment**: Requires `.env` file for API credentials (see `fetch_ga4.py`).
- **Run Analysis**: Execute scripts directly (e.g., `python analyse.py`).
- **Charts**: Matplotlib/seaborn used for visuals; output is always PNG (headless mode).
- **Data Window**: Controlled by `LAST_DAYS` constant in analysis scripts.

## Conventions & Patterns
- All paths use `pathlib.Path` for cross-platform compatibility.
- Data files and outputs are versioned by date window (e.g., `channels_last_90d.csv`).
- Helper functions for robust CSV reading and date parsing.
- No test suite or build system; scripts are run ad hoc.
- All outputs are written to `data/processed/`.

## Integration Points
- Google Analytics Data API (via `google-analytics-data`)
- Local environment variables via `.env` (not checked in)

## Example Workflow
```sh
# Setup
cd ga4_insights
pip install -r requirements.txt

# Fetch data
python fetch_ga4.py

# Analyze and generate report
python analyse.py
# or
python analyse2.py
```

## Key Files & Directories
- `ga4_insights/`: All code, data, and outputs
- `data/raw/`: Raw GA4 exports
- `data/processed/`: Cleaned data, charts, and reports
- `.env`: API credentials (see `fetch_ga4.py` for required keys)

---
_If any conventions or workflows are unclear, please ask for clarification or provide feedback to improve these instructions._
