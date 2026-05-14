"""
sync_maintenance.py
===================
Local script — runs on your PC to sync IQF maintenance data to GitHub.

WHAT IT DOES
------------
1. Connects to your local PostgreSQL repair database
2. Exports ALL IQF maintenance requests → iqf_maintenance_archive.json
3. git add → git commit → git push  (GitHub auto-triggers Render deploy)

SETUP (one-time)
----------------
1. Make sure this script is in your IQF dashboard project folder
   (the same folder that has app.py and is linked to GitHub)
2. Make sure git is installed and the folder is already a git repo:
       git remote -v   ← should show your GitHub repo URL
3. Run once to test:
       python sync_maintenance.py
4. Schedule with Windows Task Scheduler for automatic daily/shift sync
   (see TASK SCHEDULER section at the bottom of this file)

REQUIREMENTS
------------
pip install psycopg2-binary sqlalchemy pandas pytz
(these are already in your requirements.txt)
"""

import os
import json
import subprocess
import pandas as pd
from datetime import datetime

try:
    import pytz
    from sqlalchemy import create_engine, text as sa_text
except ImportError as _e:
    raise SystemExit(f"Missing dependency: {_e}\nRun: pip install psycopg2-binary sqlalchemy pandas pytz")

# ===========================================================================
# CONFIG — edit if needed
# ===========================================================================

# Your local repair database connection string
REPAIR_DATABASE_URL = "postgresql://postgres:ho2025@localhost:5432/repair"

# Output JSON — lives in the same folder as app.py (so git picks it up)
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(BASE_DIR, "iqf_maintenance_archive.json")

# Git repo folder (same as this script — change only if different)
GIT_REPO_DIR = BASE_DIR

# ===========================================================================
# (Do not change below unless your DB schema changes)
# ===========================================================================

EGYPT_TZ = pytz.timezone("Africa/Cairo")

_MAINT_COLS = [
    "id", "title", "description", "machine_name", "status", "impact",
    "shift", "failure_cause",
    "failure_start_time", "machine_receipt_time",
    "technician_name", "engineer_name", "depart",
    "repair_time_minutes", "maintenance_notes", "repair_method",
    "spare_parts", "production_machine_receipt_time",
]

_MAINT_TS_COLS = [
    "failure_start_time",
    "machine_receipt_time",
    "production_machine_receipt_time",
]


# ---------------------------------------------------------------------------
# Step 1 — Fetch from local PostgreSQL
# ---------------------------------------------------------------------------

def fetch_iqf_maintenance() -> pd.DataFrame:
    """Fetch ALL IQF maintenance requests from the local repair database."""
    cols_sql = ", ".join(_MAINT_COLS)
    query = f"""
        SELECT {cols_sql}
        FROM   public.repair_requests
        WHERE  production_line = 'IQF'
        ORDER BY COALESCE(failure_start_time, created_at) DESC;
    """
    engine = create_engine(REPAIR_DATABASE_URL, connect_args={"connect_timeout": 10})
    with engine.connect() as conn:
        df = pd.read_sql_query(sa_text(query), conn)

    # Convert UTC timestamps → Egypt local time (naive)
    for col in _MAINT_TS_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], utc=True, errors="coerce")
                .dt.tz_convert(EGYPT_TZ)
                .dt.tz_localize(None)
            )
    return df


# ---------------------------------------------------------------------------
# Step 2 — Save to JSON
# ---------------------------------------------------------------------------

def save_json(df: pd.DataFrame, path: str) -> None:
    """Save DataFrame to JSON with datetime serialization."""
    df = df.copy()

    # Convert datetime columns to ISO strings for JSON
    for col in _MAINT_TS_COLS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) and x is not None else None
            )

    records = df.to_dict(orient="records")

    payload = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records": len(records),
        "records": records,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    print(f"[OK] Saved {len(records)} records → {path}")


# ---------------------------------------------------------------------------
# Step 3 — git push to GitHub
# ---------------------------------------------------------------------------

def git_push(repo_dir: str, json_path: str) -> bool:
    """Stage, commit, and push the JSON archive to GitHub."""
    rel_path = os.path.relpath(json_path, repo_dir)
    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M")

    steps = [
        (["git", "add", rel_path],                                             True),
        (["git", "commit", "-m", f"sync: IQF maintenance archive {now_str}"], False),  # ok if no changes
        (["git", "push"],                                                       True),
    ]

    for cmd, must_succeed in steps:
        result = subprocess.run(cmd, cwd=repo_dir, capture_output=True, text=True)
        label  = " ".join(cmd)
        if result.stdout.strip():
            print(f"    {result.stdout.strip()}")
        if result.stderr.strip():
            print(f"    {result.stderr.strip()}")
        if result.returncode != 0:
            if must_succeed:
                print(f"[ERROR] Command failed: {label}")
                return False
            else:
                print(f"[INFO] {label} → nothing to commit, skipping.")
        else:
            print(f"[OK] {label}")

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*55}")
    print(f"  IQF Maintenance Sync  —  {ts}")
    print(f"{'='*55}")

    # 1. Fetch
    print("\n[1/3] Fetching from local database...")
    try:
        df = fetch_iqf_maintenance()
        print(f"[OK] {len(df)} records fetched.")
    except Exception as exc:
        print(f"[ERROR] Database connection failed:\n       {exc}")
        print("\nCheck that PostgreSQL is running and REPAIR_DATABASE_URL is correct.")
        return

    # 2. Save JSON
    print("\n[2/3] Saving JSON archive...")
    try:
        save_json(df, OUTPUT_JSON)
    except Exception as exc:
        print(f"[ERROR] Could not save JSON: {exc}")
        return

    # 3. Push to GitHub
    print("\n[3/3] Pushing to GitHub...")
    try:
        ok = git_push(GIT_REPO_DIR, OUTPUT_JSON)
        if ok:
            print("\n[DONE] Render will auto-deploy in ~2-5 minutes.")
        else:
            print("\n[WARN] Git push had errors. Check git config / credentials.")
    except Exception as exc:
        print(f"[ERROR] Git error: {exc}")

    print()


if __name__ == "__main__":
    main()


# ===========================================================================
# WINDOWS TASK SCHEDULER — run automatically every day at 08:00
# ===========================================================================
# Open Task Scheduler → Create Basic Task
#   Name:    IQF Maintenance Sync
#   Trigger: Daily, 08:00 (or every shift start)
#   Action:  Start a program
#     Program:   C:\Users\hend.maher\AppData\Local\Programs\Python\Python312\python.exe
#     Arguments: "C:\Users\hend.maher\Desktop\IQF production statistics\test_dashboard\2\sync_maintenance.py"
#
# To run every 8 hours instead:
#   After creating, open the task → Triggers → Edit → Repeat every 8 hours
# ===========================================================================
