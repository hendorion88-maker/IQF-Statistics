"""
sync_data.py  -  Daily SCADA data sync to Google Drive
=======================================================

Run this script each day after copying the new SCADA CSV files from the SCADA PC.

------------------------------------------------------------------------------
ONE-TIME SETUP  (service account - no browser login needed)
------------------------------------------------------------------------------
 1. Go to  https://console.cloud.google.com/
 2. Select your project → Enable "Google Drive API" (if not already)
 3. Go to "Credentials" → "Create Credentials" → "Service Account"
    - Give it any name (e.g. "scada-sync") → Click "Done"
 4. Click the service account you just created → "Keys" tab
    → "Add Key" → "Create new key" → JSON → Download
    Save the downloaded file as:
        service_account.json
    in the same folder as this script.

 5. Open the downloaded JSON and copy the "client_email" value, e.g.:
        scada-sync@your-project.iam.gserviceaccount.com

 6. In Google Drive, right-click your SCADA_Master folder → Share
    Paste the client_email above and give it "Editor" access → Send.

 7. Edit the CONFIG section below (LOCAL_DATA_CSV, LOCAL_ALARM_CSV, GDRIVE_FOLDER_ID)

 8. Run:   python sync_data.py
    No browser will open. It authenticates silently and syncs.

------------------------------------------------------------------------------
DAILY USE
------------------------------------------------------------------------------
 1. Copy the new SCADA export files to the paths set in LOCAL_DATA_CSV / LOCAL_ALARM_CSV
 2. Run:   python sync_data.py
 3. Done - the dashboard will show updated data on next page load.
    You can automate step 2 with Windows Task Scheduler (see bottom of file).

------------------------------------------------------------------------------
INSTALL DEPENDENCIES  (one-time)
------------------------------------------------------------------------------
  pip install google-api-python-client google-auth pandas
"""

import os
import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account


# ===========================================================================
# CONFIG  –  Edit the three lines below
# ===========================================================================

# Full path to the new SCADA export files on your PC (the files you copy daily)
LOCAL_DATA_CSV  = r"C:\Users\hend.maher\Desktop\SCADA_IQF\Data_log0.csv"
LOCAL_ALARM_CSV = r"C:\Users\hend.maher\Desktop\SCADA_IQF\Alarm_log0.csv"

# Google Drive folder ID  (from the folder URL after /folders/)
GDRIVE_FOLDER_ID = "1Or4wlNkSVLI3LUAjIKE-ZNVjPqX-8IUm"

# ===========================================================================
# (Do not change these)
MASTER_DATA_NAME  = "master_data.csv"
MASTER_ALARM_NAME = "master_alarm.csv"
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "service_account.json")
SCOPES = ["https://www.googleapis.com/auth/drive"]
# ===========================================================================


def authenticate():
    """Authenticate using a service account JSON key - no browser needed."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f"Service account key not found: {SERVICE_ACCOUNT_FILE}\n"
            "Follow the ONE-TIME SETUP instructions at the top of this file."
        )
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    print("  Authenticated via service account.")
    return service


def find_drive_file(service, folder_id, filename):
    """Return Drive file metadata dict if it exists in the folder, else None."""
    query = (
        f"'{folder_id}' in parents "
        f"and name = '{filename}' "
        f"and trashed = false"
    )
    result = service.files().list(q=query, fields="files(id, name)").execute()
    files = result.get("files", [])
    return files[0] if files else None


def download_master(service, folder_id, filename):
    """
    Download the existing master CSV from Google Drive.
    Returns (DataFrame, file_id) or (empty DataFrame, None) if not found.
    """
    meta = find_drive_file(service, folder_id, filename)
    if meta is None:
        print(f"  No existing master found on Drive - will create '{filename}' fresh.")
        return pd.DataFrame(), None
    file_id = meta["id"]
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf)
    print(f"  Downloaded master '{filename}':  {len(df):,} rows")
    return df, file_id


def upload_master(service, df, folder_id, filename, file_id=None):
    """Create or overwrite the master CSV on Google Drive."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"  Updated  '{filename}'  (ID: {file_id})")
    else:
        meta = {
            "name": filename,
            "parents": [folder_id],
            "mimeType": "text/csv",
        }
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        file_id = f["id"]
        print(f"  Created  '{filename}'  - Drive file ID: {file_id}")
    return file_id


def merge_and_dedup(master_df, new_df, dedup_cols):
    """
    Merge new_df into master_df, remove duplicate rows, and sort chronologically.
    Returns (merged DataFrame, number of new rows added).
    """
    if master_df.empty:
        return new_df.copy().reset_index(drop=True), len(new_df)

    # Use only columns that exist in both DataFrames
    common_cols = [c for c in master_df.columns if c in new_df.columns]
    combined = pd.concat(
        [master_df[common_cols], new_df[common_cols]],
        ignore_index=True,
    )

    # Deduplicate (keep last = prefer newer data on timestamp collision)
    key = [c for c in dedup_cols if c in combined.columns]
    combined = combined.drop_duplicates(subset=key, keep="last")
    combined = combined.sort_values(key[0]).reset_index(drop=True)

    n_added = len(combined) - len(master_df)
    return combined, max(n_added, 0)


def sync_file(service, local_csv, master_name, dedup_cols, folder_id, csv_sep=";"):
    """Full sync cycle for one file: download master, merge local export, upload."""
    print(f"\n-- {master_name} {'-' * max(1, 55 - len(master_name))}")

    if not os.path.exists(local_csv):
        print(f"  ERROR: Local file not found - {local_csv}")
        print(f"  Skipping {master_name}.")
        return

    # Load the new local SCADA export (raw, unmodified)
    try:
        new_df = pd.read_csv(local_csv, sep=csv_sep, encoding="utf-8")
    except UnicodeDecodeError:
        new_df = pd.read_csv(local_csv, sep=csv_sep, encoding="latin-1")
    print(f"  Local export:  {len(new_df):,} rows")

    # Download the current master from Drive
    master_df, file_id = download_master(service, folder_id, master_name)

    # Merge
    merged, n_added = merge_and_dedup(master_df, new_df, dedup_cols)
    print(f"  After merge:   {len(merged):,} total rows  (+{n_added} new rows added)")

    # Upload updated master
    upload_master(service, merged, folder_id, master_name, file_id)


def main():
    print("=" * 58)
    print("  SCADA Data Sync  ->  Google Drive")
    print("=" * 58)

    # Authenticate
    print("\nAuthenticating with Google Drive ...")
    try:
        service = authenticate()
    except Exception as exc:
        print(f"  Authentication failed: {exc}")
        return

    # Sync data log
    sync_file(
        service,
        LOCAL_DATA_CSV,
        MASTER_DATA_NAME,
        dedup_cols=["TimeString", "VarName"],
        folder_id=GDRIVE_FOLDER_ID,
    )

    # Sync alarm log
    sync_file(
        service,
        LOCAL_ALARM_CSV,
        MASTER_ALARM_NAME,
        dedup_cols=["TimeString", "MsgNumber", "StateAfter"],
        folder_id=GDRIVE_FOLDER_ID,
    )

    print("\n" + "=" * 58)
    print("  Sync complete.")
    print("  The dashboard will show updated data on next page load.")
    print("=" * 58)


# ===========================================================================
# AUTOMATE WITH WINDOWS TASK SCHEDULER (optional)
# ===========================================================================
# To run this script automatically every day:
#
#  1. Open  Task Scheduler  (search for it in the Start menu)
#  2. Click "Create Basic Task"
#  3. Give it a name, e.g. "SCADA Daily Sync"
#  4. Trigger: Daily, choose your preferred time
#  5. Action: "Start a program"
#     Program/script:   C:\Users\YourName\AppData\Local\Programs\Python\Python3x\python.exe
#     Arguments:        "C:\path\to\sync_data.py"
#  6. Finish.
#
# The script will run silently each day (no browser - service account key handles auth).
# ===========================================================================

if __name__ == "__main__":
    main()
