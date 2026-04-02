"""
Production Filler Dashboard Generator  v3
==========================================

HOW IT WORKS
─────────────
1. You configure GOOGLE_SHEET_URL below (just paste your Google Sheet share link).
2. Run:  python dashboard_generator_v3.py
3. The script downloads the sheet, processes the data, builds the dashboard HTML,
   starts a local data API server, and opens the dashboard in your browser.
4. Every time you REFRESH the browser page, it re-downloads the Google Sheet and
   reloads all charts/KPIs live — no need to re-run Python.

FILE LAYOUT (all in the same folder)
─────────────────────────────────────
  dashboard_generator_v3.py   ← this file
  data_cache.xlsx             ← auto-downloaded each refresh (do not edit)
  dashboard.html              ← generated once; open this in your browser

GOOGLE SHEET SETUP
───────────────────
  • Share the sheet: Share → Anyone with the link → Viewer
  • Paste the share URL as GOOGLE_SHEET_URL below.
  • The sheet must have Sheet1 (or Sheet2) with the expected columns.

DEPENDENCIES
─────────────
  pip install pandas openpyxl flask requests
"""

import sys, json, os, io, time, threading, webbrowser, logging
from datetime import datetime
import requests
import pandas as pd
from flask import Flask, jsonify, send_file, request
from flask.logging import default_handler

# ═════════════════════════════════════════════════════════════
#  ▶▶  CONFIGURE HERE
# ═════════════════════════════════════════════════════════════
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1TnWB5q_srpCfwV98cy6yGIbWtMDCPPIE/edit?usp=sharing"
# ↑ Replace with your actual Google Sheet share URL.
# Example: "https://docs.google.com/spreadsheets/d/1BU3VWALOVa7wkqTlUvNWxGZ-hUFbfxmy/edit?usp=sharing"

SHEET_TAB    = "Sheet1"   # Tab name to read (or 0 for the first tab)
CACHE_FILE   = "data_cache.xlsx"
OUTPUT_HTML  = "dashboard.html"
SERVER_PORT  = int(os.environ.get("PORT", 5050))  # overridden by PORT env var in production
AUTO_OPEN    = os.environ.get("AUTO_OPEN", "false").lower() == "true"  # set AUTO_OPEN=true locally to re-enable

# ─── Filler parameters ────────────────────────────────────────
SETPOINT   = 10.04
BOX_WEIGHT = 0.486
SIGMA      = 0.0004

# ─── Column names ─────────────────────────────────────────────
COL_TYPE  = "night/light"
COL_SHIFT = "# shift"
COL_DATE  = "Date"
COL_W_T1  = "W T1";  COL_W_T2  = "W T2";  COL_W_T3  = "W T3"
COL_DC    = "DC"
COL_B_T1  = "B T1";  COL_B_T2  = "B T2";  COL_B_T3  = "B T3"
COL_TT1   = "T T1";  COL_TT2   = "T T2";  COL_TT3   = "T T3"
COL_TC    = "TC"
# ═════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────
#  GOOGLE SHEETS DOWNLOAD
# ─────────────────────────────────────────────────────────────
def sheet_id_from_url(url: str) -> str:
    """Extract the spreadsheet ID from any Google Sheets URL."""
    # Handles /d/{ID}/ patterns
    import re
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not m:
        raise ValueError(
            f"Cannot extract spreadsheet ID from URL:\n  {url}\n"
            "Make sure GOOGLE_SHEET_URL is a valid Google Sheets link."
        )
    return m.group(1)


def download_sheet(url: str, dest: str) -> str:
    """Download the Google Sheet as .xlsx and save to dest. Returns dest path."""
    sid   = sheet_id_from_url(url)
    export = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"
    print(f"  Downloading sheet {sid} …", end=" ", flush=True)
    t0 = time.time()
    try:
        r = requests.get(export, timeout=30)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise ConnectionError(
            f"Failed to download Google Sheet.\n"
            f"  • Check that the sheet is shared ('Anyone with the link → Viewer').\n"
            f"  • Check your internet connection.\n"
            f"  Error: {e}"
        )
    with open(dest, "wb") as f:
        f.write(r.content)
    elapsed = time.time() - t0
    print(f"done ({elapsed:.1f}s, {len(r.content)//1024} KB)")
    return dest


# ─────────────────────────────────────────────────────────────
#  DATA LOADING
# ─────────────────────────────────────────────────────────────
def load_data(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(filepath, sheet_name=SHEET_TAB)
    except Exception:
        df = pd.read_excel(filepath, sheet_name=0)

    df.columns = [str(c).strip() for c in df.columns]

    num_cols = [COL_W_T1,COL_W_T2,COL_W_T3,COL_DC,
                COL_B_T1,COL_B_T2,COL_B_T3,
                COL_TT1, COL_TT2, COL_TT3, COL_TC]
    df[COL_SHIFT] = pd.to_numeric(df[COL_SHIFT], errors="coerce")
    df = df[df[COL_SHIFT].notna()].copy()
    df[COL_SHIFT] = df[COL_SHIFT].astype(int)
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
#  METRICS
# ─────────────────────────────────────────────────────────────
def compute_metrics(df: pd.DataFrame) -> dict:
    baseline = df[df[COL_SHIFT] == 0].iloc[0] if (df[COL_SHIFT] == 0).any() else None
    def _f(row, col): return float(row[col] or 0) if row is not None and col in row.index else 0
    base_tt1 = _f(baseline, COL_TT1)
    base_tt2 = _f(baseline, COL_TT2)
    base_tt3 = _f(baseline, COL_TT3)
    base_tc  = _f(baseline, COL_TC)

    sdf = df[df[COL_B_T1].notna() | df[COL_B_T2].notna()].copy().reset_index(drop=True)

    shifts = []
    for pos, (_, r) in enumerate(sdf.iterrows()):
        b1=float(r.get(COL_B_T1) or 0); b2=float(r.get(COL_B_T2) or 0); b3=float(r.get(COL_B_T3) or 0)
        w1=float(r.get(COL_W_T1) or 0); w2=float(r.get(COL_W_T2) or 0); w3=float(r.get(COL_W_T3) or 0)
        dc=float(r.get(COL_DC)   or 0)
        tt1=float(r.get(COL_TT1) or 0); tt2=float(r.get(COL_TT2) or 0)
        tt3=float(r.get(COL_TT3) or 0); tc=float(r.get(COL_TC)   or 0)

        tb = b1+b2+b3; tw = w1+w2+w3
        aw1=(w1/b1) if b1>0 else None
        aw2=(w2/b2) if b2>0 else None
        aw3=(w3/b3) if b3>0 else None
        aa =(tw/tb)  if tb>0 else None

        gpb1=round((aw1-10)*1000,2) if aw1 else 0
        gpb2=round((aw2-10)*1000,2) if aw2 else 0
        gpb3=round((aw3-10)*1000,2) if aw3 else 0
        og1=round((gpb1*b1)/1000,2) if aw1 else 0
        og2=round((gpb2*b2)/1000,2) if aw2 else 0
        og3=round((gpb3*b3)/1000,2) if aw3 else 0

        prev      = sdf.iloc[pos-1] if pos > 0 else baseline
        ptt1=float(prev[COL_TT1] or 0) if prev is not None else base_tt1
        ptt2=float(prev[COL_TT2] or 0) if prev is not None else base_tt2
        ptt3=float(prev[COL_TT3] or 0) if prev is not None else base_tt3
        ptc =float(prev[COL_TC]  or 0) if prev is not None else base_tc

        inc_t1=tt1-ptt1; inc_t2=tt2-ptt2; inc_t3=tt3-ptt3; inc_tc=tc-ptc
        sum_tracks=inc_t1+inc_t2+inc_t3
        gap=inc_tc-dc
        gap_pct=round(gap/inc_tc*100,3) if inc_tc else 0

        try:    date_str=pd.to_datetime(r.get(COL_DATE,"")).strftime("%d/%m")
        except: date_str=str(r.get(COL_DATE,""))[:10]

        shifts.append({
            "shift":int(r[COL_SHIFT]),"type":str(r.get(COL_TYPE,"")).strip().lower(),"date":date_str,
            "b1":b1,"b2":b2,"b3":b3,"w1":w1,"w2":w2,"w3":w3,"dc":dc,
            "total_boxes":tb,"total_weight":tw,
            "avg_w1":round(aw1,6) if aw1 else None,
            "avg_w2":round(aw2,6) if aw2 else None,
            "avg_w3":round(aw3,6) if aw3 else None,
            "avg_all":round(aa,6) if aa else None,
            "over1":round((aw1-SETPOINT)/SETPOINT*b1) if aw1 else 0,
            "over2":round((aw2-SETPOINT)/SETPOINT*b2) if aw2 else 0,
            "over3":round((aw3-SETPOINT)/SETPOINT*b3) if aw3 else 0,
            "total_over":(round((aw1-SETPOINT)/SETPOINT*b1) if aw1 else 0)+
                         (round((aw2-SETPOINT)/SETPOINT*b2) if aw2 else 0)+
                         (round((aw3-SETPOINT)/SETPOINT*b3) if aw3 else 0),
            "give1":round((aw1-SETPOINT)*b1,2) if aw1 else 0,
            "give_per_box1":gpb1,"give_per_box2":gpb2,"give_per_box3":gpb3,
            "over_g1":og1,"over_g2":og2,"over_g3":og3,
            "total_over_g":round(og1+og2+og3,2),
            "tt1":tt1,"tt2":tt2,"tt3":tt3,"tc":tc,
            "inc_t1":inc_t1,"inc_t2":inc_t2,"inc_t3":inc_t3,"inc_tc":inc_tc,
            "gap":gap,"gap_pct":gap_pct,"sum_tracks":sum_tracks,
            "diff_total":round(tc-(tt1+tt2+tt3),0),
        })

    total_pc  = sum(s["total_boxes"] for s in shifts)
    total_wt  = sum(s["total_weight"] for s in shifts)
    total_over= round(sum(s["total_over_g"] for s in shifts),2)
    total_over_pcs=sum(s["total_over"] for s in shifts)
    t1t=sum(s["b1"] for s in shifts); t2t=sum(s["b2"] for s in shifts); t3t=sum(s["b3"] for s in shifts)
    avg_f = total_wt/total_pc if total_pc else 0
    avg_r = total_pc/len(shifts) if shifts else 0
    uf_n  = sum(1 for s in shifts if s["total_over"]<0)
    dev_acc=round(100-((avg_f-10)/10),3) if total_pc else 0
    final=shifts[-1]
    tc_delta=final["tc"]-base_tc
    t1d=final["tt1"]-base_tt1; t2d=final["tt2"]-base_tt2; t3d=final["tt3"]-base_tt3
    total_gap=tc_delta-total_wt
    gap_pct_total=round(total_gap/tc_delta*100,3) if tc_delta else 0
    try:    dr=f"{shifts[0]['date']} – {shifts[-1]['date']}"
    except: dr=""
    td=tc_delta if tc_delta else 1

    return {
        "shifts":shifts,"total_pc":total_pc,"total_wt":total_wt,
        "total_over":total_over,"total_over_pcs":total_over_pcs,
        "t1_total":t1t,"t2_total":t2t,"t3_total":t3t,
        "avg_filler":round(avg_f,4),"avg_rate":round(avg_r,1),
        "underfill_n":uf_n,"device_accuracy":dev_acc,
        "avg_over_per_shift":round(total_over/len(shifts),2) if shifts else 0,
        "pct_over":round(total_over_pcs/total_pc*100,2) if total_pc else 0,
        "setpoint":SETPOINT,"sigma":SIGMA,"n_shifts":len(shifts),"date_range":dr,
        "tc_start":base_tc,"tc_end":final["tc"],"tc_delta":tc_delta,
        "t1_delta":t1d,"t2_delta":t2d,"t3_delta":t3d,
        "total_gap":total_gap,"gap_pct_total":gap_pct_total,
        "base_tt1":base_tt1,"base_tt2":base_tt2,"base_tt3":base_tt3,"base_tc":base_tc,
        "t1d_pct":round(t1d/td*100,1),"t2d_pct":round(t2d/td*100,1),"t3d_pct":round(t3d/td*100,1),
        "t1_pct":round(t1t/total_pc*100,1) if total_pc else 0,
        "t2_pct":round(t2t/total_pc*100,1) if total_pc else 0,
        "t3_pct":round(t3t/total_pc*100,1) if total_pc else 0,
        "last_updated": datetime.now().strftime("%d %b %Y %H:%M:%S"),
        "source_url": GOOGLE_SHEET_URL,
    }


# ─────────────────────────────────────────────────────────────
#  FLASK DATA SERVER
# ─────────────────────────────────────────────────────────────
app = Flask(__name__)
# Suppress Flask request logs (keep our own print statements clean)
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

@app.route("/api/data")
def api_data():
    """Called by the browser on every page load/refresh.
    Re-downloads the sheet and returns fresh metrics as JSON."""
    try:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Browser requested fresh data …")
        download_sheet(GOOGLE_SHEET_URL, CACHE_FILE)
        df = load_data(CACHE_FILE)
        m  = compute_metrics(df)
        print(f"  ✓ {m['n_shifts']} shifts | avg_w={m['avg_filler']:.4f}g | TC delta={m['tc_delta']:,.0f}")
        return jsonify({"ok": True, "data": m})
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/status")
def api_status():
    return jsonify({"ok": True, "port": SERVER_PORT, "time": datetime.now().isoformat()})

@app.route("/")
def serve_dashboard():
    """Serve the dashboard HTML file directly."""
    return send_file(OUTPUT_HTML)


# ─────────────────────────────────────────────────────────────
#  HTML GENERATION  (generated once; JS fetches live data)
# ─────────────────────────────────────────────────────────────
def build_html(m: dict) -> str:
    sp   = m["setpoint"]
    port = SERVER_PORT
    generated = datetime.now().strftime("%d %b %Y %H:%M")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Production Filler Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#0f1117;--bg2:#181c27;--bg3:#1e2333;
  --border:rgba(255,255,255,0.07);
  --text:#e8eaf0;--muted:#7b82a0;
  --accent:#6366F1;--green:#34D399;--red:#F87171;--amber:#FBBF24;
  --mono:'DM Mono',monospace;--display:'Syne',sans-serif;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--mono);font-size:13px;min-height:100vh;padding:2rem}}

/* ── Loading overlay ── */
#loading-overlay{{
  position:fixed;inset:0;background:var(--bg);z-index:9999;
  display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;
  transition:opacity 0.4s;
}}
#loading-overlay.hidden{{opacity:0;pointer-events:none}}
.spinner{{
  width:36px;height:36px;border:3px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;
  animation:spin 0.8s linear infinite;
}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
#loading-msg{{font-family:var(--display);font-size:14px;color:var(--muted)}}
#loading-detail{{font-size:11px;color:var(--muted);opacity:0.6}}

/* ── Error banner ── */
#error-banner{{
  display:none;background:rgba(248,113,113,0.1);border:1px solid rgba(248,113,113,0.3);
  border-radius:8px;padding:12px 16px;margin-bottom:1.5rem;color:#F87171;font-size:12px;
}}

/* ── Refresh bar ── */
#refresh-bar{{
  display:flex;align-items:center;justify-content:space-between;
  background:var(--bg2);border:1px solid var(--border);border-radius:8px;
  padding:8px 14px;margin-bottom:1.5rem;font-size:11px;color:var(--muted);
}}
#refresh-bar strong{{color:var(--text)}}
.refresh-btn{{
  font-family:var(--mono);font-size:11px;padding:4px 14px;
  border-radius:5px;border:1px solid rgba(99,102,241,0.4);
  background:rgba(99,102,241,0.1);color:#818CF8;cursor:pointer;transition:all 0.15s;
}}
.refresh-btn:hover{{background:rgba(99,102,241,0.2)}}
.dot-live{{
  display:inline-block;width:7px;height:7px;border-radius:50%;
  background:var(--green);margin-right:6px;
  animation:pulse 2s ease-in-out infinite;
}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:0.3}}}}

/* ── Page tabs ── */
.page-tabs{{display:flex;gap:0;margin-bottom:2rem;border-bottom:1px solid var(--border)}}
.page-tab{{
  font-family:var(--display);font-size:13px;font-weight:600;
  padding:10px 24px;cursor:pointer;color:var(--muted);
  background:transparent;border:none;border-bottom:2px solid transparent;
  margin-bottom:-1px;transition:all 0.18s;letter-spacing:0.01em;
}}
.page-tab:hover{{color:var(--text)}}
.page-tab.active{{color:#fff;border-bottom-color:var(--accent)}}
.page-content{{display:none}}.page-content.active{{display:block}}

/* ── Header ── */
.header{{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:1.75rem}}
.header-left h1{{font-family:var(--display);font-size:26px;font-weight:700;letter-spacing:-0.02em;color:#fff}}
.header-left p{{color:var(--muted);font-size:12px;margin-top:4px}}
.header-right{{text-align:right;color:var(--muted);font-size:11px;line-height:1.7}}

/* ── Section label ── */
.section-label{{font-family:var(--display);font-size:10px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:var(--muted);margin:2rem 0 0.75rem}}

/* ── KPI ── */
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}
.kpi{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px 18px;position:relative;overflow:hidden;transition:opacity 0.3s}}
.kpi.loading{{opacity:0.4}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);opacity:0.5}}
.kpi.good::before{{background:var(--green)}}.kpi.bad::before{{background:var(--red)}}.kpi.warn::before{{background:var(--amber)}}
.kpi-label{{font-size:11px;color:var(--muted);margin-bottom:6px}}
.kpi-value{{font-family:var(--display);font-size:24px;font-weight:700;color:#fff;letter-spacing:-0.02em}}
.kpi-value.red{{color:var(--red)}}.kpi-value.green{{color:var(--green)}}.kpi-value.amber{{color:var(--amber)}}
.kpi-sub{{font-size:11px;color:var(--muted);margin-top:4px}}

/* ── Charts ── */
.chart-grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}}
.chart-card{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px 18px 12px}}
.chart-card-full{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px 18px 12px;margin-top:14px}}
.chart-title{{font-family:var(--display);font-size:13px;font-weight:600;color:#fff;margin-bottom:2px}}
.chart-sub{{font-size:11px;color:var(--muted);margin-bottom:12px}}
.legend{{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:10px;font-size:11px;color:var(--muted);align-items:center}}
.leg-dot{{width:8px;height:8px;border-radius:2px;display:inline-block;margin-right:4px;flex-shrink:0}}

/* ── Shift tabs ── */
.tabs{{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}}
.tab{{font-family:var(--mono);font-size:11px;padding:4px 10px;border-radius:5px;border:1px solid var(--border);background:transparent;color:var(--muted);cursor:pointer;transition:all 0.15s}}
.tab:hover{{border-color:rgba(99,102,241,0.4);color:var(--text)}}
.tab.active{{background:rgba(99,102,241,0.15);border-color:rgba(99,102,241,0.5);color:#818CF8}}

/* ── Tables ── */
.detail-table{{width:100%;border-collapse:collapse;font-size:12px}}
.detail-table th{{font-size:10px;font-weight:500;color:var(--muted);text-align:left;padding:6px 10px;border-bottom:1px solid var(--border);letter-spacing:0.06em;text-transform:uppercase}}
.detail-table td{{padding:7px 10px;border-bottom:1px solid var(--border);color:var(--text)}}
.detail-table tr:last-child td{{border-bottom:none}}
.detail-table tfoot td{{color:#fff;font-weight:500;background:rgba(99,102,241,0.05)}}
.pill{{display:inline-block;font-size:10px;padding:2px 7px;border-radius:4px;font-weight:500}}
.pill-ok{{background:rgba(52,211,153,0.12);color:#34D399}}
.pill-warn{{background:rgba(251,191,36,0.12);color:#FBBF24}}
.pill-bad{{background:rgba(248,113,113,0.12);color:#F87171}}
.pill-info{{background:rgba(99,102,241,0.15);color:#818CF8}}

/* ── Footer ── */
.footer{{margin-top:2.5rem;padding-top:1rem;border-top:1px solid var(--border);color:var(--muted);font-size:11px;display:flex;justify-content:space-between}}
</style>
</head>
<body>

<!-- Loading overlay (shown while fetching data) -->
<div id="loading-overlay">
  <div class="spinner"></div>
  <div id="loading-msg">Connecting to Google Sheets…</div>
  <div id="loading-detail">Downloading latest data</div>
</div>

<div id="error-banner"></div>

<!-- Live data refresh bar -->
<div id="refresh-bar">
  <span><span class="dot-live"></span>Live data — fetched from Google Sheets on every page load</span>
  <span id="last-updated-label">Loading…</span>
  <button class="refresh-btn" onclick="fetchData()">↻ Refresh now</button>
</div>

<div class="header">
  <div class="header-left">
    <h1>Production Filler Dashboard</h1>
    <p id="dash-subtitle">Loading…</p>
  </div>
  <div class="header-right" id="dash-header-right">
    Setpoint: <strong style="color:var(--amber)">{sp} Kg</strong> &nbsp;|&nbsp; σ = {SIGMA}
  </div>
</div>

<!-- PAGE TABS -->
<div class="page-tabs">
  <button class="page-tab active" onclick="switchTab('shift',this)">Shift Analysis</button>
  <button class="page-tab"        onclick="switchTab('counter',this)">Counter Analysis</button>
</div>

<!-- ════ TAB 1 — SHIFT ANALYSIS ════ -->
<div id="tab-shift" class="page-content active">

  <div class="section-label">Overall summary</div>
  <div class="kpi-grid" id="kpi-top">
    <div class="kpi loading"><div class="kpi-label">Total pieces counted</div><div class="kpi-value" id="k-total-pc">—</div><div class="kpi-sub" id="k-total-pc-sub">—</div></div>
    <div class="kpi loading"><div class="kpi-label">Total weight (Kg)</div><div class="kpi-value" id="k-total-wt">—</div><div class="kpi-sub">Sum across all tracks</div></div>
    <div class="kpi loading" id="k-avg-w-card"><div class="kpi-label">Avg filler box weight</div><div class="kpi-value" id="k-avg-w">—</div><div class="kpi-sub">Setpoint: {sp} Kg</div></div>
    <div class="kpi loading"><div class="kpi-label">Avg production Pieces rate</div><div class="kpi-value" id="k-avg-rate">—</div><div class="kpi-sub">Pieces / shift</div></div>
  </div>

  <div class="kpi-grid" style="margin-top:12px">
    <div class="kpi loading" id="k-over-card"><div class="kpi-label">Total overfilling</div><div class="kpi-value" id="k-over">—</div><div class="kpi-sub" id="k-over-sub">—</div></div>
    <div class="kpi loading"><div class="kpi-label">Avg overfilling / shift</div><div class="kpi-value" id="k-avg-over">—</div><div class="kpi-sub" id="k-avg-over-sub">—</div></div>
    <div class="kpi loading" id="k-acc-card"><div class="kpi-label">Device accuracy</div><div class="kpi-value" id="k-acc">—</div><div class="kpi-sub">100 − ((avg_w − 10) / 10)</div></div>
    <div class="kpi loading" id="k-uf-card"><div class="kpi-label">Shifts with underfilling</div><div class="kpi-value" id="k-uf">—</div><div class="kpi-sub" id="k-uf-sub">—</div></div>
  </div>

  <div class="section-label">Shift trends</div>
  <div class="chart-grid-2">
    <div class="chart-card">
      <div class="chart-title">Pieces produced per shift</div>
      <div class="chart-sub">Night vs light</div>
      <div class="legend"><span><span class="leg-dot" style="background:#6366F1"></span>Night</span><span><span class="leg-dot" style="background:#34D399"></span>Light</span></div>
      <div style="position:relative;height:200px"><canvas id="c1"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Avg filler box weight per shift</div>
      <div class="chart-sub">Dashed = setpoint ({sp} Kg)</div>
      <div class="legend"><span><span class="leg-dot" style="background:#F59E0B"></span>Avg weight</span><span><span class="leg-dot" style="background:#6366F1;opacity:.5"></span>Setpoint</span></div>
      <div style="position:relative;height:200px"><canvas id="c2"></canvas></div>
    </div>
  </div>
  <div class="chart-grid-2" style="margin-top:14px">
    <div class="chart-card">
      <div class="chart-title">Overfilling per shift (Kg)</div>
      <div class="chart-sub">Red = underfilling</div>
      <div style="position:relative;height:200px"><canvas id="c3"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Track share of total production</div>
      <div class="chart-sub">Cumulative pieces per track</div>
      <div class="legend" id="donut1-legend"></div>
      <div style="position:relative;height:200px"><canvas id="c4"></canvas></div>
    </div>
  </div>
  <div class="chart-card-full">
    <div class="chart-title">Avg box weight per track — all shifts</div>
    <div class="chart-sub">Setpoint = {sp} Kg (dashed)</div>
    <div class="legend"><span><span class="leg-dot" style="background:#6366F1"></span>Track 1</span><span><span class="leg-dot" style="background:#34D399"></span>Track 2</span><span><span class="leg-dot" style="background:#F59E0B"></span>Track 3</span><span><span class="leg-dot" style="background:#6b7280;opacity:.6"></span>Setpoint</span></div>
    <div style="position:relative;height:210px"><canvas id="c5"></canvas></div>
  </div>
  <div class="chart-card-full">
    <div class="chart-title">Per-shift detail</div>
    <div class="chart-sub">Click a shift to inspect track-level statistics</div>
    <div class="tabs" id="shift-tabs"></div>
    <div id="shift-table"></div>
  </div>
</div><!-- /tab-shift -->

<!-- ════ TAB 2 — COUNTER ANALYSIS ════ -->
<div id="tab-counter" class="page-content">

  <div class="section-label">Counter summary</div>
  <div class="kpi-grid">
    <div class="kpi loading"><div class="kpi-label">Total counter (TC) start</div><div class="kpi-value amber" id="k-tc-start">—</div><div class="kpi-sub">Baseline (shift 0)</div></div>
    <div class="kpi loading"><div class="kpi-label">Total counter (TC) end</div><div class="kpi-value" id="k-tc-end">—</div><div class="kpi-sub" id="k-tc-end-sub">—</div></div>
    <div class="kpi loading good"><div class="kpi-label">TC delta (total run)</div><div class="kpi-value green" id="k-tc-delta">—</div><div class="kpi-sub">Boxes counted by TC</div></div>
    <div class="kpi loading" id="k-gap-card"><div class="kpi-label">Total gap (TC − weight)</div><div class="kpi-value" id="k-gap">—</div><div class="kpi-sub" id="k-gap-sub">—</div></div>
  </div>
  <div class="kpi-grid" style="margin-top:12px">
    <div class="kpi loading"><div class="kpi-label">Track 1 delta</div><div class="kpi-value" id="k-t1d">—</div><div class="kpi-sub" id="k-t1d-sub">—</div></div>
    <div class="kpi loading"><div class="kpi-label">Track 2 delta</div><div class="kpi-value" id="k-t2d">—</div><div class="kpi-sub" id="k-t2d-sub">—</div></div>
    <div class="kpi loading"><div class="kpi-label">Track 3 delta</div><div class="kpi-value" id="k-t3d">—</div><div class="kpi-sub" id="k-t3d-sub">—</div></div>
    <div class="kpi loading"><div class="kpi-label">Avg TC increment / shift</div><div class="kpi-value" id="k-avg-tc">—</div><div class="kpi-sub">Boxes per shift (TC)</div></div>
  </div>

  <div class="section-label">Cumulative counter growth</div>
  <div class="chart-card-full" style="margin-top:0">
    <div class="chart-title">Cumulative counters — all shifts</div>
    <div class="chart-sub">T1, T2, T3 individual tracks vs TC master counter</div>
    <div class="legend"><span><span class="leg-dot" style="background:#6366F1"></span>Track 1</span><span><span class="leg-dot" style="background:#34D399"></span>Track 2</span><span><span class="leg-dot" style="background:#F59E0B"></span>Track 3</span><span><span class="leg-dot" style="background:#e879f9"></span>TC (total)</span></div>
    <div style="position:relative;height:240px"><canvas id="cc1"></canvas></div>
  </div>
  <div class="chart-grid-2">
    <div class="chart-card">
      <div class="chart-title">Per-shift TC increment</div>
      <div class="chart-sub">Boxes counted by TC each shift</div>
      <div style="position:relative;height:210px"><canvas id="cc2"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Per-shift track increments (stacked)</div>
      <div class="chart-sub">T1 + T2 + T3 per shift</div>
      <div class="legend"><span><span class="leg-dot" style="background:#6366F1"></span>T1</span><span><span class="leg-dot" style="background:#34D399"></span>T2</span><span><span class="leg-dot" style="background:#F59E0B"></span>T3</span></div>
      <div style="position:relative;height:210px"><canvas id="cc3"></canvas></div>
    </div>
  </div>
  <div class="chart-grid-2">
    <div class="chart-card">
      <div class="chart-title">Gap per shift (TC − DC)</div>
      <div class="chart-sub">Amber = positive · Red = negative</div>
      <div style="position:relative;height:200px"><canvas id="cc4"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Track share of TC delta</div>
      <div class="chart-sub">Proportional contribution to total count</div>
      <div class="legend" id="donut2-legend"></div>
      <div style="position:relative;height:200px"><canvas id="cc5"></canvas></div>
    </div>
  </div>
  <div class="chart-card-full">
    <div class="chart-title">Per-shift counter detail</div>
    <div class="chart-sub">Cumulative values, increments, and gap per shift</div>
    <div style="overflow-x:auto">
    <table class="detail-table" style="min-width:800px">
      <thead><tr>
        <th>Shift</th><th>Type</th><th>Date</th>
        <th>T T1</th><th>T T2</th><th>T T3</th><th>TC</th><th>Diff total</th>
        <th>Inc T1</th><th>Inc T2</th><th>Inc T3</th><th>Inc TC</th>
        <th>Gap</th><th>Gap %</th>
      </tr></thead>
      <tbody id="counter-tbody"></tbody>
      <tfoot id="counter-tfoot"></tfoot>
    </table>
    </div>
  </div>
</div><!-- /tab-counter -->

<div class="footer">
  <span>Production Filler Dashboard &nbsp;·&nbsp; dashboard_generator_v3.py</span>
  <span id="footer-generated">Generated: {generated}</span>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const SETPOINT = {sp};
const API      = '/api/data';
const charts   = {{}};  // chart registry — destroyed & rebuilt on each refresh

// ── helpers ──────────────────────────────────────────────────
const gc='rgba(255,255,255,0.06)', mc='#7b82a0';
const fn=v=>v.toLocaleString();
const fp=v=>v>=0?'+'+v:String(v);
const sign=v=>v>=0?'+':'';

function baseOpts(yOpts={{}}){{
  return {{
    responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}}}},
    scales:{{
      x:{{grid:{{color:gc}},ticks:{{color:mc,font:{{size:10,family:"'DM Mono',monospace"}},maxRotation:45,autoSkip:false}}}},
      y:{{grid:{{color:gc}},ticks:{{color:mc,font:{{size:10,family:"'DM Mono',monospace"}},...(yOpts.ticks||{{}})}},...yOpts}}
    }}
  }};
}}

function mkChart(id, config){{
  if(charts[id]) charts[id].destroy();
  charts[id]=new Chart(document.getElementById(id), config);
}}

function pill(w){{
  if(!w) return '—';
  const v=parseFloat(w);
  if(v>=SETPOINT-0.006&&v<=SETPOINT+0.006) return `<span class="pill pill-ok">${{v.toFixed(4)}}</span>`;
  if(v>=9.95) return `<span class="pill pill-warn">${{v.toFixed(4)}}</span>`;
  return `<span class="pill pill-bad">${{v.toFixed(4)}}</span>`;
}}
function statusPill(w){{
  if(!w) return '—';
  return(w>=9.90&&w<=10.10)?'<span class="pill pill-ok">OK</span>':'<span class="pill pill-bad">Check</span>';
}}
function gapPill(g){{
  const a=Math.abs(g);
  const cls=a<10?'pill-ok':a<100?'pill-warn':'pill-bad';
  return `<span class="pill ${{cls}}">${{g>=0?'+':''}}${{g.toLocaleString()}}</span>`;
}}
function gapPctPill(p){{
  const v=parseFloat(p),a=Math.abs(v);
  const cls=a<0.05?'pill-ok':a<0.2?'pill-warn':'pill-bad';
  return `<span class="pill ${{cls}}">${{v>=0?'+':''}}${{v.toFixed(3)}}%</span>`;
}}

// ── render everything from API data ──────────────────────────
function renderDashboard(m){{
  const s=m.shifts;
  const labels    =s.map(x=>`S${{x.shift}}`);
  const fullLabels=s.map(x=>`Shift ${{x.shift}} · ${{x.date}} (${{x.type}})`);

  // ── KPIs tab 1 ──
  document.querySelectorAll('.kpi').forEach(el=>el.classList.remove('loading'));
  document.getElementById('k-total-pc').textContent     = fn(m.total_pc);
  document.getElementById('k-total-pc-sub').textContent = `Across ${{m.n_shifts}} shifts`;
  document.getElementById('k-total-wt').textContent     = fn(m.total_wt);
  document.getElementById('k-avg-w').textContent        = m.avg_filler.toFixed(4)+' Kg';
  const awCard=document.getElementById('k-avg-w-card');
  awCard.className='kpi '+(Math.abs(m.avg_filler-SETPOINT)>0.02?'warn':'good');
  document.getElementById('k-avg-rate').textContent     = fn(Math.round(m.avg_rate));
  const ov=m.total_over;
  document.getElementById('k-over').textContent         = (ov>=0?'+':'')+ov.toFixed(2)+' Kg';
  document.getElementById('k-over').className           = 'kpi-value'+(ov<0?' red':'');
  document.getElementById('k-over-card').className      = 'kpi'+(ov<0?' bad':'');
  document.getElementById('k-over-sub').textContent     = `Avg ${{(ov/m.n_shifts).toFixed(2)}} Kg / shift`;
  document.getElementById('k-avg-over').textContent     = (m.avg_over_per_shift>=0?'+':'')+m.avg_over_per_shift.toFixed(2)+' Kg';
  document.getElementById('k-avg-over-sub').textContent = `Total ÷ ${{m.n_shifts}} shifts`;
  document.getElementById('k-acc').textContent          = m.device_accuracy.toFixed(3)+'%';
  document.getElementById('k-acc').className            = 'kpi-value'+(m.device_accuracy<99.9?' red':' green');
  document.getElementById('k-acc-card').className       = 'kpi'+(m.device_accuracy<99.9?' bad':' good');
  document.getElementById('k-uf').textContent           = m.underfill_n;
  document.getElementById('k-uf').className             = 'kpi-value'+(m.underfill_n>0?' red':' green');
  document.getElementById('k-uf-card').className        = 'kpi'+(m.underfill_n>0?' bad':' good');
  document.getElementById('k-uf-sub').textContent       = `Of ${{m.n_shifts}} total`;

  // ── KPIs tab 2 ──
  document.getElementById('k-tc-start').textContent  = fn(m.tc_start);
  document.getElementById('k-tc-end').textContent    = fn(m.tc_end);
  document.getElementById('k-tc-end-sub').textContent= `After shift ${{m.n_shifts}}`;
  document.getElementById('k-tc-delta').textContent  = fn(m.tc_delta);
  document.getElementById('k-gap').textContent       = (m.total_gap>=0?'+':'')+fn(Math.round(m.total_gap));
  document.getElementById('k-gap').className         = 'kpi-value'+(Math.abs(m.gap_pct_total)>1?' red':' amber');
  document.getElementById('k-gap-card').className    = 'kpi'+(Math.abs(m.gap_pct_total)>1?' bad':' warn');
  document.getElementById('k-gap-sub').textContent   = (m.gap_pct_total>=0?'+':'')+m.gap_pct_total.toFixed(3)+'% of TC delta';
  document.getElementById('k-t1d').textContent       = fn(m.t1_delta);
  document.getElementById('k-t1d-sub').textContent   = m.t1d_pct+'% of TC delta';
  document.getElementById('k-t2d').textContent       = fn(m.t2_delta);
  document.getElementById('k-t2d-sub').textContent   = m.t2d_pct+'% of TC delta';
  document.getElementById('k-t3d').textContent       = fn(m.t3_delta);
  document.getElementById('k-t3d-sub').textContent   = m.t3d_pct+'% of TC delta';
  document.getElementById('k-avg-tc').textContent    = fn(Math.round(m.tc_delta/m.n_shifts));

  // ── Header ──
  document.getElementById('dash-subtitle').textContent =
    `Source: Google Sheets · ${{m.date_range}} · ${{m.n_shifts}} shifts`;
  document.getElementById('last-updated-label').innerHTML =
    `Last updated: <strong>${{m.last_updated}}</strong>`;

  // ── Y bounds for weight charts ──
  const allW=s.flatMap(x=>[x.avg_w1,x.avg_w2,x.avg_w3,x.avg_all].filter(Boolean));
  const yMin=allW.length?+(Math.min(...allW,SETPOINT)-0.08).toFixed(2):9.90;
  const yMax=allW.length?+(Math.max(...allW,SETPOINT)+0.08).toFixed(2):10.12;

  // ── Tab 1 charts ──
  mkChart('c1',{{type:'bar',
    data:{{labels,datasets:[{{
      data:s.map(x=>x.total_boxes),
      backgroundColor:s.map(x=>x.type==='night'?'#4F46E5':'#34D399'),
      borderRadius:4,borderSkipped:false
    }}]}},
    options:{{...baseOpts(),plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{title:i=>fullLabels[i[0].dataIndex]}}}}}}}},
  }});

  mkChart('c2',{{type:'line',
    data:{{labels,datasets:[
      {{data:s.map(x=>x.avg_all),borderColor:'#F59E0B',backgroundColor:'rgba(245,158,11,0.08)',tension:0.35,fill:true,pointRadius:4,pointBackgroundColor:'#F59E0B',pointBorderWidth:0}},
      {{data:labels.map(()=>SETPOINT),borderColor:'rgba(99,102,241,0.45)',borderDash:[5,4],pointRadius:0,tension:0}}
    ]}},
    options:baseOpts({{min:yMin,max:yMax,ticks:{{callback:v=>v.toFixed(2)}}}})
  }});

  mkChart('c3',{{type:'bar',
    data:{{labels,datasets:[{{
      data:s.map(x=>x.total_over_g),
      backgroundColor:s.map(x=>x.total_over_g<0?'#EF4444':'#6366F1'),
      borderRadius:4,borderSkipped:false
    }}]}},
    options:baseOpts()
  }});

  document.getElementById('donut1-legend').innerHTML=
    `<span><span class="leg-dot" style="background:#6366F1"></span>T1 (${{m.t1_pct}}%)</span>
     <span><span class="leg-dot" style="background:#34D399"></span>T2 (${{m.t2_pct}}%)</span>
     <span><span class="leg-dot" style="background:#F59E0B"></span>T3 (${{m.t3_pct}}%)</span>`;
  mkChart('c4',{{type:'doughnut',
    data:{{labels:['T1','T2','T3'],datasets:[{{data:[m.t1_total,m.t2_total,m.t3_total],backgroundColor:['#6366F1','#34D399','#F59E0B'],borderWidth:0,hoverOffset:8}}]}},
    options:{{responsive:true,maintainAspectRatio:false,cutout:'62%',plugins:{{legend:{{display:false}}}}}}
  }});

  mkChart('c5',{{type:'line',
    data:{{labels,datasets:[
      {{label:'T1',data:s.map(x=>x.avg_w1),borderColor:'#6366F1',tension:0.35,pointRadius:3,fill:false,spanGaps:true}},
      {{label:'T2',data:s.map(x=>x.avg_w2),borderColor:'#34D399',tension:0.35,pointRadius:3,fill:false,spanGaps:true}},
      {{label:'T3',data:s.map(x=>x.avg_w3),borderColor:'#F59E0B',tension:0.35,pointRadius:3,fill:false,spanGaps:true}},
      {{data:labels.map(()=>SETPOINT),borderColor:'rgba(107,114,128,0.45)',borderDash:[5,4],pointRadius:0}}
    ]}},
    options:baseOpts({{min:yMin,max:yMax,ticks:{{callback:v=>v.toFixed(2)}}}})
  }});

  // ── Tab 2 charts ──
  mkChart('cc1',{{type:'line',
    data:{{labels,datasets:[
      {{label:'T T1',data:s.map(x=>x.tt1),borderColor:'#6366F1',tension:0.3,pointRadius:3,fill:false}},
      {{label:'T T2',data:s.map(x=>x.tt2),borderColor:'#34D399',tension:0.3,pointRadius:3,fill:false}},
      {{label:'T T3',data:s.map(x=>x.tt3),borderColor:'#F59E0B',tension:0.3,pointRadius:3,fill:false}},
      {{label:'TC',  data:s.map(x=>x.tc), borderColor:'#e879f9',tension:0.3,pointRadius:3,fill:false,borderWidth:2.5}}
    ]}},
    options:baseOpts({{ticks:{{callback:v=>v.toLocaleString()}}}})
  }});

  mkChart('cc2',{{type:'bar',
    data:{{labels,datasets:[{{data:s.map(x=>x.inc_tc),backgroundColor:'rgba(232,121,249,0.7)',borderRadius:4,borderSkipped:false}}]}},
    options:baseOpts({{ticks:{{callback:v=>v.toLocaleString()}}}})
  }});

  mkChart('cc3',{{type:'bar',
    data:{{labels,datasets:[
      {{label:'T1',data:s.map(x=>x.inc_t1),backgroundColor:'rgba(99,102,241,0.75)',stack:'s'}},
      {{label:'T2',data:s.map(x=>x.inc_t2),backgroundColor:'rgba(52,211,153,0.75)',stack:'s'}},
      {{label:'T3',data:s.map(x=>x.inc_t3),backgroundColor:'rgba(245,158,11,0.75)',stack:'s'}}
    ]}},
    options:baseOpts({{ticks:{{callback:v=>v.toLocaleString()}}}})
  }});

  mkChart('cc4',{{type:'bar',
    data:{{labels,datasets:[{{
      data:s.map(x=>x.gap),
      backgroundColor:s.map(x=>x.gap<0?'#EF4444':'#F59E0B'),
      borderRadius:4,borderSkipped:false
    }}]}},
    options:baseOpts()
  }});

  document.getElementById('donut2-legend').innerHTML=
    `<span><span class="leg-dot" style="background:#6366F1"></span>T1 (${{m.t1d_pct}}%)</span>
     <span><span class="leg-dot" style="background:#34D399"></span>T2 (${{m.t2d_pct}}%)</span>
     <span><span class="leg-dot" style="background:#F59E0B"></span>T3 (${{m.t3d_pct}}%)</span>`;
  mkChart('cc5',{{type:'doughnut',
    data:{{labels:['T1','T2','T3'],datasets:[{{data:[m.t1_delta,m.t2_delta,m.t3_delta],backgroundColor:['#6366F1','#34D399','#F59E0B'],borderWidth:0,hoverOffset:8}}]}},
    options:{{responsive:true,maintainAspectRatio:false,cutout:'62%',plugins:{{legend:{{display:false}}}}}}
  }});

  // ── Per-shift detail table ──
  const tabsEl=document.getElementById('shift-tabs');
  tabsEl.innerHTML='';
  s.forEach((sh,i)=>{{
    const btn=document.createElement('button');
    btn.className='tab'+(i===0?' active':'');
    btn.textContent=`S${{sh.shift}} · ${{sh.type==='night'?'N':'L'}} ${{sh.date}}`;
    btn.onclick=()=>{{
      document.querySelectorAll('#shift-tabs .tab').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      renderShiftTable(s,i);
    }};
    tabsEl.appendChild(btn);
  }});
  if(s.length) renderShiftTable(s,0);

  // ── Counter detail table ──
  const tbody=document.getElementById('counter-tbody');
  tbody.innerHTML='';
  let totT1=0,totT2=0,totT3=0,totTC=0,totGap=0;
  s.forEach(sh=>{{
    totT1+=sh.inc_t1;totT2+=sh.inc_t2;totT3+=sh.inc_t3;totTC+=sh.inc_tc;totGap+=sh.gap;
    const tr=document.createElement('tr');
    tr.innerHTML=`
      <td><span class="pill pill-info">S${{sh.shift}}</span></td>
      <td><span class="pill ${{sh.type==='night'?'pill-warn':'pill-ok'}}">${{sh.type}}</span></td>
      <td>${{sh.date}}</td>
      <td>${{fn(sh.tt1)}}</td><td>${{fn(sh.tt2)}}</td><td>${{fn(sh.tt3)}}</td><td>${{fn(sh.tc)}}</td>
      <td style="color:#7b82a0">${{sign(sh.diff_total)}}${{fn(sh.diff_total)}}</td>
      <td style="color:#6366F1">+${{fn(sh.inc_t1)}}</td>
      <td style="color:#34D399">+${{fn(sh.inc_t2)}}</td>
      <td style="color:#F59E0B">+${{fn(sh.inc_t3)}}</td>
      <td style="color:#e879f9">+${{fn(sh.inc_tc)}}</td>
      <td>${{gapPill(sh.gap)}}</td>
      <td>${{gapPctPill(sh.gap_pct)}}</td>`;
    tbody.appendChild(tr);
  }});
  const gpt=totTC>0?totGap/totTC*100:0;
  const finalTC=s.length?s[s.length-1].tc:0;
  document.getElementById('counter-tfoot').innerHTML=`<tr>
    <td colspan="3">Totals</td><td>—</td><td>—</td><td>—</td>
    <td style="color:#e879f9">${{fn(finalTC)}}</td><td>—</td>
    <td style="color:#6366F1">+${{fn(totT1)}}</td>
    <td style="color:#34D399">+${{fn(totT2)}}</td>
    <td style="color:#F59E0B">+${{fn(totT3)}}</td>
    <td style="color:#e879f9">${{fn(totTC)}}</td>
    <td>${{gapPill(totGap)}}</td>
    <td>${{gapPctPill(gpt)}}</td>
  </tr>`;
}}

function renderShiftTable(s,i){{
  const sh=s[i];
  document.getElementById('shift-table').innerHTML=`
  <table class="detail-table">
    <thead><tr><th>Track</th><th>Boxes</th><th>Weight (Kg)</th><th>Avg weight</th><th>Giveaway (g/box)</th><th>Overfill (Kg)</th><th>Status</th></tr></thead>
    <tbody>
      <tr><td><strong>Track 1</strong></td><td>${{fn(sh.b1)}}</td><td>${{fn(sh.w1)}}</td>
        <td>${{pill(sh.avg_w1)}}</td>
        <td style="color:${{sh.give_per_box1>=0?'#34D399':'#F87171'}}">${{sign(sh.give_per_box1)}}${{sh.give_per_box1!=null?sh.give_per_box1.toFixed(2):'—'}}</td>
        <td style="color:${{sh.over_g1<0?'#F87171':'#34D399'}}">${{sign(sh.over_g1)}}${{sh.over_g1}}</td>
        <td>${{statusPill(sh.avg_w1)}}</td></tr>
      <tr><td><strong>Track 2</strong></td><td>${{fn(sh.b2)}}</td><td>${{fn(sh.w2)}}</td>
        <td>${{pill(sh.avg_w2)}}</td>
        <td style="color:${{sh.give_per_box2>=0?'#34D399':'#F87171'}}">${{sign(sh.give_per_box2)}}${{sh.give_per_box2!=null?sh.give_per_box2.toFixed(2):'—'}}</td>
        <td style="color:${{sh.over_g2<0?'#F87171':'#34D399'}}">${{sign(sh.over_g2)}}${{sh.over_g2}}</td>
        <td>${{statusPill(sh.avg_w2)}}</td></tr>
      <tr><td><strong>Track 3</strong></td><td>${{fn(sh.b3)}}</td><td>${{fn(sh.w3)}}</td>
        <td>${{pill(sh.avg_w3)}}</td>
        <td style="color:${{sh.give_per_box3>=0?'#34D399':'#F87171'}}">${{sign(sh.give_per_box3)}}${{sh.give_per_box3!=null?sh.give_per_box3.toFixed(2):'—'}}</td>
        <td style="color:${{sh.over_g3<0?'#F87171':'#34D399'}}">${{sign(sh.over_g3)}}${{sh.over_g3}}</td>
        <td>${{statusPill(sh.avg_w3)}}</td></tr>
    </tbody>
    <tfoot><tr>
      <td>Total</td><td>${{fn(sh.total_boxes)}}</td><td>${{fn(sh.total_weight)}}</td>
      <td>${{pill(sh.avg_all)}}</td><td>—</td>
      <td style="color:${{sh.total_over_g<0?'#F87171':'#34D399'}}">${{sign(sh.total_over_g)}}${{sh.total_over_g}}</td>
      <td>DC: ${{fn(sh.dc)}}</td>
    </tr></tfoot>
  </table>`;
}}

// ── Main fetch ────────────────────────────────────────────────
async function fetchData(){{
  const overlay=document.getElementById('loading-overlay');
  const errBanner=document.getElementById('error-banner');
  overlay.classList.remove('hidden');
  errBanner.style.display='none';
  document.getElementById('loading-msg').textContent='Downloading from Google Sheets…';
  document.getElementById('loading-detail').textContent='This takes a few seconds';
  try{{
    const res=await fetch(API);
    if(!res.ok) throw new Error(`Server error ${{res.status}}`);
    const json=await res.json();
    if(!json.ok) throw new Error(json.error||'Unknown error');
    renderDashboard(json.data);
    overlay.classList.add('hidden');
  }} catch(e){{
    overlay.classList.add('hidden');
    errBanner.style.display='block';
    errBanner.innerHTML=`<strong>Could not load data:</strong> ${{e.message}}<br>
      <small>Make sure dashboard_generator_v3.py is still running in your terminal.</small>`;
  }}
}}

function switchTab(name,btn){{
  document.querySelectorAll('.page-tab').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.page-content').forEach(p=>p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('tab-'+name).classList.add('active');
}}

// Fetch on every page load / refresh
fetchData();
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────
def validate_config():
    if "YOUR_SPREADSHEET_ID" in GOOGLE_SHEET_URL:
        print("─" * 60)
        print("  ERROR: GOOGLE_SHEET_URL is not configured!")
        print("  Open dashboard_generator_v3.py and set GOOGLE_SHEET_URL")
        print("  to your Google Sheet share link.")
        print("─" * 60)
        sys.exit(1)


def generate_html_shell(m: dict):
    """Write the HTML file (once at startup with initial data)."""
    html = build_html(m)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Dashboard : {OUTPUT_HTML}")


def open_browser():
    time.sleep(1.2)
    url = f"http://localhost:{SERVER_PORT}"
    print(f"  Opening   : {url}")
    webbrowser.open(url)


def main():
    validate_config()

    print("=" * 60)
    print("  Production Filler Dashboard  v3")
    print("=" * 60)

    # ── Initial download + build ──
    print("\n[1/3] Downloading Google Sheet …")
    download_sheet(GOOGLE_SHEET_URL, CACHE_FILE)

    print("[2/3] Processing data …")
    df = load_data(CACHE_FILE)
    m  = compute_metrics(df)
    print(f"  Shifts    : {m['n_shifts']}  |  total_pc={m['total_pc']:,.0f}  "
          f"avg_w={m['avg_filler']:.4f}g  underfill={m['underfill_n']}")
    print(f"  Counters  : TC delta={m['tc_delta']:,.0f}  "
          f"gap={m['total_gap']:+,.0f} ({m['gap_pct_total']:+.3f}%)")

    print("[3/3] Generating dashboard HTML …")
    generate_html_shell(m)

    print(f"\n  Server    : http://localhost:{SERVER_PORT}")
    print("  Every browser refresh re-downloads the Google Sheet.")
    print("  Press Ctrl+C to stop.\n")

    if AUTO_OPEN:
        threading.Thread(target=open_browser, daemon=True).start()

    # ── Start Flask (blocking) ──
    app.run(host="0.0.0.0", port=SERVER_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
