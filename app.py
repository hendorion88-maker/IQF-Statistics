"""
IQF Unified Dashboard
======================
Merges:
  • Filler Statistics  (Google Sheets data, Chart.js → Plotly)
  • SCADA Analysis     (Data_log0.csv + Alarm_log0.csv)

HOW TO USE
----------
1. Place Data_log0.csv and Alarm_log0.csv in the same folder as this script.
2. Run:   python app.py
3. Open:  http://127.0.0.1:8050

DEPENDENCIES
------------
  pip install dash dash-bootstrap-components plotly pandas openpyxl requests
"""

import os, sys, re, time
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import dash
from dash import dcc, html, Input, Output, State, dash_table, ALL
import dash_bootstrap_components as dbc

# ===========================================================================
# PATHS
# ===========================================================================
if getattr(sys, "frozen", False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_CSV  = os.path.join(BASE_DIR, "Data_log0.csv")
ALARM_CSV = os.path.join(BASE_DIR, "Alarm_log0.csv")
CACHE_XLS = os.path.join(BASE_DIR, "data_cache.xlsx")

# ===========================================================================
# FILLER CONFIG  (edit as needed)
# ===========================================================================
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1TnWB5q_srpCfwV98cy6yGIbWtMDCPPIE/edit?usp=sharing"
SHEET_TAB  = "Sheet1"
SETPOINT   = 10.04
BOX_WEIGHT = 0.486
SIGMA      = 0.0004

COL_TYPE  = "night/light"
COL_SHIFT = "# shift"
COL_DATE  = "Date"
COL_W_T1  = "W T1";  COL_W_T2  = "W T2";  COL_W_T3  = "W T3"
COL_DC    = "DC"
COL_B_T1  = "B T1";  COL_B_T2  = "B T2";  COL_B_T3  = "B T3"
COL_TT1   = "T T1";  COL_TT2   = "T T2";  COL_TT3   = "T T3"
COL_TC    = "TC"

# ===========================================================================
# SCADA CONFIG
# ===========================================================================
TEMP_SCALE = 10.0   # SCADA stores temperatures × 10

# ── Production monitoring thresholds ────────────────────────────────────────
# Variable name substrings (matched case-insensitively against VarName column)
AIR_TUNNEL_SUBSTR     = "air temperature in tunnel"
EVAP_TEMP_SUBSTR      = "temperature of evap"
# Air-temperature-in-tunnel: any value below 0 °C = production running; ≥ 0 °C = stopped
# -20 °C is still an in-production alarm limit; -30 °C is the expected low target
AIR_PROD_LOW          = -30.0
AIR_PROD_HIGH         = -20.0
AIR_PROD_STOP         =   0.0   # temperature at or above this = out of production
# Evaporator temperature: must stay ≤ -37 °C during production; > -37 °C = alarm
EVAP_ALARM_THRESHOLD  = -37.0

# ===========================================================================
# ── FILLER DATA FUNCTIONS ──────────────────────────────────────────────────
# ===========================================================================

def _sheet_id(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not m:
        raise ValueError(f"Cannot extract sheet ID from URL: {url}")
    return m.group(1)


def download_sheet(url: str, dest: str) -> str:
    sid = _sheet_id(url)
    export = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"
    r = requests.get(export, timeout=30)
    r.raise_for_status()
    with open(dest, "wb") as f:
        f.write(r.content)
    return dest


def load_filler_excel(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(filepath, sheet_name=SHEET_TAB)
    except Exception:
        df = pd.read_excel(filepath, sheet_name=0)
    df.columns = [str(c).strip() for c in df.columns]
    num_cols = [COL_W_T1, COL_W_T2, COL_W_T3, COL_DC,
                COL_B_T1, COL_B_T2, COL_B_T3,
                COL_TT1, COL_TT2, COL_TT3, COL_TC]
    df[COL_SHIFT] = pd.to_numeric(df[COL_SHIFT], errors="coerce")
    df = df[df[COL_SHIFT].notna()].copy()
    df[COL_SHIFT] = df[COL_SHIFT].astype(int)
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.reset_index(drop=True)


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

        try:
            _dt_parsed = pd.to_datetime(r.get(COL_DATE,""), dayfirst=True)
            date_str = _dt_parsed.strftime("%d/%m")
            date_dt  = _dt_parsed.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
        except:
            date_str = str(r.get(COL_DATE,""))[:10]
            date_dt  = None

        shifts.append({
            "shift": int(r[COL_SHIFT]), "type": str(r.get(COL_TYPE,"")).strip().lower(),
            "date": date_str, "date_dt": date_dt,
            "b1":b1,"b2":b2,"b3":b3,"w1":w1,"w2":w2,"w3":w3,"dc":dc,
            "total_boxes":tb,"total_weight":tw,
            "avg_w1":round(aw1,4) if aw1 else None,
            "avg_w2":round(aw2,4) if aw2 else None,
            "avg_w3":round(aw3,4) if aw3 else None,
            "avg_all":round(aa,4) if aa else None,
            "give_per_box1":gpb1,"give_per_box2":gpb2,"give_per_box3":gpb3,
            "over_g1":og1,"over_g2":og2,"over_g3":og3,
            "total_over_g":round(og1+og2+og3,2),
            "total_over":(round((aw1-SETPOINT)/SETPOINT*b1) if aw1 else 0)+
                         (round((aw2-SETPOINT)/SETPOINT*b2) if aw2 else 0)+
                         (round((aw3-SETPOINT)/SETPOINT*b3) if aw3 else 0),
            "tt1":tt1,"tt2":tt2,"tt3":tt3,"tc":tc,
            "inc_t1":inc_t1,"inc_t2":inc_t2,"inc_t3":inc_t3,"inc_tc":inc_tc,
            "gap":gap,"gap_pct":gap_pct,"sum_tracks":sum_tracks,
            "diff_total":round(tc-(tt1+tt2+tt3),0),
        })

    total_pc   = sum(s["total_boxes"] for s in shifts)
    total_wt   = sum(s["total_weight"] for s in shifts)
    total_over = round(sum(s["total_over_g"] for s in shifts), 2)
    total_over_pcs = sum(s["total_over"] for s in shifts)
    t1t=sum(s["b1"] for s in shifts); t2t=sum(s["b2"] for s in shifts); t3t=sum(s["b3"] for s in shifts)
    avg_f = total_wt/total_pc if total_pc else 0
    avg_r = total_pc/len(shifts) if shifts else 0
    uf_n  = sum(1 for s in shifts if s["total_over"]<0)
    dev_acc = round(100-((avg_f-10)/10),3) if total_pc else 0
    final = shifts[-1]
    tc_delta = final["tc"] - base_tc
    t1d=final["tt1"]-base_tt1; t2d=final["tt2"]-base_tt2; t3d=final["tt3"]-base_tt3
    total_gap = tc_delta - total_wt
    gap_pct_total = round(total_gap/tc_delta*100,3) if tc_delta else 0
    try:    dr=f"{shifts[0]['date']} – {shifts[-1]['date']}"
    except: dr=""
    td = tc_delta if tc_delta else 1

    return {
        "shifts": shifts, "total_pc": total_pc, "total_wt": total_wt,
        "total_over": total_over, "total_over_pcs": total_over_pcs,
        "t1_total":t1t,"t2_total":t2t,"t3_total":t3t,
        "avg_filler": round(avg_f,4), "avg_rate": round(avg_r,1),
        "underfill_n": uf_n, "device_accuracy": dev_acc,
        "avg_over_per_shift": round(total_over/len(shifts),2) if shifts else 0,
        "pct_over": round(total_over_pcs/total_pc*100,2) if total_pc else 0,
        "setpoint": SETPOINT, "n_shifts": len(shifts), "date_range": dr,
        "tc_start": base_tc, "tc_end": final["tc"], "tc_delta": tc_delta,
        "t1_delta":t1d,"t2_delta":t2d,"t3_delta":t3d,
        "total_gap": total_gap, "gap_pct_total": gap_pct_total,
        "t1d_pct":round(t1d/td*100,1),"t2d_pct":round(t2d/td*100,1),"t3d_pct":round(t3d/td*100,1),
        "t1_pct":round(t1t/total_pc*100,1) if total_pc else 0,
        "t2_pct":round(t2t/total_pc*100,1) if total_pc else 0,
        "t3_pct":round(t3t/total_pc*100,1) if total_pc else 0,
        "last_updated": datetime.now().strftime("%d %b %Y %H:%M:%S"),
    }


# ===========================================================================
# ── SCADA DATA FUNCTIONS ───────────────────────────────────────────────────
# ===========================================================================

def load_scada_data():
    try:
        df = pd.read_csv(DATA_CSV, sep=";", encoding="latin-1")
        df["TimeString"] = pd.to_datetime(df["TimeString"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["TimeString"])
        df["VarValue"] = pd.to_numeric(df["VarValue"], errors="coerce").astype(float)
        mask_temp = df["VarName"].str.contains("temperature", case=False, na=False)
        df.loc[mask_temp, "VarValue"] = df.loc[mask_temp, "VarValue"] / TEMP_SCALE
        return df
    except Exception as exc:
        print(f"[WARN] Could not load data log: {exc}")
        return pd.DataFrame(columns=["VarName", "TimeString", "VarValue"])


def load_alarms():
    try:
        df = pd.read_csv(ALARM_CSV, sep=";", encoding="latin-1")
        df["TimeString"] = pd.to_datetime(df["TimeString"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["TimeString"])
        df["MsgText"] = df["MsgText"].astype(str).str.strip()
        df = df[df["MsgClass"].isin([1, 34])]
        df = df.sort_values("TimeString")
        return df
    except Exception as exc:
        print(f"[WARN] Could not load alarm log: {exc}")
        return pd.DataFrame(columns=["TimeString", "MsgClass", "StateAfter", "MsgText", "MsgNumber"])


# ===========================================================================
# ── PRODUCTION STATS FROM SCADA ────────────────────────────────────────────
# ===========================================================================

def _compute_production_stats(df, start_dt, end_dt):
    """
    Derive production KPIs from SCADA sensor data within [start_dt, end_dt].

    Production = periods where 'air temperature in tunnel' is below
    AIR_PROD_STOP (0 °C).  A value >= 0 °C means the tunnel is not freezing.

    Returns a dict with:
      production_hours, production_pct, production_periods, cycles (list),
      air_alarm_events, air_alarm_minutes,
      evap_alarm_events, evap_alarm_minutes, total_hours
    """
    empty = dict(production_hours=0, production_pct=0, production_periods=[], cycles=[],
                 air_alarm_events=0, air_alarm_minutes=0,
                 evap_alarm_events=0, evap_alarm_minutes=0,
                 total_hours=round((end_dt - start_dt).total_seconds() / 3600, 1))

    total_minutes = max((end_dt - start_dt).total_seconds() / 60, 1)

    # ── Air temperature in tunnel ───────────────────────────────────────────
    air_raw = df[
        (df["VarName"].str.lower().str.contains(AIR_TUNNEL_SUBSTR, na=False)) &
        (df["TimeString"] >= start_dt) & (df["TimeString"] <= end_dt)
    ].set_index("TimeString")["VarValue"].sort_index()

    if air_raw.empty:
        return empty

    # Resample to 1-minute grid for stable counting
    air_1m = air_raw.resample("1min").mean().interpolate(method="time", limit=60)

    prod_mask      = air_1m < AIR_PROD_STOP          # below 0 °C = production running
    air_alarm_mask = air_1m > AIR_PROD_HIGH           # above -20 °C = insufficient cooling

    production_minutes = int(prod_mask.sum())
    air_alarm_minutes  = int(air_alarm_mask.sum())
    # Count ON-transitions (False→True) for alarm events
    air_alarm_events = int(
        ((~air_alarm_mask.shift(1).fillna(False)) & air_alarm_mask).sum()
    )

    # Build list of production time windows (used for chart shading)
    production_periods = []
    in_prod, seg_start = False, None
    for ts, flag in prod_mask.items():
        if flag and not in_prod:
            seg_start, in_prod = ts, True
        elif not flag and in_prod:
            production_periods.append((seg_start, ts))
            in_prod = False
    if in_prod and seg_start is not None:
        production_periods.append((seg_start, air_1m.index[-1]))

    # ── Evaporator temperature (ammonia) ────────────────────────────────────
    evap_alarm_events = evap_alarm_minutes = 0
    evap_1m_full = None
    evap_raw = df[
        (df["VarName"].str.lower().str.contains(EVAP_TEMP_SUBSTR, na=False)) &
        (df["TimeString"] >= start_dt) & (df["TimeString"] <= end_dt)
    ].set_index("TimeString")["VarValue"].sort_index()

    if not evap_raw.empty:
        evap_1m_full = evap_raw.resample("1min").mean().interpolate(method="time", limit=60)
        evap_aligned = evap_1m_full.reindex(air_1m.index).interpolate(method="time", limit=30)
        # Alarm only when above threshold AND during a production window
        evap_alarm_during = (evap_aligned > EVAP_ALARM_THRESHOLD) & prod_mask
        evap_alarm_minutes = int(evap_alarm_during.sum())
        evap_alarm_events  = int(
            ((~evap_alarm_during.shift(1).fillna(False)) & evap_alarm_during).sum()
        )

    # ── Per-cycle statistics ─────────────────────────────────────────────────
    cycles = []
    for i, (ps, pe) in enumerate(production_periods):
        air_cycle = air_1m[ps:pe]
        dur_min   = len(air_cycle)
        avg_air   = round(float(air_cycle.mean()), 2) if not air_cycle.empty else None

        avg_evap = min_evap = evap_amin = None
        if evap_1m_full is not None:
            _idx = pd.date_range(ps, pe, freq="1min")
            evap_slice = evap_1m_full.reindex(_idx).interpolate(method="time", limit=30)
            _valid = evap_slice.dropna()
            if not _valid.empty:
                avg_evap = round(float(_valid.mean()), 2)
                min_evap = round(float(_valid.min()),  2)
                evap_amin = int((_valid > EVAP_ALARM_THRESHOLD).sum())

        # Night = 20:00–06:00, Day = 06:00–20:00
        # Count minutes in each band across the whole cycle
        _ts_range = pd.date_range(ps, pe, freq="1min")
        night_min = sum(1 for t in _ts_range if t.hour >= 20 or t.hour < 6)
        shift_type = "night" if night_min >= len(_ts_range) / 2 else "day"

        cycles.append(dict(
            cycle=i + 1, start=ps, end=pe, duration_min=dur_min,
            shift_type=shift_type,
            avg_air=avg_air, avg_evap=avg_evap, min_evap=min_evap,
            evap_alarm_min=evap_amin if evap_amin is not None else 0,
        ))

    return dict(
        production_hours   = round(production_minutes / 60, 1),
        production_pct     = round(production_minutes / total_minutes * 100, 1),
        production_periods = production_periods,
        cycles             = cycles,
        air_alarm_events   = air_alarm_events,
        air_alarm_minutes  = air_alarm_minutes,
        evap_alarm_events  = evap_alarm_events,
        evap_alarm_minutes = evap_alarm_minutes,
        total_hours        = round(total_minutes / 60, 1),
    )


def _build_cycles_table(cycles):
    """
    Build a dark HTML table summarising each detected production cycle and
    cross-reference it against filler shift data (if cache is available).
    """
    if not cycles:
        return html.Div(
            "No production cycles detected in the selected period.",
            style={"color": "#7b82a0", "fontStyle": "italic", "padding": "8px 0"},
        )

    # ── Try to load filler data for cross-referencing ───────────────────────
    filler_shifts = []
    try:
        fdf = load_filler_excel(CACHE_XLS)
        filler_shifts = compute_metrics(fdf)["shifts"]
    except Exception:
        pass   # graceful – cross-reference section will say "no data"

    # ── Fix date_dt values that were mis-parsed by Excel locale ─────────────
    # Problem 1: dates like "12/3/2026" entered in EU format get stored by
    #            Excel as December 3 (US month-first interpretation).
    # Problem 2: typos in year like "17/3/20260" produce date_dt=None.
    # Solution:  use the SCADA cycle date range as a reference; correct any
    #            date that falls >60 days outside that range.
    if cycles and filler_shifts:
        _ref_start = min(c["start"] for c in cycles)
        _ref_end   = max(c["end"]   for c in cycles)
        _margin    = timedelta(days=60)
        corrected = []
        for s in filler_shifts:
            s = dict(s)
            d = s.get("date_dt")

            # Fallback: if None, try parsing date_str with stripped typos
            if d is None:
                try:
                    raw = re.sub(r"[^\d/\-]", "", str(s.get("date", "")))
                    parts = re.split(r"[/\-]", raw)
                    if len(parts) == 3:
                        dy, mo = int(parts[0]), int(parts[1])
                        yr = int(str(parts[2])[:4])  # first 4 digits of year
                        d = datetime(yr, mo, dy)
                except Exception:
                    pass

            # Swap month/day if the date lands outside the expected range
            if d is not None:
                if not (_ref_start - _margin <= d <= _ref_end + _margin):
                    try:
                        swapped = d.replace(month=d.day, day=d.month)
                        if _ref_start - _margin <= swapped <= _ref_end + _margin:
                            d = swapped
                    except ValueError:
                        pass

            s["date_dt"] = d
            corrected.append(s)
        filler_shifts = corrected

    def _dur_fmt(minutes):
        h, m = divmod(int(minutes), 60)
        return f"{h}h {m:02d}m"

    def _air_td(v):
        if v is None:
            return html.Td("—", style={"color": "#7b82a0"})
        # Green = normal (-30..-20), Amber = warm but still producing (-20..0), Red = stopped (>=0)
        color = "#34D399" if v <= AIR_PROD_HIGH else ("#FBBF24" if v < AIR_PROD_STOP else "#F87171")
        return html.Td(f"{v:.1f} °C", style={"color": color, "fontWeight": "600"})

    def _evap_td(v):
        if v is None:
            return html.Td("—", style={"color": "#7b82a0"})
        if v <= EVAP_ALARM_THRESHOLD:
            color = "#34D399"
        elif v <= EVAP_ALARM_THRESHOLD + 1.5:
            color = "#FBBF24"
        else:
            color = "#F87171"
        return html.Td(f"{v:.1f} °C", style={"color": color, "fontWeight": "600"})

    _TH = {"background": "#1e2333", "color": "#7b82a0", "fontSize": "10px",
           "padding": "6px 10px", "fontWeight": "600", "textTransform": "uppercase",
           "letterSpacing": "0.05em", "borderBottom": "1px solid rgba(255,255,255,0.08)"}
    _TD = {"fontSize": "12px", "padding": "8px 10px", "color": "#e8eaf0",
           "borderBottom": "1px solid rgba(255,255,255,0.04)"}
    _TD_MONO = {**_TD, "fontFamily": "monospace", "fontSize": "11px"}

    # SCADA header row
    thead = html.Thead(html.Tr([
        html.Th("#",               style=_TH),
        html.Th("Start",           style=_TH),
        html.Th("End",             style=_TH),
        html.Th("Duration",        style=_TH),
        html.Th("Avg Air Temp",    style=_TH),
        html.Th("Avg Evap Temp",   style=_TH),
        html.Th("Evap Alarm",      style=_TH),
        html.Th("Filler Production Summary", style={**_TH, "minWidth": "480px"}),
    ]))

    body_rows = []
    for c in cycles:
        # ── Match filler shifts that OVERLAP this production cycle ───────────
        # Night shift dated DD/MM: 20:00 prev-day → 08:00 that day
        # Day/Light shift dated DD/MM: 08:00 → 20:00 that day
        matched = []
        for s in filler_shifts:
            if s.get("date_dt") is None:
                continue
            d0 = s["date_dt"]   # midnight of the shift's calendar date
            if s["type"].strip().lower() == "night":
                sw_start = d0 - timedelta(hours=4)   # prev-day 20:00
                sw_end   = d0 + timedelta(hours=8)   # that day 08:00
            else:
                sw_start = d0 + timedelta(hours=8)   # that day 08:00
                sw_end   = d0 + timedelta(hours=20)  # that day 20:00
            # Standard overlap: cycle and shift window must intersect
            if c["start"] < sw_end and c["end"] > sw_start:
                matched.append(s)

        # ── Build the filler sub-content (aggregated) ──────────────────────
        if matched:
            total_boxes  = sum(s["total_boxes"]  for s in matched)
            total_weight = sum(s["total_weight"] for s in matched)
            total_over   = sum(s["total_over_g"] for s in matched)
            cycle_hours  = c["duration_min"] / 60 if c["duration_min"] > 0 else 1
            prod_rate    = total_weight / cycle_hours   # Kg per hour

            # labels row: which shifts were combined
            shift_labels = ", ".join(
                f"S#{s['shift']} ({s['date']} "
                f"{'Night' if s['type'].strip().lower() == 'night' else 'Day'})"
                for s in matched
            )
            overfill_color = "#34D399" if total_over >= 0 else "#F87171"

            # sub-table style helpers
            _STH = {"fontSize": "9px", "color": "#7b82a0", "textTransform": "uppercase",
                    "padding": "3px 8px", "fontWeight": "600",
                    "borderBottom": "1px solid rgba(255,255,255,0.06)"}
            _STD = {"fontSize": "12px", "padding": "5px 8px", "color": "#e8eaf0",
                    "fontWeight": "700", "whiteSpace": "nowrap"}

            filler_cell_content = html.Div([
                html.Div(shift_labels, style={
                    "fontSize": "10px", "color": "#9ca3af", "marginBottom": "5px"}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Shift",        style=_STH),
                        html.Th("Total Boxes",  style=_STH),
                        html.Th("Total Weight", style=_STH),
                        html.Th("Rate",         style=_STH),
                        html.Th("Overfill",     style=_STH),
                    ])),
                    html.Tbody([html.Tr([
                        html.Td(
                            ", ".join(f"S#{s['shift']}" for s in matched),
                            style={**_STD, "color": "#818CF8"},
                        ),
                        html.Td(f"{total_boxes:,.0f}",       style=_STD),
                        html.Td(f"{total_weight:,.0f} Kg",   style={**_STD, "color": "#34D399"}),
                        html.Td(f"{prod_rate:,.1f} Kg/h",    style={**_STD, "color": "#818CF8"}),
                        html.Td(f"{total_over:+.2f} Kg",     style={**_STD, "color": overfill_color}),
                    ])]),
                ], style={"borderCollapse": "collapse", "width": "100%",
                           "background": "rgba(255,255,255,0.02)",
                           "border": "1px solid rgba(255,255,255,0.06)",
                           "borderRadius": "6px"}),
            ])
        elif filler_shifts:
            filler_cell_content = html.Span(
                "No matching shift data for this cycle's dates",
                style={"color": "#7b82a0", "fontSize": "11px", "fontStyle": "italic"},
            )
        else:
            filler_cell_content = html.Span(
                "Load Filler Statistics first to see cross-reference",
                style={"color": "#7b82a0", "fontSize": "11px", "fontStyle": "italic"},
            )

        # ── SCADA summary row ────────────────────────────────────────────────
        evap_alarm_badge = _v3_pill(
            f"{c['evap_alarm_min']} min",
            "bad" if c["evap_alarm_min"] > 0 else "ok",
        )
        body_rows.append(html.Tr([
            html.Td(_v3_pill(f"#{c['cycle']}", "info"),
                    style={**_TD, "textAlign": "center"}),
            html.Td(c["start"].strftime("%d/%m/%Y  %H:%M"), style=_TD_MONO),
            html.Td(c["end"].strftime("%d/%m/%Y  %H:%M"),   style=_TD_MONO),
            html.Td(_dur_fmt(c["duration_min"]),
                    style={**_TD, "fontWeight": "700", "color": "#e8eaf0"}),
            _air_td(c["avg_air"]),
            _evap_td(c["avg_evap"]),
            html.Td(evap_alarm_badge, style=_TD),
            html.Td(filler_cell_content, style={**_TD, "verticalAlign": "top"}),
        ], style={"background": "rgba(99,102,241,0.03)" if c["cycle"] % 2 == 0 else "transparent"}))

    return html.Div([
        html.Div("Production Cycle Summary", className="filler-section-label",
                 style={"marginTop": "20px"}),
        html.Div(
            html.Table(
                [thead, html.Tbody(body_rows)],
                style={
                    "width": "100%", "borderCollapse": "collapse",
                    "background": "#181c27",
                    "border": "1px solid rgba(255,255,255,0.07)",
                    "borderRadius": "10px", "overflow": "hidden",
                },
            ),
            style={"overflowX": "auto"},
        ),
    ])


# ===========================================================================
# ── SCADA CHART BUILDERS ───────────────────────────────────────────────────
# ===========================================================================

def build_superimposed_chart(df, start_dt, end_dt, production_periods=None):
    variables = sorted(df["VarName"].dropna().unique().tolist())
    n = len(variables)
    if n == 0:
        return go.Figure()

    colours = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
               "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"]

    fig = make_subplots(specs=[[{"secondary_y": False}]])

    n_left_extra  = sum(1 for i in range(1, n) if i % 2 == 1)
    n_right_extra = sum(1 for i in range(1, n) if i % 2 == 0)
    PAD = 0.08
    domain_left  = (n_left_extra + 1) * PAD
    domain_right = 1.0 - n_right_extra * PAD

    traces = []
    for i, varname in enumerate(variables):
        var_df = df[df["VarName"] == varname].sort_values("TimeString")
        before = var_df[var_df["TimeString"] < start_dt]
        inside = var_df[
            (var_df["TimeString"] >= start_dt) &
            (var_df["TimeString"] <= end_dt)
        ]
        rows = []
        if not before.empty:
            last = before.iloc[-1][["TimeString", "VarValue"]].copy()
            last["TimeString"] = start_dt
            rows.append(pd.DataFrame([last]))
        rows.append(inside[["TimeString", "VarValue"]])
        if not inside.empty:
            tail = inside.iloc[-1][["TimeString", "VarValue"]].copy()
            tail["TimeString"] = end_dt
            rows.append(pd.DataFrame([tail]))
        if not rows or inside.empty:
            continue
        segment = (pd.concat(rows).drop_duplicates("TimeString").sort_values("TimeString"))
        yaxis_key = "y" if i == 0 else f"y{i + 1}"
        colour = colours[i % len(colours)]
        traces.append((
            i, colour, varname, segment["VarValue"],
            go.Scatter(
                x=segment["TimeString"], y=segment["VarValue"],
                mode="lines", line=dict(shape="hv", color=colour, width=2),
                name=varname, yaxis=yaxis_key,
                hovertemplate=f"<b>{varname}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
            ),
        ))

    if not traces:
        return go.Figure()

    fig.add_traces([t[4] for t in traces])

    layout_updates = dict(
        xaxis=dict(title="Time", domain=[domain_left, domain_right],
                   showgrid=True, gridcolor="#e0e0e0"),
        plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor="rgba(255,255,255,0.85)", bordercolor="#cccccc", borderwidth=1),
        hovermode="x unified",
        title="All Parameters – Superimposed Chart",
        margin=dict(t=110, b=80, l=60, r=60),
    )

    left_extra_count = right_extra_count = 0
    for i, colour, varname, values, _ in traces:
        y_min = values.min(); y_max = values.max()
        if pd.isna(y_min) or pd.isna(y_max):
            continue
        y_margin = (y_max - y_min) * 0.1 if y_max != y_min else 1.0
        axis_cfg = dict(
            tickfont=dict(color=colour, size=9),
            showgrid=(i == 0), gridcolor="#e8e8e8",
            range=[y_min - y_margin, y_max + y_margin],
            zeroline=False, showline=True, linecolor=colour, linewidth=2,
            ticks="outside", tickcolor=colour,
        )
        if i == 0:
            axis_cfg["anchor"] = "x"
            layout_updates["yaxis"] = axis_cfg
        else:
            if i % 2 == 1:
                position = domain_left - (left_extra_count + 1) * PAD
                left_extra_count += 1
                side = "left"
            else:
                position = domain_right + right_extra_count * PAD
                right_extra_count += 1
                side = "right"
            axis_cfg.update(overlaying="y", side=side, anchor="free",
                            position=max(0.0, min(1.0, position)))
            layout_updates[f"yaxis{i + 1}"] = axis_cfg

    # ── Production zone shading (green bands) ──────────────────────────────
    shapes = []
    if production_periods:
        for ps, pe in production_periods:
            shapes.append(dict(
                type="rect", xref="x", yref="paper",
                x0=str(ps), x1=str(pe), y0=0, y1=1,
                fillcolor="rgba(52,211,153,0.07)",
                line=dict(width=0), layer="below",
            ))
    layout_updates["shapes"] = shapes

    # ── Threshold reference lines on the correct y-axes ────────────────────
    threshold_traces = []
    for i, colour, varname, values, _ in traces:
        yaxis_key = "y" if i == 0 else f"y{i + 1}"
        vn_lower = varname.lower()
        if AIR_TUNNEL_SUBSTR in vn_lower:
            threshold_traces.append(go.Scatter(
                x=[start_dt, end_dt], y=[AIR_PROD_HIGH, AIR_PROD_HIGH],
                mode="lines", name="Air temp alarm limit (−20 °C)",
                line=dict(color="#ef4444", width=1.5, dash="dot"),
                yaxis=yaxis_key, hoverinfo="skip", showlegend=True,
            ))
            threshold_traces.append(go.Scatter(
                x=[start_dt, end_dt], y=[AIR_PROD_LOW, AIR_PROD_LOW],
                mode="lines", name="Air temp prod. low (−30 °C)",
                line=dict(color="#f97316", width=1, dash="dot"),
                yaxis=yaxis_key, hoverinfo="skip", showlegend=True,
            ))
        elif EVAP_TEMP_SUBSTR in vn_lower:
            threshold_traces.append(go.Scatter(
                x=[start_dt, end_dt], y=[EVAP_ALARM_THRESHOLD, EVAP_ALARM_THRESHOLD],
                mode="lines", name="Evap alarm limit (−37 °C)",
                line=dict(color="#fbbf24", width=1.5, dash="dot"),
                yaxis=yaxis_key, hoverinfo="skip", showlegend=True,
            ))
    if threshold_traces:
        fig.add_traces(threshold_traces)

    fig.update_layout(**layout_updates)
    return fig


def build_alarm_timeline(alarm_df, start_dt, end_dt):
    df = alarm_df[
        (alarm_df["TimeString"] >= start_dt) & (alarm_df["TimeString"] <= end_dt)
    ].copy()
    if df.empty:
        return go.Figure(layout=dict(title="No process alarms in this period"))

    process_alarms = df[df["MsgClass"] == 1].copy()
    cpu_alarms     = df[df["MsgClass"] == 34].copy()

    gantt_rows = []
    if not process_alarms.empty:
        for msg in sorted(process_alarms["MsgText"].unique()):
            msg_rows = process_alarms[process_alarms["MsgText"] == msg].sort_values("TimeString")
            came = msg_rows[msg_rows["StateAfter"] == 1]
            gone = msg_rows[msg_rows["StateAfter"] == 0]
            for _, came_row in came.iterrows():
                t_start = came_row["TimeString"]
                later_gone = gone[gone["TimeString"] > t_start]
                t_end = later_gone.iloc[0]["TimeString"] if not later_gone.empty else end_dt
                if t_end == t_start:
                    t_end = t_start + pd.Timedelta(minutes=1)
                gantt_rows.append({"Alarm": msg, "Start": t_start, "End": t_end})

    if not gantt_rows:
        return go.Figure(layout=dict(title="No process alarms in this period"))

    gantt_df = pd.DataFrame(gantt_rows)
    n_alarms = gantt_df["Alarm"].nunique()
    fig = px.timeline(gantt_df, x_start="Start", x_end="End", y="Alarm",
                      color="Alarm", title="Alarm Timeline (process alarms)",
                      hover_data={"Start": True, "End": True, "Alarm": False})
    fig.update_yaxes(autorange="reversed")

    if not cpu_alarms.empty:
        cpu_came = cpu_alarms[cpu_alarms["StateAfter"] == 1]
        fig.add_trace(go.Scatter(
            x=cpu_came["TimeString"], y=["PLC STOP"] * len(cpu_came),
            mode="markers",
            marker=dict(symbol="x", size=14, color="#000000", line=dict(width=2)),
            name="PLC STOP event",
            hovertemplate="PLC STOP<br>%{x}<extra></extra>",
        ))

    fig.update_layout(
        xaxis=dict(title="Time"), yaxis=dict(title=""),
        plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
        legend=dict(orientation="h", y=1.05),
        height=max(350, 55 * n_alarms + 120),
        margin=dict(l=280, t=80, b=60, r=20),
    )
    return fig


def build_alarm_frequency_chart(alarm_df, start_dt, end_dt):
    df = alarm_df[
        (alarm_df["TimeString"] >= start_dt) & (alarm_df["TimeString"] <= end_dt) &
        (alarm_df["MsgClass"] == 1) & (alarm_df["StateAfter"] == 1)
    ].copy()
    if df.empty:
        return go.Figure(layout=dict(title="No alarms in this period"))
    counts = df.groupby("MsgText").size().sort_values(ascending=True)
    fig = go.Figure(go.Bar(x=counts.values, y=counts.index, orientation="h",
                           marker_color="#d62728",
                           hovertemplate="<b>%{y}</b><br>Occurrences: %{x}<extra></extra>"))
    fig.update_layout(title="Alarm Frequency", xaxis=dict(title="Count"), yaxis=dict(title=""),
                      plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
                      height=max(300, 35 * len(counts) + 100), margin=dict(l=300))
    return fig


def build_alarm_duration_chart(alarm_df, start_dt, end_dt):
    df = alarm_df[
        (alarm_df["TimeString"] >= start_dt) & (alarm_df["TimeString"] <= end_dt) &
        (alarm_df["MsgClass"] == 1)
    ].copy()
    if df.empty:
        return go.Figure(layout=dict(title="No alarms in this period"))
    durations = {}
    for msg in df["MsgText"].unique():
        msg_rows = df[df["MsgText"] == msg].sort_values("TimeString")
        came = msg_rows[msg_rows["StateAfter"] == 1]
        gone = msg_rows[msg_rows["StateAfter"] == 0]
        total_sec = 0.0
        for _, row in came.iterrows():
            t0 = row["TimeString"]
            later = gone[gone["TimeString"] > t0]
            t1 = later.iloc[0]["TimeString"] if not later.empty else end_dt
            total_sec += (t1 - t0).total_seconds()
        durations[msg] = total_sec / 60.0
    dur_series = pd.Series(durations).sort_values(ascending=True)
    fig = go.Figure(go.Bar(x=dur_series.values, y=dur_series.index, orientation="h",
                           marker_color="#ff7f0e",
                           hovertemplate="<b>%{y}</b><br>Total: %{x:.1f} min<extra></extra>"))
    fig.update_layout(title="Total Alarm Active Duration", xaxis=dict(title="Minutes"),
                      yaxis=dict(title=""), plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
                      height=max(300, 35 * len(dur_series) + 100), margin=dict(l=300))
    return fig


# ===========================================================================
# ── FILLER CHART BUILDERS ──────────────────────────────────────────────────
# ===========================================================================

NIGHT_CLR  = "#4F46E5"
LIGHT_CLR  = "#34D399"
T1_CLR     = "#6366F1"
T2_CLR     = "#34D399"
T3_CLR     = "#F59E0B"
TC_CLR     = "#e879f9"
OVER_CLR   = "#6366F1"
UNDER_CLR  = "#EF4444"
AMBER_CLR  = "#F59E0B"

CHART_BG   = {"plot_bgcolor": "#181c27", "paper_bgcolor": "#0f1117"}

# shared dark axis base – used by _filler_dark_layout
_DA = dict(
    gridcolor="rgba(255,255,255,0.06)",
    tickfont=dict(color="#7b82a0", size=10),
    linecolor="rgba(255,255,255,0.1)",
    zerolinecolor="rgba(255,255,255,0.06)",
)


def _filler_dark_layout(title, ytitle=None, xangle=-45, yrange=None, **extra):
    """Return a dict suitable for fig.update_layout() with the v3 dark aesthetic."""
    yd = dict(**_DA)
    if ytitle:
        yd["title"] = dict(text=ytitle, font=dict(color="#7b82a0"))
    if yrange:
        yd["range"] = yrange
    d = dict(
        title=dict(text=title, font=dict(color="#e8eaf0", size=13)),
        xaxis=dict(tickangle=xangle, **_DA),
        yaxis=yd,
        font=dict(color="#7b82a0"),
        legend=dict(
            font=dict(color="#7b82a0", size=10),
            bgcolor="rgba(24,28,39,0.9)",
            bordercolor="rgba(255,255,255,0.07)",
            borderwidth=1,
        ),
        **CHART_BG,
    )
    d.update(extra)
    return d


def _shift_labels(shifts):
    return [f"S{s['shift']} · {s['date']}" for s in shifts]


def build_filler_boxes_chart(shifts):
    labels = _shift_labels(shifts)
    colors = [NIGHT_CLR if s["type"] == "night" else LIGHT_CLR for s in shifts]
    fig = go.Figure(go.Bar(
        x=labels, y=[s["total_boxes"] for s in shifts],
        marker_color=colors,
        hovertemplate="<b>%{x}</b><br>Boxes: %{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(**_filler_dark_layout("Boxes per Shift", "Boxes",
                                             margin=dict(t=50, b=100, l=60, r=20)))
    return fig


def build_filler_weight_chart(shifts, setpoint):
    labels = _shift_labels(shifts)
    all_w = [v for s in shifts for v in [s["avg_w1"], s["avg_w2"], s["avg_w3"], s["avg_all"]] if v]
    y_min = round(min(all_w + [setpoint]) - 0.08, 2) if all_w else 9.9
    y_max = round(max(all_w + [setpoint]) + 0.08, 2) if all_w else 10.2

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=labels, y=[s["avg_all"] for s in shifts],
                             mode="lines+markers", name="Overall avg",
                             line=dict(color=AMBER_CLR, width=2),
                             marker=dict(color=AMBER_CLR, size=6),
                             hovertemplate="<b>%{x}</b><br>Avg: %{y:.4f} g<extra></extra>"))
    fig.add_trace(go.Scatter(x=labels, y=[setpoint] * len(shifts),
                             mode="lines", name="Setpoint",
                             line=dict(color="rgba(99,102,241,0.5)", dash="dash", width=1),
                             hoverinfo="skip"))
    fig.update_layout(**_filler_dark_layout("Average Weight per Shift", "Weight (Kg)",
                                             yrange=[y_min, y_max],
                                             margin=dict(t=50, b=130, l=70, r=60)))
    return fig


def build_filler_overfill_chart(shifts):
    labels = _shift_labels(shifts)
    colors = [UNDER_CLR if s["total_over_g"] < 0 else OVER_CLR for s in shifts]
    fig = go.Figure(go.Bar(
        x=labels, y=[s["total_over_g"] for s in shifts],
        marker_color=colors,
        hovertemplate="<b>%{x}</b><br>Overfill: %{y:+.2f} Kg<extra></extra>",
    ))
    fig.update_layout(**_filler_dark_layout("Total Overfill per Shift (Kg)", "Kg",
                                             margin=dict(t=50, b=100, l=60, r=20)))
    return fig


def build_filler_track_split_chart(m):
    labels = ["Track 1", "Track 2", "Track 3"]
    values = [m["t1_total"], m["t2_total"], m["t3_total"]]
    fig = go.Figure(go.Pie(labels=labels, values=values,
                           marker_colors=[T1_CLR, T2_CLR, T3_CLR],
                           hole=0.55,
                           hovertemplate="<b>%{label}</b><br>%{value:,.0f} boxes (%{percent})<extra></extra>"))
    fig.update_layout(
        title=dict(text="Track Distribution", font=dict(color="#e8eaf0", size=13)),
        font=dict(color="#7b82a0"),
        legend=dict(font=dict(color="#7b82a0", size=10), bgcolor="rgba(24,28,39,0.9)",
                    bordercolor="rgba(255,255,255,0.07)", borderwidth=1),
        **CHART_BG, margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig


def build_filler_per_track_weight_chart(shifts, setpoint):
    labels = _shift_labels(shifts)
    all_w = [v for s in shifts for v in [s["avg_w1"], s["avg_w2"], s["avg_w3"]] if v]
    y_min = round(min(all_w + [setpoint]) - 0.08, 2) if all_w else 9.9
    y_max = round(max(all_w + [setpoint]) + 0.08, 2) if all_w else 10.2

    fig = go.Figure()
    for col, clr, name in [(("avg_w1"), T1_CLR, "T1"),
                            (("avg_w2"), T2_CLR, "T2"),
                            (("avg_w3"), T3_CLR, "T3")]:
        fig.add_trace(go.Scatter(
            x=labels, y=[s[col] for s in shifts],
            mode="lines+markers", name=name,
            line=dict(color=clr, width=2), marker=dict(size=5),
            connectgaps=True,
            hovertemplate=f"<b>{name}</b> %{{x}}<br>%{{y:.4f}} g<extra></extra>"))
    fig.add_trace(go.Scatter(x=labels, y=[setpoint]*len(shifts),
                             mode="lines", name="Setpoint",
                             line=dict(color="rgba(107,114,128,0.45)", dash="dash"),
                             hoverinfo="skip"))
    fig.update_layout(**_filler_dark_layout("Per-Track Average Weight", "Weight (Kg)",
                                             yrange=[y_min, y_max],
                                             margin=dict(t=50, b=130, l=70, r=60)))
    return fig


def build_counter_total_chart(shifts):
    labels = _shift_labels(shifts)
    fig = go.Figure()
    for col, clr, name in [("tt1",T1_CLR,"T T1"),("tt2",T2_CLR,"T T2"),
                             ("tt3",T3_CLR,"T T3"),("tc",TC_CLR,"TC")]:
        fig.add_trace(go.Scatter(
            x=labels, y=[s[col] for s in shifts],
            mode="lines+markers", name=name,
            line=dict(color=clr, width=2 if col!="tc" else 2.5),
            marker=dict(size=5),
            hovertemplate=f"<b>{name}</b> %{{x}}<br>%{{y:,.0f}}<extra></extra>"))
    fig.update_layout(**_filler_dark_layout("Counter Totals per Shift", "Count",
                                             margin=dict(t=50, b=100, l=70, r=20)))
    return fig


def build_counter_tc_increment_chart(shifts):
    labels = _shift_labels(shifts)
    fig = go.Figure(go.Bar(
        x=labels, y=[s["inc_tc"] for s in shifts],
        marker_color="rgba(232,121,249,0.75)",
        hovertemplate="<b>%{x}</b><br>TC increment: %{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(**_filler_dark_layout("TC Increment per Shift", "Count",
                                             margin=dict(t=50, b=100, l=70, r=20)))
    return fig


def build_counter_track_increments_chart(shifts):
    labels = _shift_labels(shifts)
    fig = go.Figure()
    for col, clr, name in [("inc_t1",T1_CLR,"T1"),("inc_t2",T2_CLR,"T2"),("inc_t3",T3_CLR,"T3")]:
        fig.add_trace(go.Bar(
            x=labels, y=[s[col] for s in shifts],
            name=name, marker_color=clr.replace(")", ",0.75)").replace("rgb","rgba"),
            hovertemplate=f"<b>{name}</b> %{{x}}<br>%{{y:,.0f}}<extra></extra>"))
    fig.update_layout(**_filler_dark_layout("Track Increments per Shift (Stacked)", "Count",
                                             barmode="stack",
                                             margin=dict(t=50, b=100, l=70, r=20)))
    return fig


def build_counter_gap_chart(shifts):
    labels = _shift_labels(shifts)
    colors = [UNDER_CLR if s["gap"] < 0 else AMBER_CLR for s in shifts]
    fig = go.Figure(go.Bar(
        x=labels, y=[s["gap"] for s in shifts], marker_color=colors,
        hovertemplate="<b>%{x}</b><br>Gap: %{y:+,.0f}<extra></extra>",
    ))
    fig.update_layout(**_filler_dark_layout("Gap per Shift (TC increment − DC)", "Count",
                                             margin=dict(t=50, b=100, l=70, r=20)))
    return fig


def build_counter_delta_split_chart(m):
    labels = ["T1 delta", "T2 delta", "T3 delta"]
    values = [abs(m["t1_delta"]), abs(m["t2_delta"]), abs(m["t3_delta"])]
    fig = go.Figure(go.Pie(labels=labels, values=values,
                           marker_colors=[T1_CLR, T2_CLR, T3_CLR], hole=0.55,
                           hovertemplate="<b>%{label}</b><br>%{value:,.0f} (%{percent})<extra></extra>"))
    fig.update_layout(
        title=dict(text="Counter Delta Distribution", font=dict(color="#e8eaf0", size=13)),
        font=dict(color="#7b82a0"),
        legend=dict(font=dict(color="#7b82a0", size=10), bgcolor="rgba(24,28,39,0.9)",
                    bordercolor="rgba(255,255,255,0.07)", borderwidth=1),
        **CHART_BG, margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig


# ===========================================================================
# ── HELPERS ────────────────────────────────────────────────────────────────
# ===========================================================================

def _kpi_card(title, value, colour="primary"):
    return dbc.Col(
        dbc.Card(dbc.CardBody([
            html.H6(title, className="card-subtitle text-muted mb-1", style={"fontSize":"11px"}),
            html.H5(value, className=f"text-{colour} mb-0", style={"fontSize":"15px"}),
        ]), className="shadow-sm h-100"),
        md=3, className="mb-2",
    )


def _parse_dt(date_str, hour, minute):
    try:
        date_part = str(date_str).split("T")[0][:10]
        dt = datetime.strptime(date_part, "%Y-%m-%d")
        return dt.replace(hour=int(hour or 0), minute=int(minute or 0), second=0)
    except Exception as exc:
        print(f"[WARN] _parse_dt failed: {exc}")
        return datetime.now()


# ── Dark KPI card (filler tab) ──────────────────────────────────────────────
_DARK_STATUS = {
    "good":      ("#34D399", "#34D399"),
    "bad":       ("#F87171", "#F87171"),
    "warn":      ("#FBBF24", "#FBBF24"),
    "primary":   ("#818CF8", "#6366F1"),
    "secondary": ("#9ca3af", "#6b7280"),
    "info":      ("#38bdf8", "#0ea5e9"),
}


def _kpi_card_dark(title, value, sub="", status="primary"):
    val_color, border_color = _DARK_STATUS.get(status, ("#fff", "#6366F1"))
    return html.Div([
        html.Div(title, style={"fontSize": "11px", "color": "#7b82a0", "marginBottom": "6px"}),
        html.Div(value, style={
            "fontSize": "22px", "fontWeight": "700", "color": val_color,
            "letterSpacing": "-0.02em", "lineHeight": "1.2",
        }),
        html.Div(sub, style={"fontSize": "11px", "color": "#7b82a0", "marginTop": "4px"}),
    ], style={
        "background": "#181c27",
        "border": "1px solid rgba(255,255,255,0.07)",
        "borderRadius": "10px",
        "padding": "16px 18px",
        "borderTop": f"3px solid {border_color}",
        "minHeight": "88px",
    })


# ── v3 table pill helpers ────────────────────────────────────────────────────
def _v3_pill(text, ptype="info"):
    _styles = {
        "info":  {"background": "rgba(99,102,241,0.15)",  "color": "#818CF8"},
        "ok":    {"background": "rgba(52,211,153,0.12)",  "color": "#34D399"},
        "warn":  {"background": "rgba(251,191,36,0.12)",  "color": "#FBBF24"},
        "bad":   {"background": "rgba(248,113,113,0.12)", "color": "#F87171"},
    }
    s = _styles.get(ptype, _styles["info"])
    return html.Span(str(text), style={
        "display": "inline-block", "fontSize": "10px", "padding": "2px 7px",
        "borderRadius": "4px", "fontWeight": "500", **s,
    })


def _weight_pill(w):
    if w is None:
        return "—"
    if SETPOINT - 0.006 <= w <= SETPOINT + 0.006:
        return _v3_pill(f"{w:.4f}", "ok")
    if w >= 9.95:
        return _v3_pill(f"{w:.4f}", "warn")
    return _v3_pill(f"{w:.4f}", "bad")


def _status_pill(w):
    if w is None:
        return "—"
    return _v3_pill("OK", "ok") if 9.90 <= w <= 10.10 else _v3_pill("Check", "bad")


def _gap_pill(g):
    a = abs(g)
    return _v3_pill(f"{g:+,.0f}", "ok" if a < 10 else ("warn" if a < 100 else "bad"))


def _gap_pct_pill(p):
    a = abs(p)
    return _v3_pill(f"{p:+.3f}%", "ok" if a < 0.05 else ("warn" if a < 0.2 else "bad"))


def _build_shift_panel(s):
    """Build a v3-style track-detail HTML table for one shift."""
    _sign = lambda v: f"+{v}" if v >= 0 else str(v)

    def _row(track, b, w, avg_w, gpb, og):
        return html.Tr([
            html.Td(html.Strong(track)),
            html.Td(f"{b:,.0f}"),
            html.Td(f"{w:,.0f}"),
            html.Td(_weight_pill(avg_w)),
            html.Td(
                f"{gpb:+.2f}" if gpb is not None else "—",
                style={"color": "#34D399" if gpb and gpb >= 0 else "#F87171"},
            ),
            html.Td(
                f"{og:+.2f}",
                style={"color": "#34D399" if og >= 0 else "#F87171"},
            ),
            html.Td(_status_pill(avg_w)),
        ])

    rows = [
        _row("Track 1", s["b1"], s["w1"], s["avg_w1"], s["give_per_box1"], s["over_g1"]),
        _row("Track 2", s["b2"], s["w2"], s["avg_w2"], s["give_per_box2"], s["over_g2"]),
        _row("Track 3", s["b3"], s["w3"], s["avg_w3"], s["give_per_box3"], s["over_g3"]),
    ]
    tfoot = html.Tfoot([html.Tr([
        html.Td(html.Strong("Total")),
        html.Td(f"{s['total_boxes']:,.0f}"),
        html.Td(f"{s['total_weight']:,.0f}"),
        html.Td(_weight_pill(s["avg_all"])),
        html.Td("—"),
        html.Td(f"{s['total_over_g']:+.2f}",
                style={"color": "#34D399" if s["total_over_g"] >= 0 else "#F87171"}),
        html.Td(f"DC: {s['dc']:,.0f}"),
    ], style={"background": "rgba(99,102,241,0.05)", "fontWeight": "500", "color": "#fff"})])

    return html.Table([
        html.Thead(html.Tr([
            html.Th(h) for h in
            ["Track", "Boxes", "Weight(Kg)", "Avg Weight(Kg)", "Give/box (g)", "Overfill(Kg)", "Status"]
        ])),
        html.Tbody(rows),
        tfoot,
    ], className="filler-detail-table")


# ===========================================================================
# ── PRELOAD FOR DEFAULT DATES ──────────────────────────────────────────────
# ===========================================================================

_scada_init = load_scada_data()
_alarm_init = load_alarms()

def _date_range(df):
    if df.empty or "TimeString" not in df.columns:
        return datetime(2025, 12, 1), datetime.now()
    return df["TimeString"].min(), df["TimeString"].max()

scada_min, scada_max = _date_range(_scada_init)
alarm_min, alarm_max = _date_range(_alarm_init)
overall_min = min(scada_min, alarm_min)
overall_max = max(scada_max, alarm_max)


# ===========================================================================
# ── LAYOUT HELPERS ─────────────────────────────────────────────────────────
# ===========================================================================

def _time_picker(id_prefix, default_start, default_end):
    """Shared date + time picker row."""
    return dbc.Card(dbc.CardBody([
        dbc.Row([
            dbc.Col(dbc.Label("Period:", className="fw-bold"), width="auto"),
            dbc.Col(
                dbc.InputGroup([
                    dbc.Input(id=f"{id_prefix}-start-date", type="date",
                              value=str(default_start.date()), size="sm"),
                    dbc.InputGroupText("→"),
                    dbc.Input(id=f"{id_prefix}-end-date", type="date",
                              value=str(default_end.date()), size="sm"),
                ]), md=4,
            ),
            dbc.Col(
                dbc.InputGroup([
                    dbc.InputGroupText("Start", style={"fontSize":"12px","padding":"2px 6px"}),
                    dbc.Input(id=f"{id_prefix}-start-hour", type="number", min=0, max=23,
                              value=default_start.hour, placeholder="HH", size="sm",
                              style={"width":"58px","minWidth":"58px"}),
                    dbc.InputGroupText("h", style={"fontSize":"11px","padding":"2px 4px"}),
                    dbc.Input(id=f"{id_prefix}-start-min", type="number", min=0, max=59,
                              value=default_start.minute, placeholder="MM", size="sm",
                              style={"width":"58px","minWidth":"58px"}),
                    dbc.InputGroupText("m  →  End", style={"fontSize":"12px","padding":"2px 8px"}),
                    dbc.Input(id=f"{id_prefix}-end-hour", type="number", min=0, max=23,
                              value=default_end.hour, placeholder="HH", size="sm",
                              style={"width":"58px","minWidth":"58px"}),
                    dbc.InputGroupText("h", style={"fontSize":"11px","padding":"2px 4px"}),
                    dbc.Input(id=f"{id_prefix}-end-min", type="number", min=0, max=59,
                              value=default_end.minute, placeholder="MM", size="sm",
                              style={"width":"58px","minWidth":"58px"}),
                    dbc.InputGroupText("m", style={"fontSize":"11px","padding":"2px 4px"}),
                ], size="sm"), md=7,
            ),
        ], align="center"),
    ]), className="mb-3 mt-2")


# ===========================================================================
# ── DASH APP ───────────────────────────────────────────────────────────────
# ===========================================================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="IQF Production Dashboard",
)
server = app.server   # expose Flask server for gunicorn / production deployment

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

app.layout = dbc.Container(fluid=True, children=[

    # ── Header ──────────────────────────────────────────────────────────────
    dbc.Row(dbc.Col(html.Div([
        html.H2("IQF Production Dashboard", className="mb-0",
                style={"color": "#e8eaf0", "fontWeight": "700", "letterSpacing": "-0.02em"}),
        html.Small("Filler Statistics  ·  SCADA Analysis",
                   style={"color": "#7b82a0", "fontSize": "12px"}),
    ], style={
        "background": "#0f1117",
        "borderBottom": "1px solid rgba(255,255,255,0.07)",
        "padding": "16px 20px",
    }))),

    # ── Main Tabs ────────────────────────────────────────────────────────────
    dbc.Tabs(id="main-tabs", active_tab="tab-scada", children=[

        # ╔══════════════════════════════════════════════════════════════════╗
        # ║  TAB 1 – SCADA Analysis                                          ║
        # ╚══════════════════════════════════════════════════════════════════╝
        dbc.Tab(label="📡 SCADA Analysis", tab_id="tab-scada", children=[

            # Shared time picker (used by both SCADA sub-tabs)
            _time_picker("shared", default_start=scada_min, default_end=scada_max),

            dbc.Tabs(id="scada-subtabs", active_tab="scada-data", children=[

                # ── Data Logs ─────────────────────────────────────────────
                dbc.Tab(label="📈 Data Logs", tab_id="scada-data", children=[
                    dbc.Row([
                        dbc.Col(dbc.Button("Plot All Parameters", id="btn-data",
                                           color="primary", className="mt-2 mb-2"), width="auto"),
                        dbc.Col(html.Div(id="data-status",
                                        className="mt-3 text-muted small"), width="auto"),
                    ], className="mt-2"),
                    # Production KPI cards (filled by callback)
                    html.Div(id="scada-prod-kpis", className="filler-kpi-grid mb-3"),
                    dbc.Spinner(
                        dcc.Graph(id="chart-superimposed",
                                  config={"scrollZoom": True, "displayModeBar": True,
                                          "toImageButtonOptions": {"format":"png","scale":2}},
                                  style={"height": "82vh"}),
                        color="primary",
                    ),
                    # Production cycle summary table (cross-referenced with filler)
                    html.Div(id="scada-prod-cycles-table"),
                ]),

                # ── Alarm Analysis ────────────────────────────────────────
                dbc.Tab(label="🚨 Alarm Analysis", tab_id="scada-alarm", children=[
                    dbc.Row([
                        dbc.Col(dbc.Button("Analyse Alarms", id="btn-alarm",
                                           color="danger", className="mt-2 mb-2"), width="auto"),
                        dbc.Col(html.Div(id="alarm-status",
                                        className="mt-3 text-muted small"), width="auto"),
                    ], className="mt-2"),
                    dbc.Row(id="alarm-kpis", className="mb-3"),
                    dbc.Spinner(dcc.Graph(id="chart-alarm-timeline",
                                         config={"scrollZoom": True},
                                         style={"minHeight": "300px"}), color="danger"),
                    dbc.Row([
                        dbc.Col(dbc.Spinner(dcc.Graph(id="chart-alarm-freq",
                                                       style={"minHeight":"300px"}),
                                            color="warning"), md=6),
                        dbc.Col(dbc.Spinner(dcc.Graph(id="chart-alarm-dur",
                                                       style={"minHeight":"300px"}),
                                            color="warning"), md=6),
                    ]),
                    html.Hr(),
                    html.H5("Alarm Log Table"),
                    html.Div(id="alarm-table-container"),
                ]),
            ]),
        ]),

        # ╔══════════════════════════════════════════════════════════════════╗
        # ║  TAB 2 – Filler Statistics                                       ║
        # ╚══════════════════════════════════════════════════════════════════╝
        dbc.Tab(label="🏭 Filler Statistics", tab_id="tab-filler", children=[

            html.Div(id="filler-dark-wrap", children=[

                dbc.Row([
                    dbc.Col(
                        dbc.Button("🔄 Refresh from Google Sheets", id="btn-filler-refresh",
                                   color="success", className="mt-2 mb-1"),
                        width="auto",
                    ),
                    dbc.Col(
                        html.Div(id="filler-status", className="mt-3 text-muted small"),
                        width="auto",
                    ),
                ], className="mt-2 mb-1"),

                # Hidden store for metrics JSON
                dcc.Store(id="filler-store"),

                # KPI cards ──────────────────────────────────────────────────
                html.Div("Overall Summary", className="filler-section-label"),
                html.Div(id="filler-kpis-weight", className="filler-kpi-grid"),
                html.Div("Counter Summary", className="filler-section-label"),
                html.Div(id="filler-kpis-counter", className="filler-kpi-grid",
                         style={"marginBottom": "20px"}),

                # Sub-tabs ───────────────────────────────────────────────────
                dbc.Tabs(id="filler-subtabs", active_tab="filler-weight", children=[

                    dbc.Tab(label="⚖ Weight Analysis", tab_id="filler-weight", children=[
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-boxes"), color="success"), md=6),
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-avg-weight"), color="success"), md=6),
                        ], className="mt-2"),
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-overfill"), color="warning"), md=6),
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-track-split"), color="info"), md=6),
                        ]),
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-per-track-weight"), color="success"), md=12),
                        ], className="mt-2"),
                        html.Hr(),
                        dbc.Card([
                            dbc.CardHeader(html.H5("Shift Detail Table", className="mb-0")),
                            dbc.CardBody(html.Div(id="filler-shift-table"),
                                         style={"overflowX": "auto", "padding": "12px"}),
                        ], className="mt-2 mb-3 shadow-sm"),
                    ]),

                    dbc.Tab(label="🔢 Counter Analysis", tab_id="filler-counter", children=[
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-counter-total"), color="primary"), md=6),
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-tc-increment"), color="secondary"), md=6),
                        ], className="mt-2"),
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-track-increments"), color="primary"), md=6),
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-gap"), color="warning"), md=6),
                        ]),
                        dbc.Row([
                            dbc.Col(dbc.Spinner(dcc.Graph(id="fig-delta-split"), color="info"), md=12),
                        ], className="mt-2"),
                        html.Hr(),
                        dbc.Card([
                            dbc.CardHeader(html.H5("Counter Detail Table", className="mb-0")),
                            dbc.CardBody(html.Div(id="filler-counter-table"),
                                         style={"overflowX": "auto", "padding": "12px"}),
                        ], className="mt-2 mb-3 shadow-sm"),
                    ]),
                ]),

            ]),   # /filler-dark-wrap
        ]),
    ]),
])


# ===========================================================================
# ── CALLBACKS ──────────────────────────────────────────────────────────────
# ===========================================================================

# ---------------------------------------------------------------------------
# Filler – refresh data from Google Sheets
# ---------------------------------------------------------------------------
@app.callback(
    Output("filler-store",           "data"),
    Output("filler-status",          "children"),
    Output("filler-kpis-weight",     "children"),
    Output("filler-kpis-counter",    "children"),
    Output("fig-boxes",              "figure"),
    Output("fig-avg-weight",         "figure"),
    Output("fig-overfill",           "figure"),
    Output("fig-track-split",        "figure"),
    Output("fig-per-track-weight",   "figure"),
    Output("fig-counter-total",      "figure"),
    Output("fig-tc-increment",       "figure"),
    Output("fig-track-increments",   "figure"),
    Output("fig-gap",                "figure"),
    Output("fig-delta-split",        "figure"),
    Output("filler-shift-table",     "children"),
    Output("filler-counter-table",   "children"),
    Input("btn-filler-refresh",      "n_clicks"),
    prevent_initial_call=False,
)
def refresh_filler(n_clicks):
    empty_fig = go.Figure()
    try:
        download_sheet(GOOGLE_SHEET_URL, CACHE_XLS)
        df = load_filler_excel(CACHE_XLS)
        m  = compute_metrics(df)
    except Exception as exc:
        err = f"⚠ {exc}"
        return (None, err,
                [], [],
                *[empty_fig]*10,
                html.Div(err, className="text-danger"),
                html.Div(err, className="text-danger"))

    shifts = m["shifts"]
    sp     = m["setpoint"]

    # ── Weight KPI cards (dark v3 style) ────────────────────────────────────
    avg_w_status = "good" if abs(m["avg_filler"] - sp) <= 0.02 else "warn"
    kpis_weight = [
        _kpi_card_dark("Total Boxes",      f"{m['total_pc']:,.0f}",
                       f"Across {m['n_shifts']} shifts",        "primary"),
        _kpi_card_dark("Total Weight (Kg)", f"{m['total_wt']:,.0f}",
                       "Sum across all tracks",                  "secondary"),
        _kpi_card_dark("Avg Weight",       f"{m['avg_filler']:.4f} Kg",
                       f"Setpoint: {sp} Kg",                     avg_w_status),
        _kpi_card_dark("Production Rate",  f"{m['avg_rate']:,.0f} boxes/sh",
                       "Pieces / shift",                         "info"),
        _kpi_card_dark("Total Overfill",   f"{m['total_over']:+.2f} Kg",
                       f"Avg {m['avg_over_per_shift']:+.2f} Kg / shift",
                       "bad" if m["total_over"] < 0 else "warn"),
        _kpi_card_dark("Avg Overfill/sh",  f"{m['avg_over_per_shift']:+.2f} Kg",
                       f"Total ÷ {m['n_shifts']} shifts",       "warn"),
        _kpi_card_dark("Device Accuracy",  f"{m['device_accuracy']:.3f}%",
                       "100 − ((avg_w − 10) / 10)",
                       "good" if m["device_accuracy"] >= 99.9 else "bad"),
        _kpi_card_dark("Underfill Shifts", str(m["underfill_n"]),
                       f"Of {m['n_shifts']} total",
                       "good" if m["underfill_n"] == 0 else "bad"),
    ]

    # ── Counter KPI cards (dark v3 style) ───────────────────────────────────
    gap_status = ("good" if abs(m["gap_pct_total"]) < 0.05 else
                  "warn" if abs(m["gap_pct_total"]) < 0.2 else "bad")
    kpis_counter = [
        _kpi_card_dark("TC Start",      f"{m['tc_start']:,.0f}",
                       "Baseline (shift 0)",                     "secondary"),
        _kpi_card_dark("TC End",        f"{m['tc_end']:,.0f}",
                       f"After shift {m['n_shifts']}",           "secondary"),
        _kpi_card_dark("TC Delta",      f"{m['tc_delta']:,.0f}",
                       "Boxes counted by TC",                    "good"),
        _kpi_card_dark("Total Gap",
                       f"{m['total_gap']:+,.0f}",
                       f"{m['gap_pct_total']:+.3f}% of TC delta", gap_status),
        _kpi_card_dark("T1 Delta",      f"{m['t1_delta']:,.0f}",
                       f"{m['t1d_pct']}% of TC delta",           "info"),
        _kpi_card_dark("T2 Delta",      f"{m['t2_delta']:,.0f}",
                       f"{m['t2d_pct']}% of TC delta",           "info"),
        _kpi_card_dark("T3 Delta",      f"{m['t3_delta']:,.0f}",
                       f"{m['t3d_pct']}% of TC delta",           "info"),
        _kpi_card_dark("Avg TC/shift",
                       f"{m['tc_delta']//m['n_shifts']:,.0f}" if m["n_shifts"] else "—",
                       "Boxes per shift (TC)",                   "secondary"),
    ]

    # ── Charts ──────────────────────────────────────────────────────────────
    fig_boxes          = build_filler_boxes_chart(shifts)
    fig_avg_weight     = build_filler_weight_chart(shifts, sp)
    fig_overfill       = build_filler_overfill_chart(shifts)
    fig_track_split    = build_filler_track_split_chart(m)
    fig_per_track      = build_filler_per_track_weight_chart(shifts, sp)
    fig_counter_total  = build_counter_total_chart(shifts)
    fig_tc_inc         = build_counter_tc_increment_chart(shifts)
    fig_track_incs     = build_counter_track_increments_chart(shifts)
    fig_gap            = build_counter_gap_chart(shifts)
    fig_delta_split    = build_counter_delta_split_chart(m)

    # ── v3 Shift detail – tabbed per-shift track view ───────────────────────
    shift_buttons = [
        html.Button(
            f"S{s['shift']} · {'N' if s['type'] == 'night' else 'L'} {s['date']}",
            id={"type": "filler-sshift-btn", "index": i},
            n_clicks=0,
            className="filler-shift-btn" + (" active" if i == 0 else ""),
        )
        for i, s in enumerate(shifts)
    ]
    shift_panels = [
        html.Div(
            _build_shift_panel(s),
            id={"type": "filler-shift-panel", "index": i},
            style={"display": "block" if i == 0 else "none"},
        )
        for i, s in enumerate(shifts)
    ]
    shift_table = html.Div([
        html.Div(shift_buttons,
                 style={"display": "flex", "flexWrap": "wrap", "gap": "6px",
                        "marginBottom": "14px"}),
        *shift_panels,
    ])

    # ── v3 Counter detail table with pills ──────────────────────────────────
    counter_tbody = []
    for s in shifts:
        counter_tbody.append(html.Tr([
            html.Td(_v3_pill(f"S{s['shift']}", "info")),
            html.Td(_v3_pill(s["type"], "warn" if s["type"] == "night" else "ok")),
            html.Td(s["date"]),
            html.Td(f"{s['tt1']:,.0f}"),
            html.Td(f"{s['tt2']:,.0f}"),
            html.Td(f"{s['tt3']:,.0f}"),
            html.Td(f"{s['tc']:,.0f}"),
            html.Td(f"{s['diff_total']:+,.0f}", style={"color": "#7b82a0"}),
            html.Td(f"+{s['inc_t1']:,.0f}", style={"color": T1_CLR}),
            html.Td(f"+{s['inc_t2']:,.0f}", style={"color": T2_CLR}),
            html.Td(f"+{s['inc_t3']:,.0f}", style={"color": T3_CLR}),
            html.Td(f"+{s['inc_tc']:,.0f}", style={"color": TC_CLR}),
            html.Td(_gap_pill(int(s["gap"]))),
            html.Td(_gap_pct_pill(s["gap_pct"])),
        ]))
    tot_t1  = sum(s["inc_t1"] for s in shifts)
    tot_t2  = sum(s["inc_t2"] for s in shifts)
    tot_t3  = sum(s["inc_t3"] for s in shifts)
    tot_tc  = sum(s["inc_tc"] for s in shifts)
    tot_gap = sum(s["gap"]    for s in shifts)
    gpt     = (tot_gap / tot_tc * 100) if tot_tc else 0
    final_tc = shifts[-1]["tc"] if shifts else 0
    tfoot_row = html.Tr([
        html.Td("Totals", colSpan=3),
        html.Td("—"), html.Td("—"), html.Td("—"),
        html.Td(f"{final_tc:,.0f}", style={"color": TC_CLR}),
        html.Td("—"),
        html.Td(f"+{tot_t1:,.0f}", style={"color": T1_CLR}),
        html.Td(f"+{tot_t2:,.0f}", style={"color": T2_CLR}),
        html.Td(f"+{tot_t3:,.0f}", style={"color": T3_CLR}),
        html.Td(f"{tot_tc:,.0f}",  style={"color": TC_CLR}),
        html.Td(_gap_pill(int(tot_gap))),
        html.Td(_gap_pct_pill(gpt)),
    ], style={"background": "rgba(99,102,241,0.05)", "fontWeight": "500", "color": "#fff"})
    counter_table = html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(h) for h in [
                "Shift", "Type", "Date",
                "T T1", "T T2", "T T3", "TC", "Diff Total",
                "Inc T1", "Inc T2", "Inc T3", "Inc TC",
                "Gap", "Gap %",
            ]])),
            html.Tbody(counter_tbody),
            html.Tfoot([tfoot_row]),
        ], className="filler-detail-table", style={"minWidth": "900px"}),
    ], style={"overflowX": "auto"})

    status = f"✓ {m['n_shifts']} shifts  ·  {m['date_range']}  ·  updated {m['last_updated']}"
    return (
        m, status,
        kpis_weight, kpis_counter,
        fig_boxes, fig_avg_weight, fig_overfill, fig_track_split, fig_per_track,
        fig_counter_total, fig_tc_inc, fig_track_incs, fig_gap, fig_delta_split,
        shift_table, counter_table,
    )


# ---------------------------------------------------------------------------
# Filler – shift tab switching (pattern-matching, client-side style)
# ---------------------------------------------------------------------------
@app.callback(
    Output({"type": "filler-shift-panel", "index": ALL}, "style"),
    Output({"type": "filler-sshift-btn",  "index": ALL}, "className"),
    Input({"type": "filler-sshift-btn",   "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _switch_shift_tab(n_clicks_list):
    import json as _json
    from dash import callback_context
    if not callback_context.triggered:
        raise dash.exceptions.PreventUpdate
    try:
        triggered_id = _json.loads(
            callback_context.triggered[0]["prop_id"].split(".")[0]
        )
        active = triggered_id["index"]
    except Exception:
        raise dash.exceptions.PreventUpdate
    n = len(n_clicks_list)
    styles     = [{"display": "block" if i == active else "none"} for i in range(n)]
    classnames = ["filler-shift-btn active" if i == active else "filler-shift-btn"
                  for i in range(n)]
    return styles, classnames


# ---------------------------------------------------------------------------
# SCADA – Data Logs
# ---------------------------------------------------------------------------
@app.callback(
    Output("chart-superimposed",     "figure"),
    Output("data-status",            "children"),
    Output("scada-prod-kpis",        "children"),
    Output("scada-prod-cycles-table","children"),
    Input("btn-data",            "n_clicks"),
    State("shared-start-date",   "value"),
    State("shared-end-date",     "value"),
    State("shared-start-hour",   "value"),
    State("shared-start-min",    "value"),
    State("shared-end-hour",     "value"),
    State("shared-end-min",      "value"),
    prevent_initial_call=False,
)
def update_data_chart(n_clicks, start_date, end_date, sh, sm, eh, em):
    df = load_scada_data()
    if df.empty:
        return go.Figure(), "⚠ Data_log0.csv not found or empty.", [], []
    start_dt = _parse_dt(start_date, sh, sm)
    end_dt   = _parse_dt(end_date,   eh, em)
    if end_dt <= start_dt:
        return go.Figure(), "⚠ End time must be after start time.", [], []

    # ── Production statistics ────────────────────────────────────────────
    stats = _compute_production_stats(df, start_dt, end_dt)

    fig = build_superimposed_chart(df, start_dt, end_dt, stats["production_periods"])
    n_pts = len(df[(df["TimeString"] >= start_dt) & (df["TimeString"] <= end_dt)])

    # ── KPI cards ───────────────────────────────────────────────────────
    air_status  = "bad"  if stats["air_alarm_events"]  > 0 else "good"
    evap_status = "bad"  if stats["evap_alarm_events"] > 0 else "good"
    prod_status = "good" if stats["production_pct"] >= 50 else "warn"

    prod_kpis = [
        _kpi_card_dark(
            "Production Time",
            f"{stats['production_hours']} h",
            f"{stats['production_pct']:.1f}% of period  ({stats['total_hours']} h total)",
            prod_status,
        ),
        _kpi_card_dark(
            "Air Temp Alarm Events",
            str(stats["air_alarm_events"]),
            f"Air temp > −20 °C  ·  {stats['air_alarm_minutes']} min total",
            air_status,
        ),
        _kpi_card_dark(
            "Evap Temp Alarm Events",
            str(stats["evap_alarm_events"]),
            f"Evap temp > −37 °C during production  ·  {stats['evap_alarm_minutes']} min",
            evap_status,
        ),
        _kpi_card_dark(
            "Period",
            f"{start_dt:%d/%m %H:%M} → {end_dt:%d/%m %H:%M}",
            f"{n_pts} data points",
            "secondary",
        ),
    ]

    status_msg = (
        f"✓ {n_pts} data points  ·  "
        f"Production: {stats['production_hours']} h ({stats['production_pct']:.1f}%)  ·  "
        f"{len(stats['cycles'])} cycle(s) detected"
    )
    cycles_table = _build_cycles_table(stats["cycles"])
    return fig, status_msg, prod_kpis, cycles_table


# ---------------------------------------------------------------------------
# SCADA – Alarm Analysis
# ---------------------------------------------------------------------------
@app.callback(
    Output("chart-alarm-timeline",   "figure"),
    Output("chart-alarm-freq",       "figure"),
    Output("chart-alarm-dur",        "figure"),
    Output("alarm-kpis",             "children"),
    Output("alarm-table-container",  "children"),
    Output("alarm-status",           "children"),
    Input("btn-alarm",               "n_clicks"),
    State("shared-start-date",       "value"),
    State("shared-end-date",         "value"),
    State("shared-start-hour",       "value"),
    State("shared-start-min",        "value"),
    State("shared-end-hour",         "value"),
    State("shared-end-min",          "value"),
    prevent_initial_call=False,
)
def update_alarm_charts(n_clicks, start_date, end_date, sh, sm, eh, em):
    alarm_df = load_alarms()
    if alarm_df.empty:
        empty = go.Figure()
        return empty, empty, empty, [], html.Div("⚠ Alarm_log0.csv not found or empty."), ""

    start_dt = _parse_dt(start_date, sh, sm)
    end_dt   = _parse_dt(end_date,   eh, em)
    if end_dt <= start_dt:
        empty = go.Figure()
        return empty, empty, empty, [], html.Div("⚠ End must be after start."), ""

    period_df = alarm_df[
        (alarm_df["TimeString"] >= start_dt) &
        (alarm_df["TimeString"] <= end_dt)
    ]
    total_events  = len(period_df)
    process_count = len(period_df[period_df["MsgClass"] == 1])
    came_count    = len(period_df[(period_df["MsgClass"] == 1) & (period_df["StateAfter"] == 1)])
    unique_alarms = period_df[period_df["MsgClass"] == 1]["MsgText"].nunique()

    kpis = [
        _kpi_card("Total Alarm Events", str(total_events),   "primary"),
        _kpi_card("Process Alarms",     str(process_count),  "warning"),
        _kpi_card("Activations (came)", str(came_count),     "danger"),
        _kpi_card("Unique Alarm Types", str(unique_alarms),  "info"),
    ]

    fig_timeline = build_alarm_timeline(alarm_df, start_dt, end_dt)
    fig_freq     = build_alarm_frequency_chart(alarm_df, start_dt, end_dt)
    fig_dur      = build_alarm_duration_chart(alarm_df, start_dt, end_dt)

    table_df = period_df[["TimeString", "MsgClass", "StateAfter", "MsgText"]].copy()
    table_df["TimeString"] = table_df["TimeString"].dt.strftime("%d/%m/%Y %H:%M:%S")
    table_df["StateAfter"] = table_df["StateAfter"].map({1: "Came", 0: "Gone"})
    table_df["MsgClass"]   = table_df["MsgClass"].map(
        {1: "Process", 34: "CPU/PLC", 3: "System"}
    ).fillna(table_df["MsgClass"].astype(str))
    table_df.columns = ["Time", "Class", "State", "Message"]

    alarm_table = dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in table_df.columns],
        page_size=20, filter_action="native", sort_action="native",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor":"#343a40","color":"white","fontWeight":"bold"},
        style_cell={"fontSize":"12px","padding":"4px 8px"},
        style_data_conditional=[
            {"if":{"filter_query":'{State} = "Came"'},"backgroundColor":"#fff3cd"},
            {"if":{"filter_query":'{Class} = "CPU/PLC"'},"backgroundColor":"#f8d7da"},
        ],
    )

    status = (f"✓ {total_events} alarm events from "
              f"{start_dt:%d/%m/%Y %H:%M} to {end_dt:%d/%m/%Y %H:%M}")
    return fig_timeline, fig_freq, fig_dur, kpis, alarm_table, status


# ===========================================================================
# ── ENTRY POINT ────────────────────────────────────────────────────────────
# ===========================================================================
if __name__ == "__main__":
    print("Starting IQF Unified Dashboard …")
    print("Open  http://127.0.0.1:8050  in your browser.")
    app.run(debug=False, host="0.0.0.0", port=8050)
