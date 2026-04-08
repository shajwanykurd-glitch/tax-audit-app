# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  -  v15.2 (Ultimate Dark UI)
#  Architecture: Optimistic UI / Local-First Mutation
#  Theme: GitHub Dark Primer · Glassmorphism · Dropdown Fix Applied
#  Features: Pure English UI, Safe Concurrency, Manager Refresh Cooldown
# =============================================================================

import html as _html
import streamlit as st
import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import textwrap
import hashlib
import time
import pytz
from datetime import datetime, timedelta
import io

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
import gspread.exceptions

# ─────────────────────────────────────────────────────────────────────────────
#  0 · LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING)
_log = logging.getLogger("audit_portal")

# ─────────────────────────────────────────────────────────────────────────────
#  1 · PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tax Audit & Compliance Portal",
    layout="wide",
    initial_sidebar_state="expanded",
)

TZ = pytz.timezone("Asia/Baghdad")

# ─────────────────────────────────────────────────────────────────────────────
#  2 · SESSION STATE DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULTS: dict = dict(
    logged_in        = False,
    user_email       = "",
    user_role        = "",
    lang             = "en",
    date_filter      = "all",
    local_df         = None,
    local_headers    = None,
    local_col_map    = None,
    local_cache_key  = None,
    local_fetched_at = None,
    last_refresh_time = 0,
)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─────────────────────────────────────────────────────────────────────────────
#  3 · CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_SHEETS  = {"UsersDB"}
USERS_SHEET    = "UsersDB"
VISIBLE_SHEETS = ["Registration", "Salary Tax", "Annual Filing"]

COL_STATUS   = "Status"
COL_LOG      = "Audit_Log"
COL_AUDITOR  = "Auditor_ID"
COL_DATE     = "Update_Date"
COL_EVAL     = "Data_Evaluation"
COL_FEEDBACK = "Correction_Notes"

SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]

VAL_DONE    = "Processed"
VAL_PENDING = "Pending"

EVAL_OPTIONS = ["Good", "Bad / Incorrect", "Duplicate"]
VALID_ROLES = ["auditor", "manager", "admin"]

READ_TTL    = 600
BACKOFF_MAX = 5
_ROW_SEP    = " \u007c "

# ─────────────────────────────────────────────────────────────────────────────
#  4 · EXPONENTIAL BACKOFF
# ─────────────────────────────────────────────────────────────────────────────
_retry_policy = retry(
    retry        = retry_if_exception_type(
        (gspread.exceptions.APIError, gspread.exceptions.GSpreadException)
    ),
    wait         = wait_exponential(multiplier=1, min=2, max=32),
    stop         = stop_after_attempt(BACKOFF_MAX),
    before_sleep = before_sleep_log(_log, logging.WARNING),
    reraise      = True,
)

def _gsheets_call(func, *args, **kwargs):
    @_retry_policy
    def _inner():
        return func(*args, **kwargs)
    return _inner()


# ─────────────────────────────────────────────────────────────────────────────
#  5 · DARK MODE CSS  (Ultimate Dropdown Fix & GitHub Theme)
# ─────────────────────────────────────────────────────────────────────────────
def inject_css() -> None:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
  --bg-canvas:      #0D1117;
  --bg-default:     #161B22;
  --bg-subtle:      #1C2128;
  --bg-muted:       #21262D;
  --bg-overlay:     #30363D;

  --border-default: rgba(240,246,252,0.12);
  --border-muted:   rgba(240,246,252,0.06);

  --text-primary:   #E6EDF3;
  --text-secondary: #8B949E;
  --text-muted:     #484F58;

  --accent:         #7C3AED;
  --blue:           #388BFD;
  --accent-glow:    rgba(124,58,237,0.22);
}

/* ── Global Reset ── */
*, *::before, *::after {
  box-sizing: border-box !important;
  font-family: 'Inter', sans-serif !important;
}
html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"], .main, .block-container {
  background-color: var(--bg-canvas) !important;
  color: var(--text-primary) !important;
}
p, span, div, li, label, h1, h2, h3, h4, h5, h6, .stMarkdown {
  color: var(--text-primary) !important;
}
#MainMenu, footer, header { display: none !important; }

/* ── ULTIMATE DROPDOWN & SELECTBOX FIX ── */
div[data-baseweb="select"] > div {
    background-color: var(--bg-subtle) !important;
    border: 1px solid var(--border-default) !important;
    border-radius: 8px !important;
}
div[data-baseweb="select"] span {
    color: var(--text-primary) !important;
}
div[data-baseweb="popover"] > div,
ul[data-baseweb="menu"],
ul[role="listbox"],
div[data-testid="stVirtualDropdown"] {
    background-color: #161B22 !important;
    border: 1px solid #30363D !important;
    border-radius: 8px !important;
    box-shadow: 0 10px 30px rgba(0,0,0,0.6) !important;
}
li[role="option"], 
div[data-baseweb="popover"] li {
    background-color: #161B22 !important;
    color: #C9D1D9 !important;
    padding: 10px 14px !important;
    transition: background-color 0.1s ease !important;
}
li[role="option"]:hover,
li[role="option"][aria-selected="true"],
li[role="option"]:focus,
div[data-baseweb="popover"] li:hover {
    background-color: #1F6FEB !important;
    color: #FFFFFF !important;
}
li[role="option"] span,
div[data-baseweb="popover"] li span {
    color: inherit !important;
}

/* ── Forms, Inputs & Cards ── */
[data-testid="stMetricContainer"], div[data-testid="stForm"] {
    background-color: rgba(22, 27, 34, 0.82) !important;
    backdrop-filter: blur(16px) !important;
    border: 1px solid var(--border-default) !important;
    border-radius: 12px !important;
}
.stTextInput > div > div > input, .stTextArea > div > div > textarea {
  background: var(--bg-subtle) !important;
  color: var(--text-primary) !important;
  border: 1px solid var(--border-default) !important;
  border-radius: 8px !important;
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, var(--accent) 0%, var(--blue) 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}

/* ── Tables ── */
.gov-table-wrap { border: 1px solid var(--border-default); border-radius: 10px; overflow: hidden; margin-bottom: 16px;}
.gov-table { width: 100%; border-collapse: collapse; background: var(--bg-default); }
.gov-table th { background: var(--bg-muted) !important; padding: 12px !important; color: var(--text-secondary) !important; font-size: 0.65rem !important; text-transform: uppercase; }
.gov-table td { border-bottom: 1px solid rgba(240,246,252,0.05) !important; padding: 10px !important; font-size: 0.85rem !important; color: var(--text-primary) !important;}
.gov-table tr:nth-child(even) td { background-color: var(--bg-subtle) !important; }
.gov-table td.row-idx, .gov-table th.row-idx { font-family: 'JetBrains Mono', monospace !important; color: var(--text-secondary) !important; text-align: center !important;}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: rgba(22, 27, 34, 0.9) !important;
    backdrop-filter: blur(20px) !important;
    border-right: 1px solid var(--border-default) !important;
}

/* ── Micro UI Elements ── */
.s-chip { display:inline-flex;align-items:center;padding:3px 9px;border-radius:999px;font-size:.63rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase; }
.s-done { background:rgba(63,185,80,0.14); color:#3FB950 !important; border:1px solid rgba(63,185,80,0.35); }
.s-pending { background:rgba(210,153,34,0.14); color:#D29922 !important; border:1px solid rgba(210,153,34,0.35); }
.s-eval-good { background:rgba(63,185,80,0.14); color:#3FB950 !important; border:1px solid rgba(63,185,80,0.35); }
.s-eval-bad { background:rgba(248,81,73,0.14); color:#F85149 !important; border:1px solid rgba(248,81,73,0.35); }
.s-eval-dup { background:rgba(210,153,34,0.14); color:#D29922 !important; border:1px solid rgba(210,153,34,0.35); }

.chip { display: inline-flex; align-items: center; gap: 5px; padding: 4px 11px; border-radius: 999px; font-size: 0.67rem; font-weight: 700; text-transform: uppercase; }
.chip-pending { background:rgba(210,153,34,0.14); color:#D29922 !important; border:1px solid rgba(210,153,34,0.35); }
.chip-done { background:rgba(63,185,80,0.14); color:#3FB950 !important; border:1px solid rgba(63,185,80,0.35); }
.chip-admin { background:rgba(124,58,237,0.18); color:#388BFD !important; border:1px solid rgba(124,58,237,0.4); }

.section-title { font-size: 0.70rem; font-weight: 800; color: var(--blue) !important; margin: 22px 0 12px; text-transform: uppercase; letter-spacing: 0.08em; display: inline-flex; align-items: center; padding: 5px 12px; border-left: 3px solid var(--accent); background: rgba(124,58,237,0.18); border-radius: 0 6px 6px 0;}
.log-line { font-family: 'JetBrains Mono', monospace !important; font-size: 0.74rem; color: #8B949E !important; padding: 5px 0; border-bottom: 1px dashed rgba(240,246,252,0.06); }

.worklist-header { display: flex; align-items: center; justify-content: space-between; background: rgba(22,27,34,0.82); border: 1px solid rgba(240,246,252,0.1); border-top: 2px solid #7C3AED; border-radius: 14px; padding: 16px 22px; margin-bottom: 16px; }
.worklist-title { font-size: 0.98rem; font-weight: 700; color: #E6EDF3 !important; }
.worklist-sub { font-size: 0.74rem; color: #8B949E !important; margin-top: 3px; }
</style>""", unsafe_allow_html=True)

inject_css()


# ─────────────────────────────────────────────────────────────────────────────
#  6 · TRANSLATIONS  (100% English)
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry":"Ministry of Finance & Customs",
        "portal_title":"Tax Audit & Compliance Portal",
        "portal_sub":"Authorised Access Only",
        "classified":"CLASSIFIED - GOVERNMENT USE ONLY",
        "login_prompt":"Use your authorised credentials to access the system.",
        "email_field":"Official Email / User ID","password_field":"Password",
        "sign_in":"Sign in","sign_out":"Sign Out",
        "bad_creds":"Authentication failed. Verify your credentials and try again.",
        "workspace":"Active Case Register","overview":"Case Overview",
        "total":"Total Cases","processed":"Processed","outstanding":"Outstanding",
        "worklist_title":"Audit Worklist","worklist_sub":"Active cases pending review",
        "tab_worklist":"Worklist","tab_archive":"Archive",
        "tab_analytics":"Analytics","tab_logs":"Auditor Logs","tab_users":"User Admin",
        "select_case":"Select a case to inspect","audit_trail":"Audit Trail",
        "approve_save":"Approve & Commit Record","reopen":"Re-open Record (Admin)",
        "leaderboard":"Auditor Productivity Leaderboard","daily_trend":"Daily Processing Trend",
        "period":"Time Period","today":"Today","this_week":"This Week",
        "this_month":"This Month","all_time":"All Time",
        "add_auditor":"Register New User","update_pw":"Update Password",
        "remove_user":"Revoke Access","staff_dir":"Authorised Staff",
        "no_records":"No records found for this period.",
        "empty_sheet":"This register contains no data.",
        "saved_ok":"Record approved and committed. View updated instantly.",
        "dup_email":"This email address is already registered.",
        "fill_fields":"All fields are required.",
        "signed_as":"Authenticated as","role_admin":"System Administrator",
        "role_auditor":"Tax Auditor","role_manager":"Manager",
        "processing":"Processing Case",
        "no_history":"No audit trail for this record.",
        "records_period":"Records (period)","active_days":"Active Days","avg_per_day":"Avg / Day",
        "adv_filters":"Advanced Filters","f_email":"Auditor Email",
        "f_binder":"Company Binder No.","f_company":"Company Name","f_license":"License Number",
        "f_status":"Status","clear_filters":"Clear Filters",
        "active_filters":"Active filters","results_shown":"results shown",
        "no_match":"No records match the applied filters.",
        "status_all":"All Statuses","status_pending":"Pending Only","status_done":"Processed Only",
        "retry_warning":"Google Sheets quota reached - retrying with backoff...",
        "local_mode":"Optimistic UI Active","cache_age":"Cache TTL",
        "rbac_notice":"Info: Your role only has access to the Worklist and Archive.",
        "logs_title":"Auditor Activity Logs",
        "logs_sub":"Full processing history from project start",
        "logs_filter_all":"All Auditors","logs_auditor_sel":"Filter by Auditor",
        "logs_total":"Total Processed","logs_auditors":"Unique Auditors",
        "logs_date_range":"Date Range","logs_no_data":"No processed records found.",
        "logs_export_hdr":"Export Full Report",
        "logs_export_sub":"Download the complete audit log as a CSV file.",
        "logs_export_btn":"Download CSV Report",
        "logs_filename":"audit_log_report.csv","logs_cols_shown":"Columns displayed",
        "eval_label":"Data Entry Quality",
        "feedback_label":"Auditor Feedback / Notes for Agent",
        "feedback_placeholder":"Optional notes, issues found, corrections made...",
        "acc_ranking_title":"Data Entry Accuracy Ranking",
        "acc_agent":"Agent Email","acc_total":"Total",
        "acc_good":"Good","acc_bad":"Bad","acc_dup":"Dup","acc_rate":"Accuracy %",
        "acc_no_data":"No evaluation data available yet.",
        "archive_quality_note":"Tip: Columns Data_Evaluation & Correction_Notes are highlighted.",
        "role_label":"Role","change_role":"Change User Role",
        "change_role_sub":"Upgrade or downgrade any user's access level",
        "role_updated":"Role updated successfully.",
        "deep_search":"Deep Search","ds_binder":"Binder No.",
        "ds_company":"Company","ds_agent":"Agent Email",
        "ds_clear":"Clear","ds_showing":"Showing results for",
        "eval_breakdown":"Evaluation Breakdown per Agent",
        "eval_breakdown_sub":"Stacked view: Good / Bad / Duplicate per data-entry agent",
        "arch_search_title":"Archive Quick Search",
    }
}

def t(key: str) -> str:
    return _LANG["en"].get(key, key)


# ─────────────────────────────────────────────────────────────────────────────
#  7 · HELPERS 
# ─────────────────────────────────────────────────────────────────────────────
_COL_KEYWORDS: dict[str, list[str]] = {
    "binder":  ["binder","file no","file_no","رقم ملف الشركة","رقم_ملف_الشركة"],
    "company": ["company name","company_name","company","اسم الشركة","اسم_الشركة"],
    "license": ["license no","license_no","license","licence","رقم الترخيص","رقم_الترخيص"],
    "agent_email": ["data entry email","agent email","data_entry_email","agent_email","email"],
}

def detect_column(headers, kind):
    keywords = sorted(_COL_KEYWORDS.get(kind, []), key=len, reverse=True)
    skip_cols = set(SYSTEM_COLS) if kind == "agent_email" else set()
    for h in headers:
        if h in skip_cols: continue
        hl = h.lower().strip()
        for kw in keywords:
            if kw.lower() in hl: return h
    return None

def hash_pw(pw):   return hashlib.sha256(pw.encode()).hexdigest()
def now_str():     return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
def parse_dt(s):
    try:    return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except: return None

def clean_cell(value):
    if value is None: return ""
    s = str(value)
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff"): s = s.replace(ch, "")
    return s.replace("\xa0", " ").strip()

_EVAL_EMOJI_STRIP = str.maketrans("", "", "\U0001f7e2\U0001f534\u26a0\ufe0f")
def _normalise_eval(raw: str) -> str:
    return raw.translate(_EVAL_EMOJI_STRIP).strip()

def _raw_to_dataframe(raw):
    if not raw: return pd.DataFrame(), [], {}
    seen = {}; headers = []
    for h in raw[0]:
        h = clean_cell(h) or "Unnamed"
        if h in seen: seen[h] += 1; headers.append(f"{h}_{seen[h]}")
        else:         seen[h] = 0;  headers.append(h)
    if not headers: return pd.DataFrame(), [], {}
    n = len(headers); rows = []
    for r in raw[1:]:
        row = [clean_cell(c) for c in r]; row = (row + [""] * n)[:n]; rows.append(row)
    if not rows: return pd.DataFrame(columns=headers), headers, {}
    df = pd.DataFrame(rows, columns=headers)
    df = df[~(df == "").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns: df[sc] = ""
    df = df.fillna("").infer_objects(copy=False)
    return df, headers, {h: i+1 for i, h in enumerate(headers)}

def apply_period_filter(df, col, period):
    if period == "all" or col not in df.columns: return df
    now = datetime.now(TZ)
    if   period == "today":      cutoff = now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_week":  cutoff = (now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_month": cutoff = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    else: return df
    return df[df[col].apply(parse_dt) >= cutoff]

def _n_active(fe, fb, fc_, fl, fs):
    return sum([bool(fe.strip()),bool(fb.strip()),bool(fc_.strip()),bool(fl.strip()),fs!="all"])

def apply_filters_locally(df, f_email, f_binder, f_company, f_license, f_status,
                          col_binder, col_company, col_license):
    r = df.copy()
    if f_status == "pending": r = r[r[COL_STATUS] != VAL_DONE]
    elif f_status == "done":  r = r[r[COL_STATUS] == VAL_DONE]
    if f_email.strip():
        ecols = [c for c in r.columns if "auditor_email" in c.lower() or c == COL_AUDITOR]
        if ecols:
            mask = pd.Series(False, index=r.index)
            for ec in ecols: mask |= r[ec].astype(str).str.contains(f_email.strip(),case=False,na=False)
            r = r[mask]
    if f_binder.strip()  and col_binder  and col_binder  in r.columns:
        r = r[r[col_binder].astype(str).str.contains(f_binder.strip(),case=False,na=False)]
    if f_company.strip() and col_company and col_company in r.columns:
        r = r[r[col_company].astype(str).str.contains(f_company.strip(),case=False,na=False)]
    if f_license.strip() and col_license and col_license in r.columns:
        r = r[r[col_license].astype(str).str.contains(f_license.strip(),case=False,na=False)]
    return r

def build_auto_diff(record: dict, new_vals: dict) -> str:
    lines = []
    for field, new_v in new_vals.items():
        old_v = clean_cell(record.get(field, ""))
        new_v_clean = clean_cell(new_v)
        if old_v != new_v_clean:
            ov = old_v[:60] + "..." if len(old_v) > 60 else old_v
            nv = new_v_clean[:60] + "..." if len(new_v_clean) > 60 else new_v_clean
            lines.append(f"[{field}]: '{ov}' -> '{nv}'")
    if lines: return "Auto-Log:\n" + "\n".join(lines)
    return "Auto-Log: No field changes detected."


# ─────────────────────────────────────────────────────────────────────────────
#  8 · GOOGLE SHEETS 
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    raw = json.loads(st.secrets["json_key"],strict=False)
    pk  = raw["private_key"]
    pk  = pk.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","")
    pk  = pk.replace("\\n","").replace("\n","")
    pk  = "".join(pk.split()); pk = "\n".join(textwrap.wrap(pk,64))
    raw["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(raw,scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_sheet_metadata():
    spr = get_spreadsheet()
    return spr.id, [ws.title for ws in spr.worksheets()]

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_raw_sheet_cached(spreadsheet_id, ws_title):
    ws = get_spreadsheet().worksheet(ws_title)
    return _gsheets_call(ws.get_all_values), now_str()

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_users_cached(spreadsheet_id):
    ws = get_spreadsheet().worksheet(USERS_SHEET)
    return _gsheets_call(ws.get_all_records)

def _data_fingerprint(raw): return hashlib.md5(str(raw[:20]).encode()).hexdigest()

def get_local_data(spreadsheet_id, ws_title):
    raw, fetched_at = _fetch_raw_sheet_cached(spreadsheet_id, ws_title)
    fp = _data_fingerprint(raw); ck = f"{ws_title}::{fp}"
    if st.session_state.get("local_cache_key") != ck:
        df, h, cm = _raw_to_dataframe(raw)
        st.session_state.local_df         = df.copy()
        st.session_state.local_headers    = h
        st.session_state.local_col_map    = cm
        st.session_state.local_cache_key  = ck
        st.session_state.local_fetched_at = fetched_at
    return (st.session_state.local_df, st.session_state.local_headers,
            st.session_state.local_col_map, st.session_state.local_fetched_at or fetched_at)


# ─────────────────────────────────────────────────────────────────────────────
#  9 · OPTIMISTIC MUTATIONS 
# ─────────────────────────────────────────────────────────────────────────────
def _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix,
                              eval_val="", feedback_val=""):
    ldf = st.session_state.local_df
    if df_iloc < 0 or df_iloc >= len(ldf): return
    for f, v in new_vals.items():
        if f in ldf.columns: ldf.at[df_iloc, f] = v
    old = str(ldf.at[df_iloc, COL_LOG]).strip() if COL_LOG in ldf.columns else ""
    ldf.at[df_iloc, COL_STATUS]   = VAL_DONE
    ldf.at[df_iloc, COL_AUDITOR]  = auditor
    ldf.at[df_iloc, COL_DATE]     = ts_now
    if COL_LOG      in ldf.columns: ldf.at[df_iloc, COL_LOG]      = f"{log_prefix}\n{old}".strip()
    if COL_EVAL     in ldf.columns: ldf.at[df_iloc, COL_EVAL]     = eval_val
    if COL_FEEDBACK in ldf.columns: ldf.at[df_iloc, COL_FEEDBACK] = feedback_val
    st.session_state.local_df = ldf

def _apply_optimistic_reopen(df_iloc):
    ldf = st.session_state.local_df
    if df_iloc < 0 or df_iloc >= len(ldf): return
    ldf.at[df_iloc, COL_STATUS] = VAL_PENDING
    st.session_state.local_df = ldf


# ─────────────────────────────────────────────────────────────────────────────
#  10 · WRITE HELPERS 
# ─────────────────────────────────────────────────────────────────────────────
def ensure_system_cols_in_sheet(ws, headers, col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            np_ = len(headers) + 1
            if np_ > ws.col_count: _gsheets_call(ws.add_cols,max(4,np_-ws.col_count+1))
            _gsheets_call(ws.update_cell,1,np_,sc)
            headers.append(sc); col_map[sc] = np_
    return headers, col_map

def write_approval_to_sheet(ws_title, sheet_row, col_map, headers, new_vals, record,
                            auditor, ts_now, log_prefix,
                            eval_val="", feedback_val="") -> bool:
    ws = get_spreadsheet().worksheet(ws_title)
    headers, col_map = ensure_system_cols_in_sheet(ws, headers, col_map)

    # ── Optimistic concurrency check: one read of a single cell ──────────────
    if COL_STATUS in col_map:
        status_a1   = rowcol_to_a1(sheet_row, col_map[COL_STATUS])
        live_status = _gsheets_call(ws.acell, status_a1).value
        if live_status == VAL_DONE:
            return False   # another auditor already committed this row

    old     = str(record.get(COL_LOG, "")).strip()
    new_log = f"{log_prefix}\n{old}".strip()
    batch   = []
    for f, v in new_vals.items():
        if f in col_map and clean_cell(record.get(f, "")) != v:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[f]), "values": [[v]]})
    for cn, v in [
        (COL_STATUS,   VAL_DONE),
        (COL_AUDITOR,  auditor),
        (COL_DATE,     ts_now),
        (COL_LOG,      new_log),
        (COL_EVAL,     eval_val),
        (COL_FEEDBACK, feedback_val),
    ]:
        if cn in col_map:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[cn]), "values": [[v]]})
    if batch:
        _gsheets_call(ws.batch_update, batch)
    return True

def write_reopen_to_sheet(ws_title, sheet_row, col_map):
    ws = get_spreadsheet().worksheet(ws_title)
    if COL_STATUS in col_map:
        _gsheets_call(ws.update_cell, sheet_row, col_map[COL_STATUS], VAL_PENDING)

def authenticate(email: str, password: str, spreadsheet_id: str):
    email = email.lower().strip()
    if email == "admin" and password == st.secrets.get("admin_password", ""):
        return "admin"
    try:
        records = _fetch_users_cached(spreadsheet_id)
        df_u    = pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return None
        row = df_u[df_u["email"] == email]
        if row.empty: return None
        if hash_pw(password) != str(row["password"].values[0]): return None
        role = "auditor"
        if "role" in df_u.columns:
            r = str(row["role"].values[0]).strip().lower()
            if r in VALID_ROLES: role = r
        return role
    except:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  11 · HTML TABLE & PAGINATION 
# ─────────────────────────────────────────────────────────────────────────────
def _eval_chip(raw: str) -> str:
    if not raw or raw == "-": return "-"
    n = _normalise_eval(raw)
    if "Good"      in n: return f"<span class='s-chip s-eval-good'>{_html.escape(raw)}</span>"
    if "Bad"       in n or "Incorrect" in n:
                         return f"<span class='s-chip s-eval-bad'>{_html.escape(raw)}</span>"
    if "Duplicate" in n: return f"<span class='s-chip s-eval-dup'>{_html.escape(raw)}</span>"
    return f"<span class='s-chip s-pending'>{_html.escape(raw)}</span>"

def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty: st.info("No records to display."); return
    display_df = df.head(max_rows)
    th = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col == COL_LOG: continue
        extra = ""
        if col == COL_EVAL:       extra = " class='col-eval'"
        elif col == COL_FEEDBACK: extra = " class='col-feedback'"
        th += f"<th{extra}>{_html.escape(col)}</th>"
    rows = ""
    for idx, row in display_df.iterrows():
        r = f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col == COL_LOG: continue
            raw  = str(row[col]) if row[col] != "" else ""
            safe = _html.escape(raw)
            d    = safe or "-"
            if col == COL_STATUS:
                d = ("<span class='s-chip s-done'>Processed</span>" if raw == VAL_DONE
                     else "<span class='s-chip s-pending'>Pending</span>")
            elif col == COL_EVAL:
                d  = _eval_chip(raw)
                r += f"<td class='col-eval'>{d}</td>"; continue
            elif col == COL_FEEDBACK:
                trunc = (safe[:160] + "...") if len(safe) > 160 else (safe or "-")
                r += f"<td class='col-feedback'>{trunc}</td>"; continue
            elif len(raw) > 55:
                d = f"<span title='{safe}'>{safe[:52]}...</span>"
            r += f"<td>{d}</td>"
        rows += f"<tr>{r}</tr>"
    st.markdown(
        f"<div class='gov-table-wrap'><table class='gov-table'>"
        f"<thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>",
        unsafe_allow_html=True)

_PAGE_SIZE = 15

def render_paginated_table(df: pd.DataFrame, page_key: str, max_rows: int = 5000) -> None:
    if df.empty: render_html_table(df); return
    if page_key not in st.session_state: st.session_state[page_key] = 1
    total_rows  = min(len(df), max_rows)
    total_pages = max(1, -(-total_rows // _PAGE_SIZE))
    st.session_state[page_key] = max(1, min(st.session_state[page_key], total_pages))
    cur   = st.session_state[page_key]
    start = (cur - 1) * _PAGE_SIZE
    end   = min(start + _PAGE_SIZE, total_rows)
    render_html_table(df.iloc[start:end], max_rows=_PAGE_SIZE)
    if total_pages > 1:
        c_prev, c_info, c_next = st.columns([1, 3, 1])
        with c_prev:
            if st.button("← Prev", key=f"{page_key}_prev",
                         disabled=(cur <= 1), use_container_width=True):
                st.session_state[page_key] -= 1; st.rerun()
        with c_info:
            st.markdown(
                f"<div style='text-align:center;padding:8px 0;font-size:.72rem;"
                f"font-weight:600;color:var(--text-secondary);font-family:var(--mono);'>"
                f"Page {cur} / {total_pages}"
                f"<span style='font-weight:400;margin-left:10px;color:var(--text-muted);'>"
                f"({start+1}–{end} of {total_rows})</span></div>",
                unsafe_allow_html=True)
        with c_next:
            if st.button("Next →", key=f"{page_key}_next",
                         disabled=(cur >= total_pages), use_container_width=True):
                st.session_state[page_key] += 1; st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  12 · LOGIN PAGE 
# ─────────────────────────────────────────────────────────────────────────────
def render_login(spreadsheet_id: str) -> None:
    st.markdown("""
<style>
[data-testid="stSidebar"],
[data-testid="collapsedControl"],
header { display: none !important; }

/* Full-page animated gradient */
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"], .main {
    background: #0D1117 !important;
}
.block-container {
    display:         flex !important;
    flex-direction:  column !important;
    justify-content: center !important;
    align-items:     center !important;
    min-height:      100vh !important;
    padding:         1rem !important;
}

/* Mesh orbs */
.stApp::before {
    content:  '';
    position: fixed;
    inset:    0;
    z-index:  0;
    background:
        radial-gradient(ellipse 80% 60% at 20%  20%, rgba(124,58,237,0.14) 0%, transparent 65%),
        radial-gradient(ellipse 60% 50% at 80%  75%, rgba(56,139,253,0.11) 0%, transparent 60%),
        radial-gradient(ellipse 40% 40% at 55%  45%, rgba(63,185,80,0.06)  0%, transparent 55%);
    pointer-events: none;
}

/* Glass login card */
[data-testid="stForm"] {
    position:               relative;
    z-index:                1;
    background:             rgba(22,27,34,0.88) !important;
    backdrop-filter:        blur(24px) saturate(1.5) !important;
    -webkit-backdrop-filter:blur(24px) saturate(1.5) !important;
    border:                 1px solid rgba(240,246,252,0.10) !important;
    border-top:             2px solid rgba(124,58,237,0.70) !important;
    border-radius:          18px !important;
    padding:                40px 36px 32px !important;
    box-shadow:
        0 0 0 1px rgba(124,58,237,0.12),
        0 24px 60px rgba(0,0,0,0.70),
        0 0 40px rgba(124,58,237,0.12) !important;
    max-width:  440px !important;
    width:      100% !important;
    margin:     0 auto !important;
}

/* Submit button inside login */
[data-testid="stFormSubmitButton"] button {
    background:    linear-gradient(135deg, #7C3AED 0%, #388BFD 100%) !important;
    color:         #FFFFFF !important;
    border:        1px solid rgba(124,58,237,0.60) !important;
    border-radius: 10px !important;
    font-weight:   700 !important;
    font-size:     0.94rem !important;
    padding:       12px !important;
    width:         100% !important;
    margin-top:    8px !important;
    box-shadow:    0 4px 20px rgba(124,58,237,0.40) !important;
    transition:    all 0.20s cubic-bezier(0.34,1.56,0.64,1) !important;
    letter-spacing: 0.02em !important;
}
[data-testid="stFormSubmitButton"] button:hover {
    transform:  translateY(-2px) scale(1.01) !important;
    box-shadow: 0 10px 32px rgba(124,58,237,0.55) !important;
}
</style>""", unsafe_allow_html=True)

    with st.form("login_form", clear_on_submit=False):
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:6px;">
          <div style="width:60px;height:60px;margin:0 auto 16px;border-radius:14px;
                      background:linear-gradient(135deg,#7C3AED,#388BFD);
                      display:flex;align-items:center;justify-content:center;
                      font-size:1.8rem;
                      box-shadow:0 8px 28px rgba(124,58,237,0.45);">&#127963;</div>
          <div style="font-size:1.4rem;font-weight:800;color:#E6EDF3;letter-spacing:-.025em;margin-bottom:5px;">
              {_html.escape(t('portal_title'))}</div>
          <div style="display:inline-block;font-size:.58rem;font-weight:800;
                      color:#F85149;background:rgba(248,81,73,0.15);
                      border:1px solid rgba(248,81,73,0.35);
                      padding:4px 12px;border-radius:9999px;
                      letter-spacing:.14em;text-transform:uppercase;margin-bottom:14px;">
              {_html.escape(t('classified'))}</div>
          <div style="font-size:.84rem;color:#8B949E;margin-bottom:22px;font-weight:400;">
              {_html.escape(t('login_prompt'))}</div>
        </div>""", unsafe_allow_html=True)

        st.text_input(t("email_field"),    placeholder="user@mof.gov.iq",  key="_login_email")
        st.text_input(t("password_field"), type="password", placeholder="••••••••••", key="_login_pw")
        submitted = st.form_submit_button(t("sign_in"))

    if submitted:
        role = authenticate(
            st.session_state.get("_login_email", ""),
            st.session_state.get("_login_pw", ""),
            spreadsheet_id,
        )
        if role:
            em = st.session_state.get("_login_email", "")
            st.session_state.logged_in  = True
            st.session_state.user_email = "Admin" if role == "admin" else em.lower().strip()
            st.session_state.user_role  = role
            st.rerun()
        else:
            st.error(t("bad_creds"))


# ─────────────────────────────────────────────────────────────────────────────
#  13 · SIDEBAR 
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(headers, col_binder, col_company, col_license, is_admin, fetched_at):
    def clear_all_filters():
        for k in ("f_email","f_binder","f_company","f_license"):
            st.session_state[k] = ""
        st.session_state["f_status"] = "all"
        for pk in ("page_worklist","page_archive","page_logs"):
            st.session_state[pk] = 1

    role       = st.session_state.user_role
    role_label = {"admin":"System Administrator","manager":"Manager",
                  "auditor":"Tax Auditor"}.get(role, role.title())
    badge_cls  = {"admin":"chip-admin","manager":"chip-manager",
                  "auditor":"chip-audit"}.get(role,"chip-audit")

    with st.sidebar:
        st.markdown(f"""
        <div style="padding: 18px 16px 14px; border-top: 2px solid var(--accent);">
          <div style="font-size:0.95rem;font-weight:800;color:var(--text-primary);letter-spacing:-.02em;margin-bottom:3px;">
            {_html.escape(t('portal_title'))}</div>
          <div style="font-size:0.58rem;color:var(--text-secondary);letter-spacing:0.14em;text-transform:uppercase;font-weight:600;">
            {_html.escape(t('ministry'))}</div>
        </div>
        <hr style="border:none;border-top:1px solid var(--border-default);margin:0;"/>
        """, unsafe_allow_html=True)

        if st.session_state.get("user_role") in ("admin", "manager"):
            COOLDOWN = 600
            if "last_refresh_time" not in st.session_state:
                st.session_state.last_refresh_time = 0
            current_time  = time.time()
            time_passed   = current_time - st.session_state.last_refresh_time
            time_left_min = int((COOLDOWN - time_passed) / 60)
            can_refresh   = (st.session_state.user_role == "admin") or (time_passed >= COOLDOWN)

            def _do_refresh():
                _fetch_raw_sheet_cached.clear()
                _fetch_users_cached.clear()
                _fetch_sheet_metadata.clear()
                st.session_state.local_cache_key = None
                st.session_state.last_refresh_time = time.time()
                st.toast("Data refreshed for all users", icon="🔄")

            if can_refresh:
                st.button("↺  Refresh Data", key="sb_refresh",
                          use_container_width=True, on_click=_do_refresh)
            else:
                st.button(f"⏳  Wait {max(1,time_left_min)} min", key="sb_refresh_disabled",
                          disabled=True, use_container_width=True,
                          help="Managers may force-refresh once every 10 minutes.")

        st.markdown(f"""
        <div style="padding:9px 16px;background:var(--bg-subtle);border-bottom:1px solid var(--border-muted);">
          <span style="display:inline-flex;align-items:center;gap:5px;background:rgba(63,185,80,0.14);color:#3FB950;border:1px solid rgba(63,185,80,0.35);border-radius:999px;font-size:0.58rem;font-weight:800;letter-spacing:0.09em;text-transform:uppercase;padding:3px 9px;">
            ⚡ {_html.escape(t('local_mode'))}
          </span>
          <div style="font-size:0.60rem;color:var(--text-secondary);margin-top:5px;font-family:var(--mono);">
            {_html.escape(t('cache_age'))}: {READ_TTL//60} min &nbsp;·&nbsp; Last sync: {fetched_at[-8:] if fetched_at else '—'}
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<hr style='border:none;border-top:1px solid var(--border-default);margin:12px 0;'/>", unsafe_allow_html=True)

        st.markdown(f"""
        <div style="font-size:0.60rem;font-weight:800;letter-spacing:0.15em;text-transform:uppercase;color:var(--blue);margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--border-default);">
          {_html.escape(t('adv_filters'))}
        </div>
        """, unsafe_allow_html=True)
        
        status_opts = {"all":t("status_all"),"pending":t("status_pending"),"done":t("status_done")}
        st.selectbox(t("f_status"), options=list(status_opts.keys()),
                     format_func=lambda k: status_opts[k], key="f_status")
        
        for key, label, hint, disabled in [
            ("f_email",   t("f_email"),   COL_AUDITOR,                     False),
            ("f_binder",  t("f_binder"),  col_binder  or "not detected",    col_binder  is None),
            ("f_company", t("f_company"), col_company or "not detected",    col_company is None),
            ("f_license", t("f_license"), col_license or "not detected",    col_license is None),
        ]:
            st.markdown(
                f"<div style='font-size:0.60rem;font-weight:800;letter-spacing:0.14em;text-transform:uppercase;color:var(--text-secondary);margin-bottom:4px;margin-top:9px;'>{_html.escape(label)}"
                f"<span style='font-size:.58rem;font-weight:400;opacity:.55;color:var(--text-secondary);'> ({_html.escape(hint)})</span></div>",
                unsafe_allow_html=True)
            st.text_input(label, key=key, disabled=disabled, label_visibility="collapsed")

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.button(f"✕  {t('clear_filters')}", use_container_width=True,
                  key="clr_f", on_click=clear_all_filters)
        st.markdown("<hr style='border:none;border-top:1px solid var(--border-default);margin:12px 0;'/>", unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background:var(--bg-subtle);border:1px solid rgba(124,58,237,0.4);border-radius:10px;padding:13px 15px;margin-bottom:10px;box-shadow:inset 0 0 0 1px rgba(124,58,237,0.18);">
          <div style="font-size:0.60rem;font-weight:800;letter-spacing:0.14em;text-transform:uppercase;color:var(--text-secondary);margin-bottom:4px;">{_html.escape(t('signed_as'))}</div>
          <div style="font-size:0.84rem;font-weight:700;color:var(--text-primary);word-break:break-all;">{_html.escape(st.session_state.user_email)}</div>
          <span class="chip {badge_cls}" style="margin-top:8px;">{_html.escape(role_label)}</span>
        </div>""", unsafe_allow_html=True)

        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k, v in _DEFAULTS.items(): st.session_state[k] = v
            st.rerun()

    return (st.session_state.get("f_email",""), st.session_state.get("f_binder",""),
            st.session_state.get("f_company",""), st.session_state.get("f_license",""),
            st.session_state.get("f_status","all"))

def render_filter_bar(total, filtered, f_email, f_binder, f_company, f_license, f_status):
    n = _n_active(f_email, f_binder, f_company, f_license, f_status)
    if n == 0: return
    badges = ""
    if f_status!="all":  badges+=f"<span style='display:inline-flex;align-items:center;gap:4px;background:rgba(124,58,237,0.18);color:var(--text-primary);border:1px solid rgba(124,58,237,0.4);border-radius:999px;font-size:0.63rem;font-weight:600;padding:2px 9px;'>{_html.escape(f_status)}</span> "
    if f_email.strip():  badges+=f"<span style='display:inline-flex;align-items:center;gap:4px;background:rgba(124,58,237,0.18);color:var(--text-primary);border:1px solid rgba(124,58,237,0.4);border-radius:999px;font-size:0.63rem;font-weight:600;padding:2px 9px;'>{_html.escape(f_email.strip()[:20])}</span> "
    if f_binder.strip(): badges+=f"<span style='display:inline-flex;align-items:center;gap:4px;background:rgba(124,58,237,0.18);color:var(--text-primary);border:1px solid rgba(124,58,237,0.4);border-radius:999px;font-size:0.63rem;font-weight:600;padding:2px 9px;'>{_html.escape(f_binder.strip()[:20])}</span> "
    if f_company.strip():badges+=f"<span style='display:inline-flex;align-items:center;gap:4px;background:rgba(124,58,237,0.18);color:var(--text-primary);border:1px solid rgba(124,58,237,0.4);border-radius:999px;font-size:0.63rem;font-weight:600;padding:2px 9px;'>{_html.escape(f_company.strip()[:20])}</span> "
    if f_license.strip():badges+=f"<span style='display:inline-flex;align-items:center;gap:4px;background:rgba(124,58,237,0.18);color:var(--text-primary);border:1px solid rgba(124,58,237,0.4);border-radius:999px;font-size:0.63rem;font-weight:600;padding:2px 9px;'>{_html.escape(f_license.strip()[:20])}</span> "
    st.markdown(f"""
    <div style="background:var(--bg-subtle);border:1px solid var(--border-default);border-left:3px solid var(--accent);border-radius:10px;padding:11px 16px;margin-bottom:16px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,0.35);">
      <span style="font-size:.68rem;font-weight:800;color:var(--blue);text-transform:uppercase;letter-spacing:.08em;">
        {_html.escape(t('active_filters'))} ({n})</span>
      {badges}
      <span style="font-size:0.74rem;color:var(--text-secondary);margin-left:auto;font-family:var(--mono);">
        <strong style="color:var(--blue);">{filtered}</strong>/{total}&nbsp;{_html.escape(t('results_shown'))}
      </span>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  [C] DEEP SEARCH WIDGET 
# ─────────────────────────────────────────────────────────────────────────────
def render_deep_search_strip(key_prefix, col_binder, col_company, col_agent_email):
    def _clear():
        for sfx in ("_binder","_company","_agent"): st.session_state[f"{key_prefix}{sfx}"] = ""
        for pk in ("page_worklist","page_archive","page_logs"): st.session_state[pk] = 1

    st.markdown(
        f"<div style='background:var(--bg-subtle);border:1px solid var(--border-default);border-left:3px solid var(--accent);border-radius:10px;padding:12px 18px 16px;margin-bottom:18px;box-shadow:0 1px 4px rgba(0,0,0,0.35);'>"
        f"<div style='font-size:0.60rem;font-weight:800;letter-spacing:0.15em;text-transform:uppercase;color:var(--blue);margin-bottom:10px;'>{_html.escape(t('deep_search'))}</div></div>",
        unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns([1,1,1,0.32])
    with c1: st.text_input(t("ds_binder"),  key=f"{key_prefix}_binder",
                           placeholder=col_binder      or "not detected",
                           disabled=(col_binder is None))
    with c2: st.text_input(t("ds_company"), key=f"{key_prefix}_company",
                           placeholder=col_company     or "not detected",
                           disabled=(col_company is None))
    with c3: st.text_input(t("ds_agent"),   key=f"{key_prefix}_agent",
                           placeholder=col_agent_email or "not detected",
                           disabled=(col_agent_email is None))
    with c4:
        st.markdown("<div style='margin-top:22px;'>", unsafe_allow_html=True)
        st.button(t("ds_clear"), key=f"{key_prefix}_clr",
                  use_container_width=True, on_click=_clear)
        st.markdown("</div>", unsafe_allow_html=True)

    return (st.session_state.get(f"{key_prefix}_binder",""),
            st.session_state.get(f"{key_prefix}_company",""),
            st.session_state.get(f"{key_prefix}_agent",""))

def apply_deep_search(df, srch_binder, srch_company, srch_agent,
                      col_binder, col_company, col_agent_email):
    r = df.copy()
    if srch_binder.strip()  and col_binder      and col_binder      in r.columns:
        r = r[r[col_binder].astype(str).str.contains(srch_binder.strip(),case=False,na=False)]
    if srch_company.strip() and col_company     and col_company     in r.columns:
        r = r[r[col_company].astype(str).str.contains(srch_company.strip(),case=False,na=False)]
    if srch_agent.strip()   and col_agent_email and col_agent_email in r.columns:
        r = r[r[col_agent_email].astype(str).str.contains(srch_agent.strip(),case=False,na=False)]
    return r

def _deep_search_active(b, c, a): return any(x.strip() for x in (b, c, a))


# ─────────────────────────────────────────────────────────────────────────────
#  14 · WORKLIST 
# ─────────────────────────────────────────────────────────────────────────────
def render_worklist(pending_display, df, headers, col_map, ws_title,
                    f_email, f_binder, f_company, f_license, f_status):
    p_count = len(pending_display)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">{_html.escape(t('worklist_title'))}</div>
      <div class="worklist-sub">{_html.escape(t('worklist_sub'))}</div></div>
      <span class="chip chip-pending">{p_count} {_html.escape(t('outstanding'))}</span>
    </div>""", unsafe_allow_html=True)

    if pending_display.empty:
        st.info(t("no_match") if _n_active(f_email,f_binder,f_company,f_license,f_status)
                else "All cases processed.")
        return

    render_paginated_table(pending_display, page_key="page_worklist")
    st.markdown(f"<div class='section-title'>{_html.escape(t('select_case'))}</div>",
                unsafe_allow_html=True)

    label_col = next((h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row")
    opts = ["-"] + [
        f"Row {idx}{_ROW_SEP}{str(row.get(label_col,''))[:55]}"
        for idx, row in pending_display.iterrows()
    ]
    row_sel = st.selectbox("", opts, key="row_sel", label_visibility="collapsed")
    if row_sel == "-": return

    sheet_row = int(row_sel.split(_ROW_SEP)[0].replace("Row","").strip())
    df_iloc   = sheet_row - 2
    if df_iloc < 0 or df_iloc >= len(df): st.error("Row index out of range."); return
    record = df.iloc[df_iloc].to_dict()

    with st.expander(t("audit_trail"), expanded=False):
        history = str(record.get(COL_LOG,"")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip():
                    st.markdown(f'<div class="log-line">{_html.escape(line)}</div>',
                                unsafe_allow_html=True)
        else: st.caption(t("no_history"))

    st.markdown(f"<div class='section-title'>{_html.escape(t('processing'))} #{sheet_row}</div>",
                unsafe_allow_html=True)
    SKIP   = set(SYSTEM_COLS)
    fields = {k: v for k, v in record.items() if k not in SKIP}

    with st.form("audit_form"):
        new_vals = {}
        for fname, fval in fields.items():
            new_vals[fname] = st.text_input(fname, value=clean_cell(fval), key=f"field_{fname}")
        st.markdown("<hr style='border-top:1px solid var(--border-muted);margin:16px 0 12px;'/>",
                    unsafe_allow_html=True)
        eval_val     = st.selectbox(t("eval_label"), options=EVAL_OPTIONS, index=0, key="form_eval")
        manual_notes = st.text_area(t("feedback_label"), placeholder=t("feedback_placeholder"),
                                    key="form_feedback", height=100)
        do_submit    = st.form_submit_button(t("approve_save"), use_container_width=True)

    if do_submit:
        ts_now    = now_str()
        auditor   = st.session_state.user_email
        log_prefix = f"[✓] {auditor} | {ts_now}"
        auto_diff  = build_auto_diff(record, new_vals)
        feedback_combined = (f"{manual_notes.strip()}\n{auto_diff}".strip()
                             if manual_notes.strip() else auto_diff)
        with st.spinner("Committing record to Google Sheets..."):
            try:
                ok = write_approval_to_sheet(
                    ws_title, sheet_row, col_map, headers, new_vals, record,
                    auditor, ts_now, log_prefix,
                    eval_val=eval_val, feedback_val=feedback_combined)
                if not ok:
                    st.toast("⚠️ Another auditor already committed this record.")
                    st.session_state.local_df.at[df_iloc, COL_STATUS] = VAL_DONE
                    time.sleep(2); st.rerun(); return
            except gspread.exceptions.APIError as e:
                st.error(f"Write failed: {e}"); return
        _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix,
                                  eval_val=eval_val, feedback_val=feedback_combined)
        st.toast(t("saved_ok"), icon="✅")
        time.sleep(0.6); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  15 · ARCHIVE 
# ─────────────────────────────────────────────────────────────────────────────
def render_archive(done_view, df, col_map, ws_title, is_admin,
                   f_email, f_binder, f_company, f_license, f_status,
                   col_binder=None, col_company=None, col_license=None):
    def clear_arch():
        for k in ("arch_binder","arch_license","arch_company","arch_auditor"):
            st.session_state[k] = ""
        st.session_state["page_archive"] = 1

    d_count = len(done_view)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">Processed Archive</div>
      <div class="worklist-sub">Completed and committed audit records</div></div>
      <span class="chip chip-done">{d_count} {_html.escape(t('processed'))}</span>
    </div>""", unsafe_allow_html=True)

    st.markdown(f"<div class='section-title'>{_html.escape(t('arch_search_title'))}</div>",
                unsafe_allow_html=True)
    c1,c2,c3,c4,c5 = st.columns([1,1,1,1,0.28])
    with c1: s_binder  = st.text_input("Binder No.",    key="arch_binder",
                                       placeholder=col_binder  or "—", disabled=(col_binder  is None))
    with c2: s_license = st.text_input("License No.",   key="arch_license",
                                       placeholder=col_license or "—", disabled=(col_license is None))
    with c3: s_company = st.text_input("Company",       key="arch_company",
                                       placeholder=col_company or "—", disabled=(col_company is None))
    with c4: s_auditor = st.text_input("Auditor Email", key="arch_auditor",
                                       placeholder="e.g. auditor@mof.gov")
    with c5:
        st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
        st.button("✕", key="arch_clr", on_click=clear_arch, use_container_width=True)

    fv = done_view.copy()
    if s_binder.strip()  and col_binder  and col_binder  in fv.columns:
        fv = fv[fv[col_binder].astype(str).str.contains(s_binder.strip(),  case=False,na=False)]
    if s_license.strip() and col_license and col_license in fv.columns:
        fv = fv[fv[col_license].astype(str).str.contains(s_license.strip(), case=False,na=False)]
    if s_company.strip() and col_company and col_company in fv.columns:
        fv = fv[fv[col_company].astype(str).str.contains(s_company.strip(), case=False,na=False)]
    if s_auditor.strip() and COL_AUDITOR in fv.columns:
        fv = fv[fv[COL_AUDITOR].astype(str).str.contains(s_auditor.strip(), case=False,na=False)]

    st.markdown("<hr style='border:none;border-top:1px solid var(--border-default);margin:12px 0;'/>", unsafe_allow_html=True)
    if fv.empty:
        st.info("No processed records match the search.")
    else:
        if is_admin:
            st.markdown(
                f"<div style='background:rgba(124,58,237,0.18);border:1px solid rgba(124,58,237,0.4);"
                f"border-left:3px solid #7C3AED;border-radius:10px;"
                f"padding:9px 14px;margin-bottom:12px;font-size:.78rem;"
                f"color:#E6EDF3!important;font-weight:500;'>"
                f"{_html.escape(t('archive_quality_note'))}</div>", unsafe_allow_html=True)
        p_cols  = [COL_STATUS,COL_EVAL,COL_FEEDBACK,COL_AUDITOR,COL_DATE]
        o_cols  = [c for c in fv.columns if c not in p_cols and c != COL_LOG]
        ordered = [c for c in p_cols if c in fv.columns] + o_cols
        render_paginated_table(fv[ordered], page_key="page_archive")

    if is_admin and not done_view.empty:
        st.markdown("<hr style='border:none;border-top:1px solid var(--border-default);margin:12px 0;'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='section-title'>{_html.escape(t('reopen'))}</div>",
                    unsafe_allow_html=True)
        ropts = ["-"] + [f"Row {idx}" for idx in done_view.index]
        rsel  = st.selectbox("Select record to re-open:", ropts, key="reopen_sel")
        if rsel != "-":
            ridx    = int(rsel.split(" ")[1])
            df_iloc = ridx - 2
            if st.button(t("reopen"), key="reopen_btn"):
                with st.spinner("Re-opening..."):
                    try:    write_reopen_to_sheet(ws_title, ridx, col_map)
                    except gspread.exceptions.APIError as e: st.error(f"Error: {e}"); return
                _apply_optimistic_reopen(df_iloc); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  16 · ANALYTICS  
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df, col_agent_email=None, col_binder=None, col_company=None):
    pt  = "plotly_dark"
    pb  = "#161B22"      # --bg-default
    pg  = "#21262D"      # --bg-muted (grid lines)
    fc  = "#E6EDF3"      # --text-primary
    tc  = "#8B949E"      # --text-secondary (axis ticks)
    nvy = "#7C3AED"      # --accent
    blu = "#388BFD"      # --blue

    srch_binder, srch_company, srch_agent = render_deep_search_strip(
        "anal", col_binder, col_company, col_agent_email)
    work_df = apply_deep_search(df, srch_binder, srch_company, srch_agent,
                                col_binder, col_company, col_agent_email)

    if _deep_search_active(srch_binder, srch_company, srch_agent):
        terms = [_html.escape(x) for x in (srch_binder,srch_company,srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:rgba(124,58,237,0.18);border:1px solid rgba(124,58,237,0.4);"
            f"border-radius:10px;padding:9px 16px;margin-bottom:14px;"
            f"font-size:.78rem;color:#E6EDF3!important;font-weight:500;'>"
            f"{_html.escape(t('ds_showing'))} <strong>{' · '.join(terms)}</strong>"
            f" — <strong>{len(work_df)}</strong> records matched</div>",
            unsafe_allow_html=True)

    st.markdown(f"<div class='section-title'>{_html.escape(t('period'))}</div>",
                unsafe_allow_html=True)
    periods = [("all",t("all_time")),("today",t("today")),
               ("this_week",t("this_week")),("this_month",t("this_month"))]
    for cw,(pk,pl) in zip(st.columns(len(periods)), periods):
        lbl = f"[{pl}]" if st.session_state.date_filter==pk else pl
        if cw.button(lbl, use_container_width=True, key=f"pf_{pk}"):
            st.session_state.date_filter=pk; st.rerun()

    done_base = work_df[work_df[COL_STATUS]==VAL_DONE].copy()
    done_f    = apply_period_filter(done_base, COL_DATE, st.session_state.date_filter)
    if done_f.empty: st.info(t("no_records")); return

    ma,mb,mc = st.columns(3)
    ma.metric(t("records_period"), len(done_f))
    active = 0
    if COL_DATE in done_f.columns:
        active = done_f[COL_DATE].apply(lambda s: parse_dt(s).date() if parse_dt(s) else None).nunique()
    mb.metric(t("active_days"), active)
    mc.metric(t("avg_per_day"), f"{len(done_f)/max(active,1):.1f}")

    left, right = st.columns([1,1.6], gap="large")

    with left:
        st.markdown(f"<div class='section-title'>{_html.escape(t('leaderboard'))}</div>",
                    unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = done_f[COL_AUDITOR].replace("","-").value_counts().reset_index()
            lb.columns = ["Auditor","Count"]
            for i, r in lb.head(10).iterrows():
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:12px;padding:11px 16px;background:rgba(22,27,34,0.82);border:1px solid rgba(240,246,252,0.1);border-radius:10px;margin-bottom:7px;box-shadow:0 1px 4px rgba(0,0,0,0.35);">'
                    f'<span style="font-size:0.78rem;font-weight:700;min-width:30px;text-align:center;color:#8B949E;font-family:\'JetBrains Mono\', monospace !important;background:#21262D;border-radius:6px;padding:2px 5px;">{i+1}.</span>'
                    f'<span style="flex:1; font-size:.84rem; font-weight:500; color:#E6EDF3 !important;">{_html.escape(str(r["Auditor"]))}</span>'
                    f'<span style="font-size:0.86rem;font-weight:700;color:#388BFD !important;font-family:\'JetBrains Mono\', monospace !important;background:rgba(56,139,253,0.15);padding:3px 10px;border-radius:999px;border:1px solid rgba(56,139,253,0.4);">{r["Count"]}</span>'
                    f'</div>', unsafe_allow_html=True)
            fig = px.bar(lb.head(10), x="Count", y="Auditor", orientation="h",
                         color="Count", color_continuous_scale=[blu, nvy], template=pt)
            fig.update_traces(marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>Records: <b>%{x}</b><extra></extra>")
            fig.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Inter",color=fc,size=11),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color=tc)),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color=tc)),
                height=min(320, max(180, 36*len(lb.head(10)))))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown(f"<div class='section-title'>{_html.escape(t('daily_trend'))}</div>",
                    unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f["_date"] = done_f[COL_DATE].apply(
                lambda s: parse_dt(s).date() if parse_dt(s) else None)
            trend = done_f.dropna(subset=["_date"]).groupby("_date").size().reset_index(name="Records")
            trend.columns = ["Date","Records"]
            if not trend.empty:
                if len(trend) > 1:
                    rng   = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = trend.set_index("Date").reindex(rng.date,fill_value=0).reset_index()
                    trend.columns = ["Date","Records"]
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],
                    mode="none",fill="tozeroy",
                    fillcolor="rgba(124,58,237,0.10)",showlegend=False))
                fig2.add_trace(go.Scatter(
                    x=trend["Date"],y=trend["Records"],mode="lines+markers",
                    line=dict(color=nvy,width=2.5),
                    marker=dict(color=blu,size=6,line=dict(color=pb,width=2)),
                    hovertemplate="<b>%{x}</b><br>Records: <b>%{y}</b><extra></extra>"))
                fig2.update_layout(
                    template=pt,paper_bgcolor=pb,plot_bgcolor=pb,
                    font=dict(family="Inter",color=fc,size=11),
                    showlegend=False,margin=dict(l=8,r=8,t=10,b=8),
                    xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=tc)),
                    yaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=tc)),
                    height=380,hovermode="x unified")
                st.plotly_chart(fig2, use_container_width=True)
            else: st.info(t("no_records"))

    # ── Accuracy ranking ──────────────────────────────────────────────────────
    st.markdown(f"<div class='section-title'>{_html.escape(t('acc_ranking_title'))}</div>",
                unsafe_allow_html=True)
    if col_agent_email and col_agent_email in done_f.columns and COL_EVAL in done_f.columns:
        eval_df = done_f[[col_agent_email,COL_EVAL]].copy()
        eval_df[col_agent_email] = eval_df[col_agent_email].replace("","-")
        def _cls(v):
            n = _normalise_eval(v)
            if "Good"       in n: return "good"
            if "Bad"        in n or "Incorrect" in n: return "bad"
            if "Duplicate"  in n: return "dup"
            return "unrated"
        eval_df["_cls"] = eval_df[COL_EVAL].apply(_cls)
        acc = (eval_df.groupby(col_agent_email)["_cls"]
               .value_counts().unstack(fill_value=0).reset_index())
        for cn in ("good","bad","dup","unrated"):
            if cn not in acc.columns: acc[cn] = 0
        acc["Total"]    = acc["good"]+acc["bad"]+acc["dup"]+acc["unrated"]
        acc["Accuracy"] = (acc["good"]/acc["Total"].replace(0,1)*100).round(1)
        acc = acc.sort_values("Accuracy",ascending=False).reset_index(drop=True)

        th_row = (f"<tr><th>#</th><th>{t('acc_agent')}</th><th>{t('acc_total')}</th>"
                  f"<th>{t('acc_good')}</th><th>{t('acc_bad')}</th><th>{t('acc_dup')}</th>"
                  f"<th>{t('acc_rate')}</th></tr>")
        td_rows = ""
        for i, row in acc.iterrows():
            pct = row["Accuracy"]
            if pct >= 80:   rc="color:#3FB950 !important;font-weight:700!important;font-family:'JetBrains Mono', monospace !important;"; bc="#3FB950"
            elif pct >= 50: rc="color:#D29922 !important;font-weight:700!important;font-family:'JetBrains Mono', monospace !important;";  bc="#D29922"
            else:           rc="color:#F85149 !important;font-weight:700!important;font-family:'JetBrains Mono', monospace !important;";  bc="#F85149"
            bar = (f"<span style='background:#21262D;border-radius:999px;height:6px;width:80px;display:inline-block;vertical-align:middle;margin-left:8px;'>"
                   f"<span style='height:100%;border-radius:999px;width:{int(pct)}%;background:{bc};display:block;'></span>"
                   f"</span>")
            td_rows += (
                f"<tr>"
                f"<td style='color:#484F58;font-family:\'JetBrains Mono\', monospace !important;font-size:.68rem;text-align:center;'>{i+1}</td>"
                f"<td style='font-weight:500;'>{_html.escape(str(row[col_agent_email]))}</td>"
                f"<td style='font-family:\'JetBrains Mono\', monospace !important;font-weight:600;'>{int(row['Total'])}</td>"
                f"<td><span class='s-chip s-eval-good'>{int(row['good'])}</span></td>"
                f"<td><span class='s-chip s-eval-bad'>{int(row['bad'])}</span></td>"
                f"<td><span class='s-chip s-eval-dup'>{int(row['dup'])}</span></td>"
                f"<td style='{rc}'>{pct}% {bar}</td>"
                f"</tr>")
        st.markdown(
            f"<div class='gov-table-wrap'><table class='gov-table'>"
            f"<thead>{th_row}</thead><tbody>{td_rows}</tbody></table></div>",
            unsafe_allow_html=True)

        # Stacked bar chart — dark palette
        st.markdown(f"<div class='section-title'>{_html.escape(t('eval_breakdown'))}</div>",
                    unsafe_allow_html=True)
        st.caption(t("eval_breakdown_sub"))
        if not acc.empty:
            g_pct = (acc["good"]   /acc["Total"].replace(0,1)*100).round(1)
            b_pct = (acc["bad"]    /acc["Total"].replace(0,1)*100).round(1)
            d_pct = (acc["dup"]    /acc["Total"].replace(0,1)*100).round(1)
            u_pct = (acc["unrated"]/acc["Total"].replace(0,1)*100).round(1)
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name="Good",x=acc[col_agent_email],y=acc["good"],
                marker_color="#3FB950",
                hovertemplate="<b>%{x}</b><br>Good: <b>%{y}</b> (%{customdata[0]}%)<extra></extra>",
                customdata=list(zip(g_pct,acc["Total"]))))
            fig3.add_trace(go.Bar(name="Bad/Incorrect",x=acc[col_agent_email],y=acc["bad"],
                marker_color="#F85149",
                hovertemplate="<b>%{x}</b><br>Bad: <b>%{y}</b> (%{customdata[0]}%)<extra></extra>",
                customdata=list(zip(b_pct,acc["Total"]))))
            fig3.add_trace(go.Bar(name="Duplicate",x=acc[col_agent_email],y=acc["dup"],
                marker_color="#D29922",
                hovertemplate="<b>%{x}</b><br>Dup: <b>%{y}</b> (%{customdata[0]}%)<extra></extra>",
                customdata=list(zip(d_pct,acc["Total"]))))
            if acc["unrated"].sum() > 0:
                fig3.add_trace(go.Bar(name="Unrated",x=acc[col_agent_email],y=acc["unrated"],
                    marker_color="#484F58",
                    hovertemplate="<b>%{x}</b><br>Unrated: <b>%{y}</b> (%{customdata}%)<extra></extra>",
                    customdata=u_pct))
            fig3.update_layout(
                barmode="stack",template=pt,paper_bgcolor=pb,plot_bgcolor=pb,
                font=dict(family="Inter",color=fc,size=11),
                legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1,
                            font=dict(size=11),bgcolor="rgba(22,27,34,0.9)",
                            bordercolor=pg,borderwidth=1),
                margin=dict(l=8,r=8,t=40,b=60),
                xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=tc),
                           tickangle=-30,title=dict(text="Agent",font=dict(size=11,color=tc))),
                yaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=tc),
                           title=dict(text="Records",font=dict(size=11,color=tc))),
                height=400,hovermode="x")
            fig3.update_traces(marker_line_width=0)
            st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info(t("acc_no_data") +
                ("" if col_agent_email else " (Agent Email column not detected.)"))


# ─────────────────────────────────────────────────────────────────────────────
#  17 · AUDITOR LOGS 
# ─────────────────────────────────────────────────────────────────────────────
def render_auditor_logs(df, col_company, col_binder, col_agent_email=None):
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">{_html.escape(t('logs_title'))}</div>
      <div class="worklist-sub">{_html.escape(t('logs_sub'))}</div></div>
      <span class="chip chip-admin">Admin / Manager</span>
    </div>""", unsafe_allow_html=True)

    srch_binder, srch_company, srch_agent = render_deep_search_strip(
        "logs", col_binder, col_company, col_agent_email)

    done_df = df[df[COL_STATUS]==VAL_DONE].copy()
    if done_df.empty: st.info(t("logs_no_data")); return

    done_df = apply_deep_search(done_df, srch_binder, srch_company, srch_agent,
                                col_binder, col_company, col_agent_email)
    if _deep_search_active(srch_binder, srch_company, srch_agent):
        terms = [_html.escape(x) for x in (srch_binder,srch_company,srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:rgba(124,58,237,0.18);border:1px solid rgba(124,58,237,0.4);"
            f"border-radius:10px;padding:9px 14px;margin-bottom:12px;"
            f"font-size:.78rem;color:#E6EDF3!important;font-weight:500;'>"
            f"{_html.escape(t('ds_showing'))} <strong>{' · '.join(terms)}</strong>"
            f" — <strong>{len(done_df)}</strong> records matched</div>",
            unsafe_allow_html=True)

    if done_df.empty: st.info(t("logs_no_data")); return

    display_cols = [COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]
    if col_company     and col_company     in done_df.columns: display_cols.insert(1, col_company)
    if col_binder      and col_binder      in done_df.columns: display_cols.insert(1, col_binder)
    if col_agent_email and col_agent_email in done_df.columns: display_cols.insert(2, col_agent_email)
    seen_c: set = set()
    display_cols = [c for c in display_cols
                    if c in done_df.columns and not (c in seen_c or seen_c.add(c))]

    auditor_list = sorted(
        [a for a in done_df[COL_AUDITOR].unique() if str(a).strip() not in ("","-")],
        key=str.lower)
    all_opt = t("logs_filter_all")
    sel_aud = st.selectbox(t("logs_auditor_sel"), options=[all_opt]+auditor_list,
                           key="logs_auditor_sel")
    view_df = (done_df[done_df[COL_AUDITOR]==sel_aud].copy()
               if sel_aud!=all_opt else done_df.copy())

    total_p = len(view_df)
    uniq_a  = view_df[COL_AUDITOR].nunique()
    pdates  = view_df[COL_DATE].apply(parse_dt).dropna()
    dr_str  = (f"{pdates.min().strftime('%Y-%m-%d')} — {pdates.max().strftime('%Y-%m-%d')}"
               if not pdates.empty else "-")

    st.markdown(f"""
    <div style="background:rgba(22,27,34,0.82);backdrop-filter:blur(16px);border:1px solid rgba(240,246,252,0.1);border-top:2px solid #7C3AED;border-radius:14px;padding:20px 24px;box-shadow:0 4px 16px rgba(0,0,0,0.45);margin-bottom:16px;">
      <div style="display:flex;align-items:center;gap:28px;flex-wrap:wrap;">
        <div style="display:flex;flex-direction:column;gap:3px;">
          <span style="font-size:1.45rem;font-weight:800;color:#388BFD;letter-spacing:-.02em;font-family:'JetBrains Mono', monospace !important;">{total_p}</span>
          <span style="font-size:.60rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#8B949E;">{_html.escape(t('logs_total'))}</span>
        </div>
        <div style="width:1px;height:38px;background:rgba(240,246,252,0.1);"></div>
        <div style="display:flex;flex-direction:column;gap:3px;">
          <span style="font-size:1.45rem;font-weight:800;color:#388BFD;letter-spacing:-.02em;font-family:'JetBrains Mono', monospace !important;">{uniq_a}</span>
          <span style="font-size:.60rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#8B949E;">{_html.escape(t('logs_auditors'))}</span>
        </div>
        <div style="width:1px;height:38px;background:rgba(240,246,252,0.1);"></div>
        <div style="display:flex;flex-direction:column;gap:3px;">
          <span style="font-size:1.0rem;font-weight:800;color:#388BFD;letter-spacing:-.02em;font-family:'JetBrains Mono', monospace !important;">{_html.escape(dr_str)}</span>
          <span style="font-size:.60rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#8B949E;">{_html.escape(t('logs_date_range'))}</span>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    shown_label = " · ".join(display_cols)
    st.markdown(
        f"<div class='section-title'>{_html.escape(t('logs_cols_shown'))}: "
        f"<span style='font-weight:400;text-transform:none;letter-spacing:0;"
        f"color:#8B949E;'>{_html.escape(shown_label)}</span></div>",
        unsafe_allow_html=True)

    table_df = view_df[display_cols].copy()
    if COL_DATE in table_df.columns:
        table_df["_sort"] = table_df[COL_DATE].apply(parse_dt)
        table_df = (table_df.sort_values("_sort", ascending=False, na_position="last")
                    .drop(columns=["_sort"]))
    table_df = table_df.reset_index(drop=True)
    render_paginated_table(table_df, page_key="page_logs")

    csv_buf = io.StringIO()
    table_df.to_csv(csv_buf, index=False, encoding="utf-8-sig")
    csv_bytes = csv_buf.getvalue().encode("utf-8-sig")
    dtag  = datetime.now(TZ).strftime("%Y%m%d")
    atag  = (sel_aud.replace("@","_").replace(".","_") if sel_aud!=all_opt else "all_auditors")
    st.markdown(f"""
    <div style="background:var(--bg-subtle);border:1px solid rgba(63,185,80,0.35);border-left:3px solid #3FB950;border-radius:10px;padding:13px 18px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:18px;">
      <div>
        <div style="font-size:.80rem;font-weight:600;color:#E6EDF3;">{_html.escape(t('logs_export_hdr'))}</div>
        <div style="font-size:.68rem;color:#8B949E;margin-top:2px;">{_html.escape(t('logs_export_sub'))} · {total_p} rows · {len(display_cols)} columns</div>
      </div>
    </div>""", unsafe_allow_html=True)
    st.download_button(label=t("logs_export_btn"), data=csv_bytes,
                       file_name=f"audit_log_{atag}_{dtag}.csv", mime="text/csv",
                       key="logs_csv_download")


# ─────────────────────────────────────────────────────────────────────────────
#  18 · USER ADMIN 
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_role_col(df_u):
    if "role" not in df_u.columns:
        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
        col_idx = len(df_u.columns) + 1
        try:
            _gsheets_call(uws.update_cell, 1, col_idx, "role")
            for i in range(2, len(df_u) + 2):
                _gsheets_call(uws.update_cell, i, col_idx, "auditor")
            _fetch_users_cached.clear()
        except Exception:
            pass
    return df_u

def render_user_admin(spreadsheet_id):
    staff_raw = _fetch_users_cached(spreadsheet_id)
    staff     = pd.DataFrame(staff_raw) if staff_raw else pd.DataFrame()
    if not staff.empty: staff = _ensure_role_col(staff)

    cl, cr = st.columns([1,1], gap="large")
    with cl:
        st.markdown(f"<div class='section-title'>{_html.escape(t('add_auditor'))}</div>",
                    unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_e = st.text_input("Email", placeholder="user@mof.gov")
            nu_p = st.text_input("Password", type="password")
            nu_r = st.selectbox(t("role_label"), VALID_ROLES, format_func=lambda r: r.title())
            if st.form_submit_button("Register User", use_container_width=True):
                if nu_e.strip() and nu_p.strip():
                    already = (not staff.empty and
                               nu_e.lower().strip() in staff.get("email",pd.Series()).values)
                    if already: st.error(t("dup_email"))
                    else:
                        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                        _gsheets_call(uws.append_row,
                                      [nu_e.lower().strip(),hash_pw(nu_p.strip()),nu_r,now_str()])
                        _fetch_users_cached.clear()
                        st.success(f"{nu_e} registered as {nu_r}.")
                        time.sleep(0.7); st.rerun()
                else: st.warning(t("fill_fields"))

        st.markdown(f"<div class='section-title'>{_html.escape(t('update_pw'))}</div>",
                    unsafe_allow_html=True)
        if not staff.empty and "email" in staff.columns:
            with st.form("upd_pw_form"):
                se  = st.selectbox("Select staff", staff["email"].tolist(), key="upd_pw_sel")
                np_ = st.text_input("New Password", type="password")
                if st.form_submit_button("Update Password", use_container_width=True):
                    if np_.strip():
                        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                        cell = _gsheets_call(uws.find, se)
                        if cell:
                            _gsheets_call(uws.update_cell, cell.row, 2, hash_pw(np_.strip()))
                            st.success(f"Updated for {se}."); time.sleep(0.7); st.rerun()

        st.markdown(f"<div class='section-title'>{_html.escape(t('change_role'))}</div>",
                    unsafe_allow_html=True)
        st.caption(t("change_role_sub"))
        if not staff.empty and "email" in staff.columns:
            with st.form("change_role_form"):
                cr_email = st.selectbox("Select user", staff["email"].tolist(), key="cr_email_sel")
                cr_role  = st.selectbox("New Role", VALID_ROLES,
                                        format_func=lambda r: r.title(), key="cr_role_sel")
                if st.form_submit_button("Update Role", use_container_width=True):
                    try:
                        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                        hdr  = _gsheets_call(uws.row_values, 1)
                        rcol = (hdr.index("role") + 1) if "role" in hdr else len(hdr) + 1
                        if "role" not in hdr: _gsheets_call(uws.update_cell, 1, rcol, "role")
                        uc = _gsheets_call(uws.find, cr_email)
                        if uc:
                            _gsheets_call(uws.update_cell, uc.row, rcol, cr_role)
                            _fetch_users_cached.clear()
                            st.success(f"{t('role_updated')} ({cr_email} → {cr_role})")
                            time.sleep(0.7); st.rerun()
                        else: st.error("User not found.")
                    except Exception as e:
                        st.error(f"Role update failed: {e}")

    with cr:
        st.markdown(f"<div class='section-title'>{_html.escape(t('staff_dir'))}</div>",
                    unsafe_allow_html=True)
        if not staff.empty and "email" in staff.columns:
            show_cols = [c for c in ["email","role","created_at"] if c in staff.columns]
            tbl = staff[show_cols].copy().reset_index()
            th_html = ("<tr><th class='row-idx' style='text-align:center;'>#</th>" +
                       "".join(f"<th>{_html.escape(c)}</th>" for c in show_cols) + "</tr>")
            td_html = ""
            for _, row in tbl.iterrows():
                tr = f"<td class='row-idx' style='text-align:center; color:#8B949E; font-family:\'JetBrains Mono\', monospace;'>{row['index']}</td>"
                for c in show_cols:
                    val = str(row.get(c,"")) or "-"
                    if c == "role":
                        sr = val if val in VALID_ROLES else "auditor"
                        badge_bg = "rgba(124,58,237,0.18)" if sr=="admin" else "rgba(240,136,62,0.14)" if sr=="manager" else "rgba(63,185,80,0.14)"
                        badge_co = "#388BFD" if sr=="admin" else "#F0883E" if sr=="manager" else "#3FB950"
                        badge_bo = "rgba(124,58,237,0.4)" if sr=="admin" else "rgba(240,136,62,0.35)" if sr=="manager" else "rgba(63,185,80,0.35)"
                        tr += f"<td><span style='display:inline-block;background:{badge_bg};color:{badge_co};border:1px solid {badge_bo};border-radius:999px;padding:2px 10px;font-size:0.60rem;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;'>{_html.escape(val.title())}</span></td>"
                    else:
                        tr += f"<td>{_html.escape(val[:40])}</td>"
                td_html += f"<tr>{tr}</tr>"
            st.markdown(
                f"<div class='gov-table-wrap'><table class='gov-table'>"
                f"<thead><tr>{th_html}</tr></thead><tbody>{td_html}</tbody>"
                f"</table></div>", unsafe_allow_html=True)

            st.markdown(f"<div class='section-title'>{_html.escape(t('remove_user'))}</div>",
                        unsafe_allow_html=True)
            de = st.selectbox("Select to revoke", ["-"]+staff["email"].tolist(), key="del_sel")
            if de != "-":
                if st.button(f"Revoke access — {_html.escape(de)}", key="del_btn"):
                    spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                    cell = _gsheets_call(uws.find, de)
                    if cell:
                        _gsheets_call(uws.delete_rows, cell.row)
                        _fetch_users_cached.clear()
                        st.success(f"{de} revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered yet.")


# ─────────────────────────────────────────────────────────────────────────────
#  19 · MAIN CONTROLLER 
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        def _on_ws_change():
            for k in ("f_email","f_binder","f_company","f_license"):
                st.session_state[k] = ""
            st.session_state["f_status"] = "all"
            for k in ("arch_binder","arch_license","arch_company","arch_auditor"):
                st.session_state[k] = ""
            for pref in ("anal","logs"):
                for sfx in ("_binder","_company","_agent"):
                    st.session_state[f"{pref}{sfx}"] = ""
            for pk in ("page_worklist","page_archive","page_logs"):
                st.session_state[pk] = 1
            for k in ("local_cache_key","local_df","local_headers",
                      "local_col_map","active_ws_key"):
                st.session_state[k] = None

        sid, all_titles = _fetch_sheet_metadata()

        if USERS_SHEET not in all_titles:
            spr = get_spreadsheet()
            uw  = spr.add_worksheet(title=USERS_SHEET, rows="500", cols="4")
            _gsheets_call(uw.append_row, ["email","password","role","created_at"])
            _fetch_sheet_metadata.clear()
            all_titles.append(USERS_SHEET)

        if not st.session_state.logged_in:
            render_login(sid); return

        role          = st.session_state.user_role
        is_admin      = (role == "admin")
        is_manager    = (role == "manager")
        can_analytics = is_admin or is_manager

        ts_str = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-header">
          <div>
            <div class="page-title">{_html.escape(t('portal_title'))}</div>
            <div class="page-subtitle">{_html.escape(t('ministry'))}</div>
          </div>
          <div class="page-timestamp">{ts_str}</div>
        </div>""", unsafe_allow_html=True)

        atm       = {title.strip().lower(): title for title in all_titles}
        available = [atm[s.strip().lower()] for s in VISIBLE_SHEETS
                     if s.strip().lower() in atm]

        df = pd.DataFrame(); headers=[]; col_map={}; ws_title=None; fetched_at="-"

        if not available:
            st.warning("None of the configured worksheets found. Expected: " +
                       ", ".join(VISIBLE_SHEETS))
        else:
            ws_title = st.selectbox(t("workspace"), available,
                                    key="ws_sel", on_change=_on_ws_change)
            try:
                df, headers, col_map, fetched_at = get_local_data(sid, ws_title)
            except gspread.exceptions.WorksheetNotFound:
                st.error(f"Worksheet '{ws_title}' not found.")
            except gspread.exceptions.APIError as e:
                st.error(f"{t('retry_warning')}\n\n{e}")

        col_binder      = detect_column(headers, "binder")
        col_company     = detect_column(headers, "company")
        col_license     = detect_column(headers, "license")
        col_agent_email = detect_column(headers, "agent_email")

        f_email, f_binder, f_company, f_license, f_status = render_sidebar(
            headers, col_binder, col_company, col_license, is_admin, fetched_at)

        if not df.empty:
            st.markdown(f"<div class='section-title'>{_html.escape(t('overview'))}</div>",
                        unsafe_allow_html=True)
            total_n   = len(df)
            done_n    = int((df[COL_STATUS]==VAL_DONE).sum())
            pending_n = total_n - done_n
            pct       = done_n / total_n if total_n else 0
            m1,m2,m3  = st.columns(3)
            m1.metric(t("total"),       total_n)
            m2.metric(t("processed"),   done_n,    delta=f"{int(pct*100)}%")
            m3.metric(t("outstanding"), pending_n,
                      delta=f"{100-int(pct*100)}% remaining", delta_color="inverse")
            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;font-size:0.70rem;color:var(--text-secondary);font-weight:600;margin-bottom:3px;">
              <span>{_html.escape(t('processed'))}</span>
              <span>{int(pct*100)}%</span>
            </div>
            <div style="background:var(--bg-muted);border-radius:999px;height:6px;overflow:hidden;margin:5px 0 10px;">
              <div style="height:100%;border-radius:999px;background:linear-gradient(90deg, #7C3AED, #388BFD);transition:width 0.8s cubic-bezier(0.4,0,0.2,1);box-shadow:0 0 10px rgba(124,58,237,0.28);width:{int(pct*100)}%;"></div>
            </div>""", unsafe_allow_html=True)
            filtered_df = apply_filters_locally(
                df, f_email, f_binder, f_company, f_license, f_status,
                col_binder, col_company, col_license)
            render_filter_bar(total_n, len(filtered_df),
                              f_email, f_binder, f_company, f_license, f_status)
        else:
            filtered_df = pd.DataFrame()

        # ── Tab construction (RBAC) ───────────────────────────────────────────
        if is_admin:
            tabs = st.tabs([t("tab_worklist"),t("tab_archive"),
                            t("tab_analytics"),t("tab_logs"),t("tab_users")])
            t_work,t_arch,t_anal,t_logs,t_uadm = tabs
        elif is_manager:
            tabs = st.tabs([t("tab_worklist"),t("tab_archive"),
                            t("tab_analytics"),t("tab_logs")])
            t_work,t_arch,t_anal,t_logs = tabs
            t_uadm = None
        else:
            st.markdown(f"<div class='rbac-banner'>{_html.escape(t('rbac_notice'))}</div>",
                        unsafe_allow_html=True)
            tabs = st.tabs([t("tab_worklist"),t("tab_archive")])
            t_work,t_arch = tabs
            t_anal=t_logs=t_uadm=None

        with t_work:
            if not df.empty and ws_title:
                pv  = filtered_df[filtered_df[COL_STATUS]!=VAL_DONE].copy()
                pd_ = pv.copy(); pd_.index = pd_.index + 2
                render_worklist(pd_, df, headers, col_map, ws_title,
                                f_email, f_binder, f_company, f_license, f_status)

        with t_arch:
            if not df.empty and ws_title:
                dv = filtered_df[filtered_df[COL_STATUS]==VAL_DONE].copy()
                dv.index = dv.index + 2
                render_archive(dv, df, col_map, ws_title, is_admin,
                               f_email, f_binder, f_company, f_license, f_status,
                               col_binder=col_binder, col_company=col_company,
                               col_license=col_license)

        if can_analytics and t_anal is not None:
            with t_anal:
                if not df.empty:
                    render_analytics(df, col_agent_email=col_agent_email,
                                     col_binder=col_binder, col_company=col_company)

        if can_analytics and t_logs is not None:
            with t_logs:
                if df.empty: st.warning(t("empty_sheet"))
                else:        render_auditor_logs(df, col_company, col_binder, col_agent_email)

        if is_admin and t_uadm is not None:
            with t_uadm:
                render_user_admin(sid)

    except Exception as exc:
        st.error(f"System Error: {exc}")
        with st.expander("Technical Details", expanded=False):
            st.exception(exc)


if __name__ == "__main__":
    main()
