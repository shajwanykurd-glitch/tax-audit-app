# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  -  v15.4 (Complete & Unbroken)
#  Theme: GitHub Dark Primer · Glassmorphism · BaseWeb Dropdown Fix
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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
import logging
import gspread.exceptions

# ─────────────────────────────────────────────────────────────────────────────
#  0 · LOGGING & CONFIG
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING)
_log = logging.getLogger("audit_portal")

st.set_page_config(page_title="Tax Audit & Compliance Portal", layout="wide", initial_sidebar_state="expanded")
TZ = pytz.timezone("Asia/Baghdad")

# ─────────────────────────────────────────────────────────────────────────────
#  1 · STATE DEFAULTS & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULTS = dict(logged_in=False, user_email="", user_role="", lang="en", date_filter="all",
                 local_df=None, local_headers=None, local_col_map=None, local_cache_key=None,
                 local_fetched_at=None, last_refresh_time=0)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state: st.session_state[_k] = _v

SYSTEM_SHEETS  = {"UsersDB"}
USERS_SHEET    = "UsersDB"
VISIBLE_SHEETS = ["Registration", "Salary Tax", "Annual Filing"]

COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK = "Status", "Audit_Log", "Auditor_ID", "Update_Date", "Data_Evaluation", "Correction_Notes"
SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]
VAL_DONE, VAL_PENDING = "Processed", "Pending"
EVAL_OPTIONS = ["Good", "Bad / Incorrect", "Duplicate"]
VALID_ROLES = ["auditor", "manager", "admin"]
READ_TTL = 600
BACKOFF_MAX = 5
_ROW_SEP = " \u007c "

_retry_policy = retry(
    retry=retry_if_exception_type((gspread.exceptions.APIError, gspread.exceptions.GSpreadException)),
    wait=wait_exponential(multiplier=1, min=2, max=32),
    stop=stop_after_attempt(BACKOFF_MAX),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True
)

def _gsheets_call(func, *args, **kwargs):
    @_retry_policy
    def _inner(): return func(*args, **kwargs)
    return _inner()

# ─────────────────────────────────────────────────────────────────────────────
#  2 · DARK MODE CSS (Ultimate BaseWeb Fix)
# ─────────────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
  --bg-canvas: #0D1117; --bg-default: #161B22; --bg-subtle: #1C2128; --bg-muted: #21262D;
  --border-default: rgba(240,246,252,0.12); --border-strong: rgba(240,246,252,0.18);
  --text-primary: #E6EDF3; --text-secondary: #8B949E; --text-muted: #484F58;
  --accent: #7C3AED; --blue: #388BFD; --accent-glow: rgba(124,58,237,0.22);
}
*, *::before, *::after { box-sizing: border-box !important; font-family: 'Inter', sans-serif !important; }
html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"], .main, .block-container { background-color: var(--bg-canvas) !important; color: var(--text-primary) !important; }
#MainMenu, footer, header { display: none !important; }

/* BaseWeb Dropdown & Selectbox Fix */
div[data-baseweb="select"] > div { background-color: var(--bg-subtle) !important; border: 1px solid var(--border-default) !important; border-radius: 8px !important; }
div[data-baseweb="select"] span { color: var(--text-primary) !important; }
div[data-baseweb="popover"] > div, ul[data-baseweb="menu"], ul[role="listbox"], div[data-testid="stVirtualDropdown"] { background-color: #161B22 !important; border: 1px solid #30363D !important; border-radius: 8px !important; box-shadow: 0 10px 30px rgba(0,0,0,0.6) !important; }
li[role="option"], div[data-baseweb="popover"] li { background-color: #161B22 !important; color: #C9D1D9 !important; padding: 10px 14px !important; transition: background-color 0.1s ease !important; }
li[role="option"]:hover, li[role="option"][aria-selected="true"], li[role="option"]:focus, div[data-baseweb="popover"] li:hover { background-color: #1F6FEB !important; color: #FFFFFF !important; }
li[role="option"] span, div[data-baseweb="popover"] li span { color: inherit !important; }

/* General UI */
[data-testid="stSidebar"] { background-color: rgba(22, 27, 34, 0.9) !important; backdrop-filter: blur(20px) !important; border-right: 1px solid var(--border-default) !important; }
[data-testid="stMetricContainer"], div[data-testid="stForm"] { background-color: rgba(22, 27, 34, 0.82) !important; backdrop-filter: blur(16px) !important; border: 1px solid var(--border-default) !important; border-radius: 12px !important; }
.stTextInput > div > div > input, .stTextArea > div > div > textarea { background: var(--bg-subtle) !important; color: var(--text-primary) !important; border: 1px solid var(--border-default) !important; border-radius: 8px !important; }
.stButton > button { background: linear-gradient(135deg, var(--accent) 0%, var(--blue) 100%) !important; color: white !important; border: none !important; border-radius: 8px !important; font-weight: 600 !important; }

/* Tables */
.gov-table-wrap { border: 1px solid var(--border-default); border-radius: 10px; overflow: hidden; margin-bottom: 16px;}
.gov-table { width: 100%; border-collapse: collapse; background: var(--bg-default); }
.gov-table th { background: var(--bg-muted) !important; padding: 12px !important; color: var(--text-secondary) !important; font-size: 0.65rem !important; text-transform: uppercase; }
.gov-table td { border-bottom: 1px solid rgba(240,246,252,0.05) !important; padding: 10px !important; font-size: 0.85rem !important; color: var(--text-primary) !important;}
.gov-table tr:nth-child(even) td { background-color: var(--bg-subtle) !important; }
.gov-table td.row-idx, .gov-table th.row-idx { font-family: 'JetBrains Mono', monospace !important; color: var(--text-secondary) !important; text-align: center !important;}

/* Micro UI Elements */
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
</style>
    """, unsafe_allow_html=True)

inject_css()

# ─────────────────────────────────────────────────────────────────────────────
#  3 · TRANSLATIONS (English)
# ─────────────────────────────────────────────────────────────────────────────
_LANG = {
    "en": {
        "ministry":"Ministry of Finance & Customs", "portal_title":"Tax Audit & Compliance Portal",
        "classified":"CLASSIFIED - GOVERNMENT USE", "login_prompt":"Use your authorised credentials.",
        "email_field":"Official Email / User ID", "password_field":"Password", "sign_in":"Sign In",
        "workspace":"Active Case Register", "overview":"Case Overview", "total":"Total Cases",
        "processed":"Processed", "outstanding":"Outstanding", "worklist_title":"Audit Worklist",
        "worklist_sub":"Active cases pending review", "tab_worklist":"Worklist", "tab_archive":"Archive",
        "tab_analytics":"Analytics", "tab_logs":"Auditor Logs", "tab_users":"User Admin",
        "select_case":"Select a case to inspect", "audit_trail":"Audit Trail", "approve_save":"Approve & Commit",
        "reopen":"Re-open Record (Admin)", "leaderboard":"Auditor Productivity", "daily_trend":"Processing Trend",
        "period":"Time Period", "today":"Today", "this_week":"This Week", "this_month":"This Month",
        "all_time":"All Time", "add_auditor":"Register User", "update_pw":"Update Password",
        "remove_user":"Revoke Access", "staff_dir":"Authorised Staff", "no_records":"No records found.",
        "empty_sheet":"Register is empty.", "saved_ok":"Record approved and committed.",
        "dup_email":"Email already registered.", "fill_fields":"All fields required.", "signed_as":"Authenticated as",
        "processing":"Processing Case", "no_history":"No audit trail.", "records_period":"Records",
        "active_days":"Active Days", "avg_per_day":"Avg / Day", "adv_filters":"Filters",
        "f_email":"Auditor Email", "f_binder":"Binder No.", "f_company":"Company Name", "f_license":"License No.",
        "f_status":"Status", "clear_filters":"Clear Filters", "active_filters":"Active filters",
        "results_shown":"results", "no_match":"No records match.", "status_all":"All", "status_pending":"Pending Only",
        "status_done":"Processed Only", "local_mode":"Optimistic UI", "cache_age":"Cache TTL",
        "rbac_notice":"Restricted to Worklist and Archive.", "logs_title":"Activity Logs",
        "logs_sub":"Processing history", "logs_filter_all":"All Auditors", "logs_auditor_sel":"Filter Auditor",
        "logs_total":"Total Processed", "logs_auditors":"Unique Auditors", "logs_date_range":"Date Range",
        "logs_no_data":"No records found.", "logs_export_hdr":"Export Report", "logs_export_sub":"Download CSV file.",
        "logs_export_btn":"Download CSV", "logs_cols_shown":"Columns", "eval_label":"Data Entry Quality",
        "feedback_label":"Correction Notes", "feedback_placeholder":"Notes for Agent...",
        "acc_ranking_title":"Accuracy Ranking", "acc_agent":"Agent", "acc_total":"Total", "acc_good":"Good",
        "acc_bad":"Bad", "acc_dup":"Dup", "acc_rate":"Accuracy %", "acc_no_data":"No evaluation data.",
        "archive_quality_note":"Quality columns highlighted.", "role_label":"Role", "change_role":"Change Role",
        "change_role_sub":"Upgrade/downgrade level", "role_updated":"Role updated.", "deep_search":"Deep Search",
        "ds_binder":"Binder No.", "ds_company":"Company", "ds_agent":"Agent Email", "ds_clear":"Clear",
        "ds_showing":"Showing results for", "eval_breakdown":"Evaluation Breakdown", "eval_breakdown_sub":"Stacked view",
        "arch_search_title":"Archive Search",
    }
}
def t(key: str) -> str: return _LANG["en"].get(key, key)

# ─────────────────────────────────────────────────────────────────────────────
#  4 · DATA HELPERS
# ─────────────────────────────────────────────────────────────────────────────
_COL_KEYWORDS = {
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

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def now_str(): return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
def parse_dt(s):
    try: return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except: return None
def clean_cell(value): return str(value).replace("\u200b", "").replace("\xa0", " ").strip() if value is not None else ""
def _normalise_eval(raw: str): return str(raw).strip()

def _raw_to_dataframe(raw):
    if not raw: return pd.DataFrame(), [], {}
    seen = {}; headers = []
    for h in raw[0]:
        h = clean_cell(h) or "Unnamed"
        if h in seen: seen[h] += 1; headers.append(f"{h}_{seen[h]}")
        else: seen[h] = 0; headers.append(h)
    if not headers: return pd.DataFrame(), [], {}
    n = len(headers); rows = []
    for r in raw[1:]:
        row = [clean_cell(c) for c in r]; rows.append((row + [""] * n)[:n])
    df = pd.DataFrame(rows, columns=headers)
    df = df[~(df == "").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns: df[sc] = ""
    return df.fillna("").infer_objects(copy=False), headers, {h: i+1 for i, h in enumerate(headers)}

def apply_period_filter(df, col, period):
    if period == "all" or col not in df.columns: return df
    now = datetime.now(TZ)
    if period == "today": cutoff = now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_week": cutoff = (now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_month": cutoff = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    else: return df
    return df[df[col].apply(parse_dt) >= cutoff]

def _n_active(fe, fb, fc, fl, fs): return sum([bool(fe.strip()),bool(fb.strip()),bool(fc.strip()),bool(fl.strip()),fs!="all"])

def apply_filters_locally(df, f_email, f_binder, f_company, f_license, f_status, col_binder, col_company, col_license):
    r = df.copy()
    if f_status == "pending": r = r[r[COL_STATUS] != VAL_DONE]
    elif f_status == "done": r = r[r[COL_STATUS] == VAL_DONE]
    if f_email.strip():
        ecols = [c for c in r.columns if "auditor_email" in c.lower() or c == COL_AUDITOR]
        if ecols:
            mask = pd.Series(False, index=r.index)
            for ec in ecols: mask |= r[ec].astype(str).str.contains(f_email.strip(),case=False,na=False)
            r = r[mask]
    if f_binder.strip() and col_binder and col_binder in r.columns: r = r[r[col_binder].astype(str).str.contains(f_binder.strip(),case=False,na=False)]
    if f_company.strip() and col_company and col_company in r.columns: r = r[r[col_company].astype(str).str.contains(f_company.strip(),case=False,na=False)]
    if f_license.strip() and col_license and col_license in r.columns: r = r[r[col_license].astype(str).str.contains(f_license.strip(),case=False,na=False)]
    return r

def build_auto_diff(record: dict, new_vals: dict) -> str:
    lines = []
    for field, new_v in new_vals.items():
        old_v = clean_cell(record.get(field, ""))
        new_v_clean = clean_cell(new_v)
        if old_v != new_v_clean:
            lines.append(f"[{field}]: '{old_v[:60]}' -> '{new_v_clean[:60]}'")
    return "Auto-Log:\n" + "\n".join(lines) if lines else "Auto-Log: No field changes detected."

# ─────────────────────────────────────────────────────────────────────────────
#  5 · GOOGLE SHEETS & MUTATIONS
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    raw = json.loads(st.secrets["json_key"],strict=False)
    pk = raw["private_key"].replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","").replace("\\n","").replace("\n","")
    raw["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{chr(10).join(textwrap.wrap(''.join(pk.split()),64))}\n-----END PRIVATE KEY-----\n"
    return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(raw,scope)).open("site CIT QA - Tranche 4")

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_sheet_metadata():
    return get_spreadsheet().id, [ws.title for ws in get_spreadsheet().worksheets()]

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_raw_sheet_cached(spreadsheet_id, ws_title):
    return _gsheets_call(get_spreadsheet().worksheet(ws_title).get_all_values), now_str()

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_users_cached(spreadsheet_id):
    return _gsheets_call(get_spreadsheet().worksheet(USERS_SHEET).get_all_records)

def get_local_data(spreadsheet_id, ws_title):
    raw, fetched_at = _fetch_raw_sheet_cached(spreadsheet_id, ws_title)
    ck = f"{ws_title}::{hashlib.md5(str(raw[:20]).encode()).hexdigest()}"
    if st.session_state.get("local_cache_key") != ck:
        df, h, cm = _raw_to_dataframe(raw)
        st.session_state.update(local_df=df.copy(), local_headers=h, local_col_map=cm, local_cache_key=ck, local_fetched_at=fetched_at)
    return st.session_state.local_df, st.session_state.local_headers, st.session_state.local_col_map, st.session_state.local_fetched_at

def _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix, eval_val="", feedback_val=""):
    ldf = st.session_state.local_df
    if 0 <= df_iloc < len(ldf):
        for f, v in new_vals.items():
            if f in ldf.columns: ldf.at[df_iloc, f] = v
        old = str(ldf.at[df_iloc, COL_LOG]).strip() if COL_LOG in ldf.columns else ""
        ldf.at[df_iloc, COL_STATUS], ldf.at[df_iloc, COL_AUDITOR], ldf.at[df_iloc, COL_DATE] = VAL_DONE, auditor, ts_now
        if COL_LOG in ldf.columns: ldf.at[df_iloc, COL_LOG] = f"{log_prefix}\n{old}".strip()
        if COL_EVAL in ldf.columns: ldf.at[df_iloc, COL_EVAL] = eval_val
        if COL_FEEDBACK in ldf.columns: ldf.at[df_iloc, COL_FEEDBACK] = feedback_val
        st.session_state.local_df = ldf

def write_approval_to_sheet(ws_title, sheet_row, col_map, headers, new_vals, record, auditor, ts_now, log_prefix, eval_val="", feedback_val="") -> bool:
    ws = get_spreadsheet().worksheet(ws_title)
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            np_ = len(headers) + 1
            if np_ > ws.col_count: _gsheets_call(ws.add_cols, max(4, np_-ws.col_count+1))
            _gsheets_call(ws.update_cell, 1, np_, sc)
            headers.append(sc); col_map[sc] = np_
    
    if COL_STATUS in col_map and _gsheets_call(ws.acell, rowcol_to_a1(sheet_row, col_map[COL_STATUS])).value == VAL_DONE:
        return False

    old = str(record.get(COL_LOG, "")).strip()
    batch = [{"range": rowcol_to_a1(sheet_row, col_map[f]), "values": [[v]]} for f, v in new_vals.items() if f in col_map and clean_cell(record.get(f, "")) != v]
    for cn, v in [(COL_STATUS, VAL_DONE), (COL_AUDITOR, auditor), (COL_DATE, ts_now), (COL_LOG, f"{log_prefix}\n{old}".strip()), (COL_EVAL, eval_val), (COL_FEEDBACK, feedback_val)]:
        if cn in col_map: batch.append({"range": rowcol_to_a1(sheet_row, col_map[cn]), "values": [[v]]})
    if batch: _gsheets_call(ws.batch_update, batch)
    return True

def write_reopen_to_sheet(ws_title, sheet_row, col_map):
    if COL_STATUS in col_map: _gsheets_call(get_spreadsheet().worksheet(ws_title).update_cell, sheet_row, col_map[COL_STATUS], VAL_PENDING)

def authenticate(email: str, password: str, spreadsheet_id: str):
    email = email.lower().strip()
    if email == "admin" and password == st.secrets.get("admin_password", ""): return "admin"
    try:
        df_u = pd.DataFrame(_fetch_users_cached(spreadsheet_id))
        if not df_u.empty and "email" in df_u.columns:
            row = df_u[df_u["email"] == email]
            if not row.empty and hash_pw(password) == str(row["password"].values[0]):
                return str(row["role"].values[0]).strip().lower() if "role" in df_u.columns else "auditor"
    except: pass
    return None

# ─────────────────────────────────────────────────────────────────────────────
#  6 · UI COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────
def _eval_chip(raw: str):
    n = _normalise_eval(raw)
    if "Good" in n: return f"<span class='s-chip s-eval-good'>{_html.escape(raw)}</span>"
    if "Bad" in n or "Incorrect" in n: return f"<span class='s-chip s-eval-bad'>{_html.escape(raw)}</span>"
    if "Duplicate" in n: return f"<span class='s-chip s-eval-dup'>{_html.escape(raw)}</span>"
    return f"<span class='s-chip s-pending'>{_html.escape(raw)}</span>"

def render_html_table(df: pd.DataFrame, max_rows: int = 500):
    if df.empty: return st.info("No records to display.")
    display_df = df.head(max_rows)
    th = "<th class='row-idx'>#</th>" + "".join(f"<th class='{'col-eval' if c==COL_EVAL else 'col-feedback' if c==COL_FEEDBACK else ''}'>{_html.escape(c)}</th>" for c in display_df.columns if c != COL_LOG)
    rows = ""
    for idx, row in display_df.iterrows():
        r = f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col == COL_LOG: continue
            raw = str(row[col]) if row[col] != "" else ""
            safe = _html.escape(raw)
            if col == COL_STATUS: r += f"<td><span class='s-chip s-{'done' if raw == VAL_DONE else 'pending'}'>{raw or '-'}</span></td>"
            elif col == COL_EVAL: r += f"<td class='col-eval'>{_eval_chip(raw)}</td>"
            elif col == COL_FEEDBACK: r += f"<td class='col-feedback'>{safe[:160] + '...' if len(safe) > 160 else (safe or '-')}</td>"
            else: r += f"<td>{f'<span title={safe}>{safe[:52]}...</span>' if len(raw) > 55 else (safe or '-')}</td>"
        rows += f"<tr>{r}</tr>"
    st.markdown(f"<div class='gov-table-wrap'><table class='gov-table'><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>", unsafe_allow_html=True)

def render_paginated_table(df: pd.DataFrame, page_key: str, max_rows: int = 5000):
    if df.empty: return render_html_table(df)
    total_pages = max(1, -(-min(len(df), max_rows) // 15))
    cur = st.session_state.setdefault(page_key, 1) = max(1, min(st.session_state.get(page_key, 1), total_pages))
    start = (cur - 1) * 15
    render_html_table(df.iloc[start:min(start + 15, len(df))], max_rows=15)
    if total_pages > 1:
        c1, c2, c3 = st.columns([1, 3, 1])
        if c1.button("← Prev", key=f"{page_key}_prev", disabled=(cur <= 1), use_container_width=True): st.session_state[page_key] -= 1; st.rerun()
        c2.markdown(f"<div style='text-align:center;padding:8px 0;font-size:.72rem;font-weight:600;color:var(--text-secondary);font-family:var(--mono);'>Page {cur} / {total_pages} <span style='font-weight:400;margin-left:10px;color:var(--text-muted);'>({start+1}–{min(start + 15, len(df))} of {len(df)})</span></div>", unsafe_allow_html=True)
        if c3.button("Next →", key=f"{page_key}_next", disabled=(cur >= total_pages), use_container_width=True): st.session_state[page_key] += 1; st.rerun()

def render_login(spreadsheet_id: str):
    st.markdown("""<style>[data-testid="stSidebar"],header{display:none !important;}html,body,.stApp,.block-container{background:#0D1117 !important;display:flex;justify-content:center;align-items:center;min-height:100vh;}.stApp::before{content:'';position:fixed;inset:0;background:radial-gradient(ellipse 80% 60% at 20% 20%, rgba(124,58,237,0.14) 0%, transparent 65%),radial-gradient(ellipse 60% 50% at 80% 75%, rgba(56,139,253,0.11) 0%, transparent 60%);}[data-testid="stForm"]{background:rgba(22,27,34,0.88)!important;backdrop-filter:blur(24px)!important;border:1px solid rgba(240,246,252,0.1)!important;border-top:2px solid #7C3AED!important;border-radius:18px!important;padding:40px 36px 32px!important;box-shadow:0 24px 60px rgba(0,0,0,0.7)!important;max-width:440px!important;margin:0 auto!important;}[data-testid="stFormSubmitButton"] button{background:linear-gradient(135deg,#7C3AED,#388BFD)!important;color:#FFF!important;border-radius:10px!important;padding:12px!important;width:100%!important;font-weight:700!important;}</style>""", unsafe_allow_html=True)
    with st.form("login_form", clear_on_submit=False):
        st.markdown(f"<div style='text-align:center;margin-bottom:22px;'><div style='width:60px;height:60px;margin:0 auto 16px;border-radius:14px;background:linear-gradient(135deg,#7C3AED,#388BFD);display:flex;align-items:center;justify-content:center;font-size:1.8rem;'>🏢</div><div style='font-size:1.4rem;font-weight:800;color:#E6EDF3;'>{t('portal_title')}</div><div style='font-size:.84rem;color:#8B949E;margin-top:8px;'>{t('login_prompt')}</div></div>", unsafe_allow_html=True)
        st.text_input(t("email_field"), key="_login_email")
        st.text_input(t("password_field"), type="password", key="_login_pw")
        if st.form_submit_button(t("sign_in")):
            role = authenticate(st.session_state._login_email, st.session_state._login_pw, spreadsheet_id)
            if role:
                st.session_state.update(logged_in=True, user_email="Admin" if role=="admin" else st.session_state._login_email.lower().strip(), user_role=role)
                st.rerun()
            else: st.error(t("bad_creds"))

def render_sidebar(headers, col_binder, col_company, col_license, is_admin, fetched_at):
    role, role_label = st.session_state.user_role, {"admin":"System Admin","manager":"Manager"}.get(st.session_state.user_role, "Tax Auditor")
    with st.sidebar:
        st.markdown(f"<div style='padding:18px 16px;border-top:2px solid var(--accent);'><div style='font-size:.95rem;font-weight:800;'>{t('portal_title')}</div><div style='font-size:.58rem;color:var(--text-secondary);'>{t('ministry')}</div></div><hr style='margin:0;border:none;border-top:1px solid var(--border-default);'/>", unsafe_allow_html=True)
        if role in ("admin", "manager"):
            time_passed = time.time() - st.session_state.last_refresh_time
            if role == "admin" or time_passed >= 600:
                if st.button("↺ Refresh Data", use_container_width=True):
                    _fetch_raw_sheet_cached.clear(); _fetch_users_cached.clear(); _fetch_sheet_metadata.clear(); st.session_state.update(local_cache_key=None, last_refresh_time=time.time()); st.toast("Refreshed", icon="🔄")
            else: st.button(f"⏳ Wait {int((600-time_passed)/60)+1} min", disabled=True, use_container_width=True)
        
        st.markdown(f"<div style='padding:9px 16px;background:var(--bg-subtle);'><span style='color:#3FB950;font-size:.58rem;font-weight:800;'>⚡ Optimistic UI</span><br><span style='font-size:.6rem;color:var(--text-secondary);'>Sync: {fetched_at[-8:] if fetched_at else '-'}</span></div><hr style='margin:0;border:none;border-top:1px solid var(--border-default);'/>", unsafe_allow_html=True)
        st.selectbox("Status", ["all", "pending", "done"], key="f_status")
        for k, l, d in [("f_email","Auditor Email",False), ("f_binder","Binder",col_binder is None), ("f_company","Company",col_company is None)]: st.text_input(l, key=k, disabled=d)
        if st.button("✕ Clear Filters", use_container_width=True): st.session_state.update(f_email="", f_binder="", f_company="", f_license="", f_status="all"); st.rerun()
        st.markdown(f"<hr style='margin:12px 0;border:none;border-top:1px solid var(--border-default);'/><div style='padding:13px 15px;background:var(--bg-subtle);border-radius:10px;'><div style='font-size:.6rem;color:var(--text-secondary);'>{t('signed_as')}</div><div style='font-size:.84rem;font-weight:700;'>{st.session_state.user_email}</div><span style='color:#388BFD;font-size:.6rem;font-weight:800;'>{role_label}</span></div>", unsafe_allow_html=True)
        if st.button("→ Sign Out", use_container_width=True): st.session_state.clear(); st.rerun()
    return st.session_state.get("f_email",""), st.session_state.get("f_binder",""), st.session_state.get("f_company",""), st.session_state.get("f_license",""), st.session_state.get("f_status","all")

def render_deep_search_strip(key_prefix, col_binder, col_company, col_agent_email):
    st.markdown("<div style='background:var(--bg-subtle);border:1px solid var(--border-default);border-left:3px solid var(--accent);border-radius:10px;padding:12px 18px 16px;margin-bottom:18px;'><div style='font-size:0.6rem;font-weight:800;color:var(--blue);margin-bottom:10px;'>DEEP SEARCH</div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns([1,1,1,0.32])
    c1.text_input("Binder No.", key=f"{key_prefix}_b", disabled=(col_binder is None))
    c2.text_input("Company", key=f"{key_prefix}_c", disabled=(col_company is None))
    c3.text_input("Agent Email", key=f"{key_prefix}_a", disabled=(col_agent_email is None))
    if c4.button("Clear", key=f"{key_prefix}_clr", use_container_width=True): st.session_state.update({f"{key_prefix}_b":"", f"{key_prefix}_c":"", f"{key_prefix}_a":""}); st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    return st.session_state.get(f"{key_prefix}_b",""), st.session_state.get(f"{key_prefix}_c",""), st.session_state.get(f"{key_prefix}_a","")

# ─────────────────────────────────────────────────────────────────────────────
#  7 · TAB COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────
def render_worklist(pending_display, df, headers, col_map, ws_title):
    st.markdown(f"<div class='worklist-header'><div><div class='worklist-title'>Audit Worklist</div><div class='worklist-sub'>Pending review</div></div><span class='chip chip-pending'>{len(pending_display)} Pending</span></div>", unsafe_allow_html=True)
    if pending_display.empty: return st.info("No cases.")
    render_paginated_table(pending_display, "page_worklist")
    st.markdown("<div class='section-title'>Select Case</div>", unsafe_allow_html=True)
    opts = ["-"] + [f"Row {idx}{_ROW_SEP}{str(row.get(headers[0] if headers else 'Row',''))[:55]}" for idx, row in pending_display.iterrows()]
    row_sel = st.selectbox("", opts, label_visibility="collapsed")
    if row_sel != "-":
        sheet_row = int(row_sel.split(_ROW_SEP)[0].replace("Row","").strip())
        record = df.iloc[sheet_row - 2].to_dict()
        with st.form("audit_form"):
            new_vals = {k: st.text_input(k, value=clean_cell(v)) for k, v in record.items() if k not in SYSTEM_COLS}
            st.markdown("<hr style='border-top:1px solid var(--border-muted);'/>", unsafe_allow_html=True)
            eval_val = st.selectbox("Quality", EVAL_OPTIONS)
            notes = st.text_area("Notes", height=100)
            if st.form_submit_button("Approve & Commit", use_container_width=True):
                ts, auditor = now_str(), st.session_state.user_email
                log, fb = f"[✓] {auditor} | {ts}", f"{notes.strip()}\n{build_auto_diff(record, new_vals)}".strip()
                with st.spinner("Saving..."):
                    if write_approval_to_sheet(ws_title, sheet_row, col_map, headers, new_vals, record, auditor, ts, log, eval_val, fb):
                        _apply_optimistic_approve(sheet_row - 2, new_vals, auditor, ts, log, eval_val, fb); st.toast("Saved!", icon="✅"); time.sleep(0.6); st.rerun()
                    else: st.toast("⚠️ Collision detected."); st.session_state.local_df.at[sheet_row-2, COL_STATUS] = VAL_DONE; time.sleep(1.5); st.rerun()

def render_archive(done_view, df, col_map, ws_title, is_admin, col_binder, col_company):
    st.markdown(f"<div class='worklist-header'><div><div class='worklist-title'>Archive</div></div><span class='chip chip-done'>{len(done_view)} Processed</span></div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,1,0.3]); s_b = c1.text_input("Search Binder", key="a_b"); s_c = c2.text_input("Search Company", key="a_c")
    if c3.button("✕", key="a_clr", use_container_width=True): st.session_state.update(a_b="", a_c=""); st.rerun()
    fv = done_view.copy()
    if s_b and col_binder in fv.columns: fv = fv[fv[col_binder].astype(str).str.contains(s_b, case=False, na=False)]
    if s_c and col_company in fv.columns: fv = fv[fv[col_company].astype(str).str.contains(s_c, case=False, na=False)]
    render_paginated_table(fv[[COL_STATUS,COL_EVAL,COL_FEEDBACK,COL_AUDITOR,COL_DATE] + [c for c in fv.columns if c not in SYSTEM_COLS]], "page_archive")
    if is_admin and not done_view.empty:
        rsel = st.selectbox("Re-open Record:", ["-"] + [f"Row {i}" for i in done_view.index])
        if rsel != "-" and st.button("Re-open"): write_reopen_to_sheet(ws_title, int(rsel.split(" ")[1]), col_map); st.session_state.local_df.at[int(rsel.split(" ")[1])-2, COL_STATUS] = VAL_PENDING; st.rerun()

def render_user_admin(spreadsheet_id):
    staff = pd.DataFrame(_fetch_users_cached(spreadsheet_id))
    if not staff.empty and "role" not in staff.columns: _gsheets_call(get_spreadsheet().worksheet(USERS_SHEET).update_cell, 1, len(staff.columns)+1, "role"); _fetch_users_cached.clear(); st.rerun()
    c1, c2 = st.columns(2, gap="large")
    with c1:
        with st.form("add"):
            e, p, r = st.text_input("Email"), st.text_input("Password", type="password"), st.selectbox("Role", VALID_ROLES)
            if st.form_submit_button("Register User") and e and p:
                _gsheets_call(get_spreadsheet().worksheet(USERS_SHEET).append_row, [e.lower(), hash_pw(p), r, now_str()]); _fetch_users_cached.clear(); st.success("Added."); st.rerun()
    with c2:
        if not staff.empty:
            st.markdown("<div class='gov-table-wrap'><table class='gov-table'><thead><tr><th>Email</th><th>Role</th></tr></thead><tbody>" + "".join(f"<tr><td>{row['email']}</td><td><span class='role-badge-{row.get('role','auditor')}'>{str(row.get('role','auditor')).title()}</span></td></tr>" for _, row in staff.iterrows()) + "</tbody></table></div>", unsafe_allow_html=True)
            de = st.selectbox("Revoke Access", ["-"] + staff["email"].tolist())
            if de != "-" and st.button(f"Revoke {de}"):
                c = _gsheets_call(get_spreadsheet().worksheet(USERS_SHEET).find, de)
                if c: _gsheets_call(get_spreadsheet().worksheet(USERS_SHEET).delete_rows, c.row); _fetch_users_cached.clear(); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
#  8 · MAIN CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        sid, titles = _fetch_sheet_metadata()
        if USERS_SHEET not in titles: _gsheets_call(get_spreadsheet().add_worksheet(title=USERS_SHEET, rows="100", cols="4").append_row, ["email","password","role","created_at"]); _fetch_sheet_metadata.clear(); titles.append(USERS_SHEET)
        if not st.session_state.logged_in: return render_login(sid)
        st.markdown("<style>[data-testid='stSidebar']{display:flex!important;}</style>", unsafe_allow_html=True)

        st.markdown(f"<div class='page-header'><div><div class='page-title'>{t('portal_title')}</div><div class='page-subtitle'>{t('ministry')}</div></div><div class='page-timestamp'>{datetime.now(TZ).strftime('%A, %d %B %Y · %H:%M')}</div></div>", unsafe_allow_html=True)
        avail = [t for t in titles if t in VISIBLE_SHEETS]
        if not avail: return st.warning("Sheets not found.")
        
        ws_title = st.selectbox("Workspace", avail, on_change=lambda: st.session_state.update(f_email="", f_binder="", f_company="", f_status="all", local_cache_key=None))
        df, hdrs, cmap, fat = get_local_data(sid, ws_title)
        
        cb, cc, cl, ce = detect_column(hdrs, "binder"), detect_column(hdrs, "company"), detect_column(hdrs, "license"), detect_column(hdrs, "agent_email")
        fe, fb, fc, fl, fs = render_sidebar(hdrs, cb, cc, cl, st.session_state.user_role == "admin", fat)

        if not df.empty:
            dn, tn = int((df[COL_STATUS]==VAL_DONE).sum()), len(df)
            c1, c2, c3 = st.columns(3); c1.metric("Total Cases", tn); c2.metric("Processed", dn, f"{int((dn/tn)*100 if tn else 0)}%"); c3.metric("Pending", tn-dn, f"{100-int((dn/tn)*100 if tn else 0)}% remaining", "inverse")
            st.markdown(f"<div class='prog-wrap'><div class='prog-fill' style='width:{int((dn/tn)*100 if tn else 0)}%;'></div></div>", unsafe_allow_html=True)
            fdf = apply_filters_locally(df, fe, fb, fc, fl, fs, cb, cc, cl)
            if _n_active(fe, fb, fc, fl, fs) > 0: st.markdown(f"<div style='background:var(--bg-subtle);border-left:3px solid var(--accent);padding:11px 16px;border-radius:10px;margin-bottom:16px;'><strong style='color:var(--blue);'>Filtered:</strong> {len(fdf)} / {tn} records</div>", unsafe_allow_html=True)
        else: fdf = pd.DataFrame()

        tabs = st.tabs(["Worklist", "Archive", "Analytics", "Users"] if st.session_state.user_role == "admin" else ["Worklist", "Archive", "Analytics"] if st.session_state.user_role == "manager" else ["Worklist", "Archive"])
        
        with tabs[0]: 
            if not df.empty: p = fdf[fdf[COL_STATUS]!=VAL_DONE].copy(); p.index += 2; render_worklist(p, df, hdrs, cmap, ws_title)
        with tabs[1]: 
            if not df.empty: d = fdf[fdf[COL_STATUS]==VAL_DONE].copy(); d.index += 2; render_archive(d, df, cmap, ws_title, st.session_state.user_role=="admin", cb, cc)
        if len(tabs) > 2:
            with tabs[2]:
                if not df.empty: 
                    st.markdown("<div class='section-title'>Auditor Productivity</div>", unsafe_allow_html=True)
                    st.bar_chart(df[df[COL_STATUS]==VAL_DONE][COL_AUDITOR].value_counts())
        if len(tabs) > 3:
            with tabs[3]: render_user_admin(sid)

    except Exception as e: st.error(f"System Error: {e}")

if __name__ == "__main__": main()
