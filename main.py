# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  -  v14.9
#  Architecture: Optimistic UI / Local-First Mutation
#  Changes from v14.8:
#    [1] Added dark_mode to _DEFAULTS; inject_dark_mode() CSS-variables only
#    [2] Dark Mode toggle shown to admins only in sidebar
#    [3] Language buttons restricted to admin/manager only
#    [4] Auditors see ONLY the Worklist tab (t_arch = None for auditors)
#    All cookie auth, sheets backend, pagination, optimistic UI unchanged.
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
import extra_streamlit_components as stx

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
import gspread.exceptions

# -----------------------------------------------------------------------------
#  0 . LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.WARNING)
_log = logging.getLogger("audit_portal")

# -----------------------------------------------------------------------------
#  1 . PAGE CONFIG
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Tax Audit & Compliance Portal",
    layout="wide",
    initial_sidebar_state="expanded",
)

TZ = pytz.timezone("Asia/Baghdad")

# -----------------------------------------------------------------------------
#  2 . SESSION STATE DEFAULTS
# -----------------------------------------------------------------------------
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
    dark_mode        = False,   # [1] NEW
)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# -----------------------------------------------------------------------------
#  3 . CONSTANTS
# -----------------------------------------------------------------------------
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

EVAL_OPTIONS = [
    "Good (باش)",
    "Bad / Incorrect (خراپ)",
    "Duplicate (دووبارە)",
]

VALID_ROLES = ["auditor", "manager", "admin"]

READ_TTL    = 600
BACKOFF_MAX = 5
_ROW_SEP    = " \u007c "

_PAGE_SIZE  = 10

_COOKIE_NAME = "portal_auth"

# -----------------------------------------------------------------------------
#  4 . EXPONENTIAL BACKOFF  (unchanged)
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
#  5 . CSS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def inject_css() -> None:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
:root {
  --bg:            #F7F8FC;
  --surface:       #FFFFFF;
  --surface-2:     #F0F2F9;
  --border:        #E4E7F0;
  --border-2:      #D0D5E8;
  --text-primary:    #0D1117;
  --text-secondary:  #4B5563;
  --text-muted:      #9CA3AF;
  --indigo-50:  #EEF2FF;
  --indigo-100: #E0E7FF;
  --indigo-400: #818CF8;
  --indigo-500: #6366F1;
  --indigo-600: #4F46E5;
  --indigo-700: #4338CA;
  --blue-400:   #60A5FA;
  --blue-500:   #3B82F6;
  --green-50:   #F0FDF4;
  --green-200:  #A7F3D0;
  --green-600:  #16A34A;
  --green-700:  #15803D;
  --amber-50:   #FFFBEB;
  --amber-200:  #FDE68A;
  --amber-700:  #B45309;
  --red-50:     #FFF1F2;
  --red-200:    #FECDD3;
  --red-600:    #DC2626;
  --radius-sm:   6px;
  --radius-md:   10px;
  --radius-lg:   16px;
  --radius-xl:   24px;
  --radius-full: 9999px;
  --shadow-sm: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
  --shadow-md: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
  --shadow-lg: 0 12px 32px rgba(0,0,0,0.10), 0 4px 8px rgba(0,0,0,0.06);
  --ring: 0 0 0 3px rgba(99,102,241,0.18);
  --font: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
  --mono: 'JetBrains Mono', 'Courier New', monospace;
}
*, *::before, *::after { box-sizing: border-box !important; font-family: var(--font) !important; }
html, body, .stApp, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main, .block-container {
  background-color: var(--bg) !important; color: var(--text-primary) !important;
}
p, span, div, li, label, h1, h2, h3, h4, h5, h6,
.stMarkdown, [data-testid="stMarkdownContainer"] { color: var(--text-primary) !important; }
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"], [data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"] { display: none !important; }
[data-testid="stSidebar"] {
  background-color: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
  box-shadow: 4px 0 24px rgba(0,0,0,0.04) !important;
}
[data-testid="stSidebar"] * { color: var(--text-primary) !important; }
.stTextInput > div > div > input, .stTextArea > div > div > textarea {
  background: var(--surface) !important; color: var(--text-primary) !important;
  border: 1.5px solid var(--border-2) !important; border-radius: var(--radius-md) !important;
  font-size: 0.875rem !important; font-weight: 500 !important; padding: 11px 14px !important;
  box-shadow: var(--shadow-sm) !important;
  transition: border-color 0.18s ease, box-shadow 0.18s ease !important;
}
.stTextInput > div > div > input:focus, .stTextArea > div > div > textarea:focus {
  border-color: var(--indigo-500) !important; box-shadow: var(--ring) !important; outline: none !important;
}
.stTextInput > div > div > input::placeholder, .stTextArea > div > div > textarea::placeholder {
  color: var(--text-muted) !important; font-weight: 400 !important;
}
.stTextInput > label, .stTextArea > label, .stSelectbox > label, .stMultiSelect > label {
  color: var(--text-secondary) !important; font-size: 0.72rem !important;
  font-weight: 700 !important; letter-spacing: 0.07em !important; text-transform: uppercase !important;
}
.stSelectbox > div > div, [data-baseweb="select"] > div {
  background: var(--surface) !important; color: var(--text-primary) !important;
  border-color: var(--border-2) !important; border-radius: var(--radius-md) !important;
  box-shadow: var(--shadow-sm) !important; font-weight: 500 !important;
}
[data-baseweb="menu"] li, [data-baseweb="menu"] [role="option"] {
  background: var(--surface) !important; color: var(--text-primary) !important; font-size: 0.875rem !important;
}
[data-baseweb="menu"] li:hover, [data-baseweb="menu"] [aria-selected="true"] {
  background: var(--indigo-50) !important; color: var(--indigo-600) !important;
}
[data-testid="stMetricContainer"] {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-top: 3px solid var(--indigo-500) !important; border-radius: var(--radius-lg) !important;
  padding: 22px 26px !important; box-shadow: var(--shadow-md) !important;
  transition: transform 0.22s cubic-bezier(0.34,1.56,0.64,1), box-shadow 0.22s ease !important;
}
[data-testid="stMetricContainer"]:hover {
  transform: translateY(-4px) !important;
  box-shadow: 0 16px 40px rgba(99,102,241,0.14), 0 4px 12px rgba(0,0,0,0.06) !important;
}
[data-testid="stMetricValue"] {
  font-size: 2.1rem !important; font-weight: 800 !important;
  color: var(--indigo-600) !important; letter-spacing: -0.03em !important; line-height: 1.1 !important;
}
[data-testid="stMetricLabel"] {
  font-size: 0.68rem !important; font-weight: 700 !important;
  letter-spacing: 0.10em !important; text-transform: uppercase !important; color: var(--text-muted) !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem !important; font-weight: 600 !important; }
.stButton > button {
  background: linear-gradient(135deg, var(--indigo-600) 0%, var(--blue-500) 100%) !important;
  color: #FFFFFF !important; border: none !important; border-radius: var(--radius-md) !important;
  font-weight: 700 !important; font-size: 0.84rem !important; padding: 10px 20px !important;
  letter-spacing: 0.01em !important; box-shadow: 0 2px 8px rgba(99,102,241,0.35) !important;
  transition: all 0.18s cubic-bezier(0.34,1.56,0.64,1) !important;
}
.stButton > button:hover {
  background: linear-gradient(135deg, var(--indigo-700) 0%, var(--indigo-600) 100%) !important;
  transform: translateY(-2px) scale(1.01) !important;
  box-shadow: 0 8px 24px rgba(99,102,241,0.45) !important;
}
.stButton > button:active { transform: translateY(0) scale(0.99) !important; box-shadow: 0 2px 8px rgba(99,102,241,0.25) !important; }
[data-testid="stDownloadButton"] > button {
  background: linear-gradient(135deg, #0D9488 0%, #0891B2 100%) !important;
  color: #FFFFFF !important; border: none !important; border-radius: var(--radius-md) !important;
  font-weight: 700 !important; font-size: 0.84rem !important; padding: 10px 20px !important;
  box-shadow: 0 2px 8px rgba(13,148,136,0.35) !important;
  transition: all 0.18s cubic-bezier(0.34,1.56,0.64,1) !important;
}
[data-testid="stDownloadButton"] > button:hover {
  background: linear-gradient(135deg, #0F766E 0%, #0E7490 100%) !important;
  transform: translateY(-2px) scale(1.01) !important;
  box-shadow: 0 8px 24px rgba(13,148,136,0.45) !important;
}
div[data-testid="stForm"] {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: var(--radius-lg) !important; padding: 28px 32px !important;
  box-shadow: var(--shadow-md) !important;
}
[data-testid="stFormSubmitButton"] > button {
  background: linear-gradient(135deg, var(--indigo-600) 0%, var(--blue-500) 100%) !important;
  color: #FFFFFF !important; border: none !important; border-radius: var(--radius-md) !important;
  font-weight: 700 !important; font-size: 0.84rem !important; padding: 10px 20px !important;
  letter-spacing: 0.01em !important; box-shadow: 0 2px 8px rgba(99,102,241,0.35) !important;
  transition: all 0.18s cubic-bezier(0.34,1.56,0.64,1) !important;
}
[data-testid="stFormSubmitButton"] > button:hover {
  background: linear-gradient(135deg, var(--indigo-700) 0%, var(--indigo-600) 100%) !important;
  transform: translateY(-2px) scale(1.01) !important;
  box-shadow: 0 8px 24px rgba(99,102,241,0.45) !important;
}
[data-testid="stFormSubmitButton"] > button:active {
  transform: translateY(0) scale(0.99) !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.25) !important;
}
.stTabs [data-baseweb="tab-list"] {
  gap: 2px !important; background: var(--surface-2) !important;
  border: 1px solid var(--border) !important; border-radius: var(--radius-full) !important;
  padding: 4px !important; width: fit-content !important; box-shadow: var(--shadow-sm) !important;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important; color: var(--text-muted) !important;
  border-radius: var(--radius-full) !important; border: none !important;
  padding: 8px 22px !important; font-weight: 600 !important; font-size: 0.82rem !important;
  transition: all 0.18s ease !important;
}
.stTabs [data-baseweb="tab"]:hover { color: var(--text-primary) !important; background: rgba(0,0,0,0.04) !important; }
.stTabs [aria-selected="true"] {
  background: var(--surface) !important; color: var(--indigo-600) !important;
  border: none !important; box-shadow: var(--shadow-sm) !important;
}
.streamlit-expanderHeader {
  background: var(--surface-2) !important; color: var(--text-primary) !important;
  border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important;
  font-weight: 600 !important; font-size: 0.85rem !important;
}
.streamlit-expanderContent {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-top: none !important; border-radius: 0 0 var(--radius-md) var(--radius-md) !important; padding: 18px !important;
}
[data-testid="stAlert"] {
  border-radius: var(--radius-md) !important; border: 1px solid var(--border) !important;
  background: var(--surface) !important; box-shadow: var(--shadow-sm) !important;
}
[data-testid="stAlert"] * { color: var(--text-primary) !important; }
.chip-manager { background:#FFF7ED;color:#C2410C!important;border:1px solid #FED7AA; }
.role-badge-admin   { background:#EDE9FE;color:#6D28D9!important;border:1px solid #DDD6FE;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }
.role-badge-manager { background:#FFF7ED;color:#C2410C!important;border:1px solid #FED7AA;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }
.role-badge-auditor { background:#F0FDF4;color:#15803D!important;border:1px solid #A7F3D0;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }
.deep-search-strip {
  background: var(--surface); border: 1px solid var(--border);
  border-left: 4px solid var(--indigo-500); border-radius: var(--radius-md);
  padding: 12px 20px 16px; margin-bottom: 20px; box-shadow: var(--shadow-sm);
}
.deep-search-title {
  font-size: .62rem; font-weight: 800; letter-spacing: .14em;
  text-transform: uppercase; color: var(--indigo-600) !important; margin-bottom: 10px;
}
.page-header { display:flex;align-items:center;justify-content:space-between;padding:4px 0 24px;border-bottom:1px solid var(--border);margin-bottom:28px; }
.page-title  { font-size:1.55rem;font-weight:800;color:var(--text-primary)!important;letter-spacing:-.03em;margin:0; }
.page-subtitle { font-size:.78rem;color:var(--text-muted)!important;margin-top:4px;font-weight:500; }
.page-timestamp { font-size:.74rem;color:var(--text-muted)!important;font-weight:600;background:var(--surface);padding:7px 16px;border-radius:var(--radius-full);border:1px solid var(--border);box-shadow:var(--shadow-sm);font-family:var(--mono)!important; }
.section-title { display:inline-flex;align-items:center;gap:8px;font-size:.70rem;font-weight:800;color:var(--indigo-600)!important;margin:24px 0 14px;padding:6px 14px 6px 10px;border-left:3px solid var(--indigo-500);border-radius:0 var(--radius-sm) var(--radius-sm) 0;background:var(--indigo-50);text-transform:uppercase;letter-spacing:.07em; }
.worklist-header { display:flex;align-items:center;justify-content:space-between;background:var(--surface);border:1px solid var(--border);border-top:3px solid var(--indigo-500);border-radius:var(--radius-lg);padding:18px 24px;margin-bottom:18px;box-shadow:var(--shadow-md); }
.worklist-title { font-size:1.02rem;font-weight:800;color:var(--text-primary)!important; }
.worklist-sub   { font-size:.76rem;color:var(--text-muted)!important;margin-top:3px; }
.log-summary-card { background:var(--surface);border:1px solid var(--border);border-top:3px solid var(--indigo-500);border-radius:var(--radius-lg);padding:20px 26px;box-shadow:var(--shadow-md);margin-bottom:18px; }
.log-stat-row { display:flex;align-items:center;gap:28px;flex-wrap:wrap; }
.log-stat { display:flex;flex-direction:column;gap:2px; }
.log-stat-value { font-size:1.55rem;font-weight:800;color:var(--indigo-600)!important;letter-spacing:-.03em;font-family:var(--mono)!important; }
.log-stat-label { font-size:.62rem;font-weight:700;letter-spacing:.10em;text-transform:uppercase;color:var(--text-muted)!important; }
.log-stat-divider { width:1px;height:40px;background:var(--border); }
.export-strip { background:linear-gradient(135deg,#F0FDF4 0%,#EFF6FF 100%);border:1px solid var(--green-200);border-radius:var(--radius-md);padding:14px 18px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:20px; }
.export-text { font-size:.80rem;font-weight:600;color:var(--text-secondary)!important; }
.export-sub  { font-size:.68rem;color:var(--text-muted)!important;margin-top:2px; }
.prog-wrap  { background:var(--border);border-radius:var(--radius-full);height:7px;overflow:hidden;margin:6px 0 12px; }
.prog-fill  { height:100%;border-radius:var(--radius-full);background:linear-gradient(90deg,var(--indigo-600),var(--blue-400));transition:width .8s cubic-bezier(.4,0,.2,1);box-shadow:0 0 12px rgba(99,102,241,0.40); }
.prog-labels{ display:flex;justify-content:space-between;font-size:.72rem;color:var(--text-muted)!important;font-weight:600;margin-bottom:4px; }
.chip { display:inline-flex;align-items:center;gap:5px;padding:4px 12px;border-radius:var(--radius-full);font-size:.68rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase; }
.chip-done    { background:var(--green-50); color:var(--green-700) !important; border:1px solid var(--green-200); }
.chip-pending { background:var(--amber-50); color:var(--amber-700) !important; border:1px solid var(--amber-200); }
.chip-admin   { background:var(--indigo-50);color:var(--indigo-600)!important;border:1px solid var(--indigo-100); }
.chip-audit   { background:var(--green-50); color:var(--green-600) !important; border:1px solid var(--green-200); }
.s-chip { display:inline-flex;align-items:center;padding:3px 10px;border-radius:var(--radius-full);font-size:.63rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase; }
.s-done    { background:var(--green-50); color:var(--green-700) !important; border:1px solid var(--green-200); }
.s-pending { background:var(--amber-50); color:var(--amber-700) !important; border:1px solid var(--amber-200); }
.s-eval-good { background:var(--green-50);color:var(--green-700)!important;border:1px solid var(--green-200); }
.s-eval-bad  { background:var(--red-50);color:var(--red-600)!important;border:1px solid var(--red-200); }
.s-eval-dup  { background:var(--amber-50);color:var(--amber-700)!important;border:1px solid var(--amber-200); }
.gov-table-wrap { overflow-x:auto;border:1px solid var(--border);border-radius:var(--radius-lg);margin-bottom:18px;box-shadow:var(--shadow-md); }
.gov-table { width:100%;border-collapse:collapse;background:var(--surface);font-size:.84rem; }
.gov-table thead tr { background:var(--surface-2);border-bottom:1px solid var(--border); }
.gov-table th { color:var(--text-muted)!important;background:var(--surface-2)!important;font-weight:700!important;font-size:.63rem!important;letter-spacing:.09em!important;text-transform:uppercase!important;padding:13px 18px!important;white-space:nowrap;text-align:left!important; }
.gov-table td { color:var(--text-primary)!important;background:var(--surface)!important;padding:11px 18px!important;font-size:.84rem!important;font-weight:500!important;border-bottom:1px solid var(--border)!important;vertical-align:middle!important;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;transition:background .14s ease!important; }
.gov-table tbody tr:nth-child(even) td { background:#FBFCFF!important; }
.gov-table tbody tr:hover td { background:var(--indigo-50)!important;color:var(--text-primary)!important; }
.gov-table tbody tr:last-child td { border-bottom:none!important; }
.gov-table td.row-idx,.gov-table th.row-idx { color:var(--text-muted)!important;font-family:var(--mono)!important;font-size:.70rem!important;min-width:50px;text-align:center!important; }
.gov-table th.col-eval, .gov-table th.col-feedback { background:var(--indigo-50)!important;color:var(--indigo-600)!important;border-bottom:2px solid var(--indigo-400)!important; }
.gov-table td.col-feedback { max-width:280px;white-space:normal!important;word-break:break-word;font-size:.75rem!important;font-family:var(--mono)!important;color:var(--text-secondary)!important; }
.lb-row { display:flex;align-items:center;gap:12px;padding:12px 18px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-md);margin-bottom:8px;box-shadow:var(--shadow-sm);transition:all .18s cubic-bezier(.34,1.56,.64,1); }
.lb-row:hover { transform:translateX(5px);border-color:var(--indigo-400);box-shadow:0 6px 20px rgba(99,102,241,0.12); }
.lb-medal { font-size:.80rem;font-weight:800;min-width:32px;text-align:center;color:var(--text-muted);font-family:var(--mono)!important;background:var(--surface-2);border-radius:var(--radius-sm);padding:2px 4px; }
.lb-name  { flex:1;font-size:.85rem;font-weight:600;color:var(--text-primary)!important; }
.lb-count { font-size:.88rem;font-weight:800;color:var(--indigo-600)!important;font-family:var(--mono)!important;background:var(--indigo-50);padding:3px 10px;border-radius:var(--radius-full);border:1px solid var(--indigo-100); }
.log-line { font-family:var(--mono)!important;font-size:.74rem;color:var(--text-secondary)!important;padding:6px 0;border-bottom:1px dashed var(--border);line-height:1.5; }
.log-line:last-child { border-bottom:none; }
.sidebar-header { border-top:3px solid var(--indigo-500);padding:20px 18px 16px; }
.sidebar-logo-text { font-size:.98rem;font-weight:800;color:var(--text-primary)!important;letter-spacing:-.02em;margin-bottom:3px; }
.sidebar-ministry { font-size:.60rem;color:var(--text-muted)!important;letter-spacing:.12em;text-transform:uppercase;font-weight:600; }
.sb-label { font-size:.62rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:var(--text-muted)!important;margin-bottom:5px; }
.sb-email { font-size:.85rem;font-weight:700;color:var(--text-primary)!important;word-break:break-all; }
.sb-user-card { background:linear-gradient(135deg,var(--indigo-50) 0%,#F5F0FF 100%);border:1px solid var(--indigo-100);border-radius:var(--radius-md);padding:14px 16px;margin-bottom:12px;box-shadow:var(--shadow-sm); }
.cache-badge { display:inline-flex;align-items:center;gap:5px;background:var(--green-50);color:var(--green-700)!important;border:1px solid var(--green-200);border-radius:var(--radius-full);font-size:.58rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;padding:3px 10px; }
.cache-info  { font-size:.62rem;color:var(--text-muted)!important;margin-top:6px;font-family:var(--mono)!important; }
.cache-strip { padding:10px 18px;background:var(--surface-2);border-bottom:1px solid var(--border); }
.adv-filter-header { font-size:.62rem;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:var(--indigo-600)!important;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border); }
.col-hint { font-size:.58rem;font-weight:400;opacity:.55;color:var(--text-muted)!important; }
.filter-result-bar { background:var(--surface);border:1px solid var(--border);border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);padding:12px 18px;margin-bottom:18px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;box-shadow:var(--shadow-sm); }
.filter-badge { display:inline-flex;align-items:center;gap:4px;background:var(--indigo-50);color:var(--indigo-600)!important;border:1px solid var(--indigo-100);border-radius:var(--radius-full);font-size:.64rem;font-weight:700;padding:3px 10px; }
.result-count { font-size:.76rem;color:var(--text-muted)!important;margin-left:auto;font-family:var(--mono)!important; }
.rbac-banner { background:var(--indigo-50);border:1px solid var(--indigo-100);border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);padding:12px 18px;margin-bottom:18px;font-size:.80rem;color:var(--indigo-600)!important;font-weight:600; }
.divider { border:none;border-top:1px solid var(--border);margin:14px 0; }
.acc-table { width:100%;border-collapse:collapse;font-size:.83rem; }
.acc-table th { background:var(--indigo-50)!important;color:var(--indigo-600)!important;font-size:.62rem!important;font-weight:800!important;letter-spacing:.09em!important;text-transform:uppercase!important;padding:11px 16px!important;border-bottom:2px solid var(--indigo-100)!important;text-align:left!important; }
.acc-table td { padding:10px 16px!important;border-bottom:1px solid var(--border)!important;vertical-align:middle!important;font-weight:500!important;color:var(--text-primary)!important;background:var(--surface)!important; }
.acc-table tbody tr:nth-child(even) td { background:#FBFCFF!important; }
.acc-table tbody tr:hover td { background:var(--indigo-50)!important; }
.acc-table tbody tr:last-child td { border-bottom:none!important; }
.acc-rate-high { color:var(--green-700)!important;font-weight:800!important;font-family:var(--mono)!important; }
.acc-rate-mid  { color:var(--amber-700)!important;font-weight:800!important;font-family:var(--mono)!important; }
.acc-rate-low  { color:var(--red-600)!important;font-weight:800!important;font-family:var(--mono)!important; }
.acc-bar-wrap  { background:var(--border);border-radius:var(--radius-full);height:6px;width:80px;display:inline-block;vertical-align:middle;margin-left:8px; }
.acc-bar-fill  { height:100%;border-radius:var(--radius-full); }
</style>""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
#  5b. DARK MODE OVERRIDE  [1]
#  Only overrides CSS custom properties — no layout, margin, padding, or
#  font-size values are touched. Tables, dropdowns, and submit button retain
#  their exact sizes and gradient styles; colours adapt to dark backgrounds.
# -----------------------------------------------------------------------------
def inject_dark_mode() -> None:
    st.markdown("""
<style>
:root {
  /* ── Backgrounds ── */
  --bg:       #0D1117;
  --surface:  #161B22;
  --surface-2:#21262D;

  /* ── Borders ── */
  --border:   rgba(240,246,252,0.10);
  --border-2: rgba(240,246,252,0.16);

  /* ── Text — high-contrast for all table cells, labels, metrics ── */
  --text-primary:   #E6EDF3;
  --text-secondary: #8B949E;
  --text-muted:     #484F58;

  /* ── Indigo accent shades — kept visible on dark backgrounds ── */
  --indigo-50:  rgba(99,102,241,0.14);
  --indigo-100: rgba(99,102,241,0.22);
  --indigo-400: #818CF8;
  --indigo-500: #6366F1;
  --indigo-600: #818CF8;
  --indigo-700: #6366F1;

  /* ── Blue accent ── */
  --blue-400: #93C5FD;
  --blue-500: #60A5FA;

  /* ── Semantic ── */
  --green-50:  rgba(63,185,80,0.14);
  --green-200: rgba(63,185,80,0.35);
  --green-600: #3FB950;
  --green-700: #3FB950;
  --amber-50:  rgba(210,153,34,0.14);
  --amber-200: rgba(210,153,34,0.35);
  --amber-700: #D29922;
  --red-50:    rgba(248,81,73,0.14);
  --red-200:   rgba(248,81,73,0.35);
  --red-600:   #F85149;

  /* ── Shadows — deeper on dark ── */
  --shadow-sm: 0 1px 4px rgba(0,0,0,0.40);
  --shadow-md: 0 4px 16px rgba(0,0,0,0.55);
  --shadow-lg: 0 12px 40px rgba(0,0,0,0.65);
  --ring: 0 0 0 3px rgba(129,140,248,0.25);
}

/* ── Make sure table even-rows use the dark surface ── */
.gov-table tbody tr:nth-child(even) td { background: #1C2128 !important; }
.gov-table tbody tr:hover td { background: rgba(99,102,241,0.12) !important; }
.acc-table tbody tr:nth-child(even) td { background: #1C2128 !important; }
.acc-table tbody tr:hover td { background: rgba(99,102,241,0.12) !important; }

/* ── Selectbox dropdown menu background ── */
[data-baseweb="menu"], [data-baseweb="popover"] > div {
  background: #21262D !important;
  border: 1px solid rgba(240,246,252,0.16) !important;
}

/* ── Submit button: keep the same gradient, just use the dark-adjusted indigo ── */
[data-testid="stFormSubmitButton"] > button {
  background: linear-gradient(135deg, #6366F1 0%, #3B82F6 100%) !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.50) !important;
}
[data-testid="stFormSubmitButton"] > button:hover {
  background: linear-gradient(135deg, #4F46E5 0%, #6366F1 100%) !important;
  box-shadow: 0 8px 24px rgba(99,102,241,0.60) !important;
}

/* ── Regular buttons: same treatment ── */
.stButton > button {
  background: linear-gradient(135deg, #6366F1 0%, #3B82F6 100%) !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.50) !important;
}
.stButton > button:hover {
  background: linear-gradient(135deg, #4F46E5 0%, #6366F1 100%) !important;
  box-shadow: 0 8px 24px rgba(99,102,241,0.60) !important;
}

/* ── Login page form card ── */
[data-testid="stForm"] {
  background: rgba(22,27,34,0.96) !important;
  border-color: rgba(240,246,252,0.12) !important;
}

/* ── Expander ── */
.streamlit-expanderContent {
  background: #161B22 !important;
}

/* ── Progress bar track ── */
.prog-wrap { background: #21262D; }

/* ── Export strip ── */
.export-strip {
  background: linear-gradient(135deg, rgba(22,27,34,0.90) 0%, rgba(28,33,40,0.90) 100%) !important;
  border-color: rgba(63,185,80,0.35) !important;
}
</style>""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
#  6 . TRANSLATIONS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
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
        "language":"Interface Language",
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
        "adv_filters":"Advanced Filters",
        "f_binder":"Company Binder No.","f_license":"License Number",
        "clear_filters":"Clear Filters",
        "active_filters":"Active filters","results_shown":"results shown",
        "no_match":"No records match the applied filters.",
        "retry_warning":"Google Sheets quota reached - retrying with backoff...",
        "local_mode":"Optimistic UI Active","cache_age":"Cache TTL",
        "rbac_notice":"Info: Your role only has access to the Audit Worklist.",
        "logs_title":"Auditor Activity Logs",
        "logs_sub":"Full processing history from project start",
        "logs_filter_all":"All Auditors","logs_auditor_sel":"Filter by Auditor",
        "logs_total":"Total Processed","logs_auditors":"Unique Auditors",
        "logs_date_range":"Date Range","logs_no_data":"No processed records found.",
        "logs_export_hdr":"Export Full Report",
        "logs_export_sub":"Download the complete audit log as a CSV file.",
        "logs_export_btn":"Download CSV Report",
        "logs_filename":"audit_log_report.csv","logs_cols_shown":"Columns displayed",
        "eval_label":"Data Entry Quality (کوالێتی داتا)",
        "feedback_label":"Auditor Feedback / Notes for Agent (تێبینی)",
        "feedback_placeholder":"Optional notes, issues found, corrections made...",
        "acc_ranking_title":"Data Entry Accuracy Ranking",
        "acc_agent":"Agent Email","acc_total":"Total",
        "acc_good":"Good","acc_bad":"Bad","acc_dup":"Dup","acc_rate":"Accuracy %",
        "acc_no_data":"No evaluation data available yet.",
        "archive_quality_note":"Tip: Columns Data_Evaluation & Correction_Notes are highlighted.",
        "role_label":"Role","change_role":"Change User Role",
        "change_role_sub":"Upgrade or downgrade any user's access level",
        "role_updated":"Role updated successfully.",
        "deep_search":"Deep Search","ds_binder":"Binder No.","ds_agent":"Agent Email",
        "ds_clear":"Clear","ds_showing":"Showing results for",
        "eval_breakdown":"Evaluation Breakdown per Agent",
        "eval_breakdown_sub":"Stacked view: Good / Bad / Duplicate per data-entry agent",
        "arch_search_title":"Archive Quick Search",
        "dark_mode_label":"Dark Mode",
    },
    "ku": {
        "ministry":"وەزارەتی دارایی و گومرگ",
        "portal_title":"پۆرتەلی فەرمی وردبینی باج و پابەندبوون",
        "portal_sub":"تەنها دەستپێگەیشتنی مەرجدارکراو",
        "classified":"نهێنی - تەنها بەکارهێنانی حکومی",
        "login_prompt":"زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email_field":"ئیمەیڵی فەرمی / ناساندن","password_field":"پاسۆرد",
        "sign_in":"چوونەژوورەوە","sign_out":"چوونەدەرەوە",
        "bad_creds":"ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "language":"زمانی ڕووکار",
        "workspace":"تۆماری کەیسە چالاکەکان","overview":"کورتەی کەیسەکان",
        "total":"کۆی کەیسەکان","processed":"کارکراوە","outstanding":"ماوە",
        "worklist_title":"لیستی کاری وردبینی","worklist_sub":"کیسە چالاکەکانی چاوەڕوان",
        "tab_worklist":"لیستی کاری","tab_archive":"ئەرشیف",
        "tab_analytics":"ئەنالیتیکس","tab_logs":"لۆگی ئۆدیتۆر","tab_users":"بەکارهێنەر",
        "select_case":"کیسێک هەڵبژێرە بۆ پشکنین","audit_trail":"مێژووی گۆڕانکاری",
        "approve_save":"پەسەندکردن و پاشەکەوتکردن","reopen":"( دووبارە کردنەوەی کەیسەکان ( گەراندنەوەی کەیسەکان",
        "leaderboard":"تەختەی بەرهەمهێنانی ئۆدیتۆر","daily_trend":"ترەندی ڕۆژانە",
        "period":"ماوەی کات","today":"ئەمڕۆ","this_week":"ئەم هەفتەیە",
        "this_month":"ئەم مانگەیە","all_time":"هەموو کات",
        "add_auditor":"تۆمارکردنی بەکارهێنەری نوێ","update_pw":"نوێکردنەوەی پاسۆرد",
        "remove_user":"هەڵوەشاندنەوەی دەستپێگەیشتن","staff_dir":"کارمەندە مەرجداركراوەکان",
        "no_records":"هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet":"ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok":"کیسەکە پەسەندکرا. دیمەن نوێکرایەوە.",
        "dup_email":"ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields":"هەموو خانەکان پەیوەندییانە.",
        "signed_as":"چووییتە ژوورەوە بەناوی","role_admin":"بەڕێوەبەری سیستەم",
        "role_auditor":"ئۆدیتۆری باج","role_manager":"بەڕێوەبەر",
        "processing":"پشکنینی کیسی","no_history":"هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period":"تۆمارەکان (ماوە)","active_days":"ڕۆژی چالاک","avg_per_day":"تێکڕای ڕۆژانە",
        "adv_filters":"فلتەرە پێشکەوتووەکان",
        "f_binder":"ژمارەی بایندەری کۆمپانیا","f_license":"ژمارەی مۆڵەتی",
        "clear_filters":"سڕینەوەی فلتەرەکان",
        "active_filters":"فلتەرە چالاکەکان","results_shown":"ئەنجامی پیشاندراو",
        "no_match":"هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "retry_warning":"کووتای گووگڵ شیت گەیشت - دووبارە هەوڵدەدرێت...",
        "local_mode":"Optimistic UI چالاکە","cache_age":"Cache TTL",
        "rbac_notice":"ڕۆڵەکەت تەنها دەستپێگەیشتن بە لیستی کاری هەیە.",
        "logs_title":"لۆگی چالاکی ئۆدیتۆرەکان","logs_sub":"مێژووی تەواوی پرۆسەکردن",
        "logs_filter_all":"هەموو ئۆدیتۆرەکان","logs_auditor_sel":"فلتەر بە ئۆدیتۆر",
        "logs_total":"کۆی گشتی کارکراو","logs_auditors":"ژمارەی ئۆدیتۆرەکان",
        "logs_date_range":"ماوەی بەروار","logs_no_data":"هیچ تۆماری کارکراوی نییە.",
        "logs_export_hdr":"هەناردەکردنی ڕاپۆرتی تەواو",
        "logs_export_sub":"ئەم لۆگە وەک فایلی CSV داگرە.",
        "logs_export_btn":"داگرتنی ڕاپۆرتی CSV",
        "logs_filename":"audit_log_report.csv","logs_cols_shown":"ستوونەکانی پیشاندراو",
        "eval_label":"کوالێتی داتا (Data Entry Quality)",
        "feedback_label":"تێبینی ئۆدیتۆر / تێبینی بۆ ئەجنت",
        "feedback_placeholder":"تێبینی ئارەزوومەندانە، کێشەکان، سەرەستکردنەکان...",
        "acc_ranking_title":"رێزبەندی شیازی داخلکردنی داتا",
        "acc_agent":"ئیمەیڵی ئەجنت","acc_total":"کۆی گشتی",
        "acc_good":"باش","acc_bad":"خراپ","acc_dup":"دووبارە","acc_rate":"ڕێژەی شیازی %",
        "acc_no_data":"هیچ داتای هەڵسەنگاندنی بەردەست نییە.",
        "archive_quality_note":"تێبینی: ستوونەکانی Data_Evaluation و Correction_Notes نیشاندراون.",
        "role_label":"ڕۆڵ","change_role":"گۆڕینی ڕۆڵی بەکارهێنەر",
        "change_role_sub":"بەرزکردنەوە یان دابەزاندنی ئاستی دەستپێگەیشتن",
        "role_updated":"ڕۆڵەکە بە سەرکەوتوویی نوێکرایەوە.",
        "deep_search":"گەڕانی قووڵ","ds_binder":"ژمارەی بایندەر","ds_agent":"ئیمەیڵی ئەجنت",
        "ds_clear":"سڕینەوە","ds_showing":"پیشاندانی ئەنجام بۆ",
        "eval_breakdown":"داڕشتنی هەڵسەنگاندن بەپێی ئەجنت",
        "eval_breakdown_sub":"دیمەنی خورەکی باش / خراپ / دووبارە بەپێی ئەجنتی داخلکردنی داتا",
        "arch_search_title":"گەڕانی خێرای ئەرشیف",
        "dark_mode_label":"دیمەنی تاریک",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)


# -----------------------------------------------------------------------------
#  7 . HELPERS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
_COL_KEYWORDS: dict[str, list[str]] = {
    "binder":  ["رقم ملف الشركة","رقم_ملف_الشركة","رقم ملف","ملف الشركة",
                "ژمارەی بایندەری کۆمپانیا","ژمارەی بایندەری","بایندەری",
                "binder","file no","file_no"],
    "company": ["ناوی کۆمپانیا","اسم الشركة","اسم_الشركة","اسم الشركه",
                "کۆمپانیای","کۆمپانیا","كومبانيا","شركة",
                "company name","company_name","company"],
    "license": ["رقم الترخيص","رقم_الترخيص","الترخيص",
                "ژمارەی مۆڵەتی کۆمپانیا","ژمارەی مۆڵەتی","مۆڵەتی","مۆڵەت",
                "license no","license_no","license","licence"],
    "agent_email": [
        "data entry email","agent email","data_entry_email","agent_email",
        "ئیمەیڵی ئەجنت","ئیمەیل ئەجنت","ئیمەیل داخڵکەر",
        "email agent","داخلكننده","وارد کننده",
        "email","ئیمەیل","ایمیل",
    ],
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
    if   period == "today":      cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_week":  cutoff = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_month": cutoff = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else: return df
    return df[df[col].apply(parse_dt) >= cutoff]

def _n_active(fb: str, fl: str) -> int:
    return sum([bool(fb.strip()), bool(fl.strip())])

def apply_filters_locally(df, f_binder: str, f_license: str, col_binder, col_license):
    r = df.copy()
    if f_binder.strip() and col_binder and col_binder in r.columns:
        r = r[r[col_binder].astype(str).str.contains(f_binder.strip(), case=False, na=False)]
    if f_license.strip() and col_license and col_license in r.columns:
        r = r[r[col_license].astype(str).str.contains(f_license.strip(), case=False, na=False)]
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


# -----------------------------------------------------------------------------
#  8 . GOOGLE SHEETS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    raw = json.loads(st.secrets["json_key"], strict=False)
    pk  = raw["private_key"]
    pk  = pk.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","")
    pk  = pk.replace("\\n","").replace("\n","")
    pk  = "".join(pk.split()); pk = "\n".join(textwrap.wrap(pk, 64))
    raw["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(raw, scope)
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
        st.session_state.local_df        = df.copy()
        st.session_state.local_headers   = h
        st.session_state.local_col_map   = cm
        st.session_state.local_cache_key = ck
        st.session_state.local_fetched_at = fetched_at
    return (st.session_state.local_df, st.session_state.local_headers,
            st.session_state.local_col_map, st.session_state.local_fetched_at or fetched_at)


# -----------------------------------------------------------------------------
#  9 . OPTIMISTIC MUTATIONS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix,
                              eval_val: str = "", feedback_val: str = ""):
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


# -----------------------------------------------------------------------------
#  10 . WRITE HELPERS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def ensure_system_cols_in_sheet(ws, headers, col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            np_ = len(headers) + 1
            if np_ > ws.col_count: _gsheets_call(ws.add_cols, max(4, np_ - ws.col_count + 1))
            _gsheets_call(ws.update_cell, 1, np_, sc)
            headers.append(sc); col_map[sc] = np_
    return headers, col_map

def write_approval_to_sheet(ws_title, sheet_row, col_map, headers, new_vals, record,
                            auditor, ts_now, log_prefix,
                            eval_val: str = "", feedback_val: str = "") -> bool:
    ws = get_spreadsheet().worksheet(ws_title)
    headers, col_map = ensure_system_cols_in_sheet(ws, headers, col_map)
    if COL_STATUS in col_map:
        status_a1 = rowcol_to_a1(sheet_row, col_map[COL_STATUS])
        live_status = _gsheets_call(ws.acell, status_a1).value
        if live_status == VAL_DONE:
            return False
    old = str(record.get(COL_LOG, "")).strip()
    new_log = f"{log_prefix}\n{old}".strip()
    batch = []
    for f, v in new_vals.items():
        if f in col_map and clean_cell(record.get(f, "")) != v:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[f]), "values": [[v]]})
    for cn, v in [
        (COL_STATUS, VAL_DONE), (COL_AUDITOR, auditor), (COL_DATE, ts_now),
        (COL_LOG, new_log), (COL_EVAL, eval_val), (COL_FEEDBACK, feedback_val),
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
        df_u = pd.DataFrame(records)
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


# -----------------------------------------------------------------------------
#  11 . HTML TABLE & PAGINATION  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def _eval_chip(raw: str) -> str:
    if not raw or raw == "-": return "-"
    n = _normalise_eval(raw)
    if "Good" in n:
        return f"<span class='s-chip s-eval-good'>{_html.escape(raw)}</span>"
    if "Bad" in n or "Incorrect" in n:
        return f"<span class='s-chip s-eval-bad'>{_html.escape(raw)}</span>"
    if "Duplicate" in n:
        return f"<span class='s-chip s-eval-dup'>{_html.escape(raw)}</span>"
    return f"<span class='s-chip s-pending'>{_html.escape(raw)}</span>"

def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty: st.info("No records to display."); return
    display_df = df.head(max_rows)
    th = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col == COL_LOG: continue
        extra_cls = ""
        if col == COL_EVAL:       extra_cls = " class='col-eval'"
        elif col == COL_FEEDBACK: extra_cls = " class='col-feedback'"
        th += f"<th{extra_cls}>{_html.escape(col)}</th>"
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
                d = _eval_chip(raw)
                r += f"<td class='col-eval'>{d}</td>"
                continue
            elif col == COL_FEEDBACK:
                trunc = (safe[:160] + "...") if len(safe) > 160 else (safe or "-")
                r += f"<td class='col-feedback'>{trunc}</td>"
                continue
            elif len(raw) > 55:
                d = f"<span title='{safe}'>{safe[:52]}...</span>"
            r += f"<td>{d}</td>"
        rows += f"<tr>{r}</tr>"
    st.markdown(
        f"<div class='gov-table-wrap'><table class='gov-table'>"
        f"<thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>",
        unsafe_allow_html=True)

def render_paginated_table(df: pd.DataFrame, page_key: str, max_rows: int = 5000) -> None:
    if df.empty:
        render_html_table(df)
        return
    if page_key not in st.session_state:
        st.session_state[page_key] = 1
    total_rows  = min(len(df), max_rows)
    total_pages = max(1, -(-total_rows // _PAGE_SIZE))
    st.session_state[page_key] = max(1, min(st.session_state[page_key], total_pages))
    current = st.session_state[page_key]
    start   = (current - 1) * _PAGE_SIZE
    end     = min(start + _PAGE_SIZE, total_rows)
    render_html_table(df.iloc[start:end], max_rows=_PAGE_SIZE)
    if total_pages > 1:
        col_prev, col_info, col_next = st.columns([1, 3, 1])
        with col_prev:
            if st.button("Prev", key=f"{page_key}_prev", disabled=(current <= 1), use_container_width=True):
                st.session_state[page_key] -= 1; st.rerun()
        with col_info:
            st.markdown(
                f"<div style='text-align:center;padding:8px 0;font-size:.75rem;font-weight:700;"
                f"color:var(--text-muted);font-family:var(--mono);'>"
                f"Page {current} of {total_pages} "
                f"<span style='font-weight:400;margin-left:8px;'>({start+1}-{end} of {total_rows} rows)</span>"
                f"</div>", unsafe_allow_html=True)
        with col_next:
            if st.button("Next", key=f"{page_key}_next", disabled=(current >= total_pages), use_container_width=True):
                st.session_state[page_key] += 1; st.rerun()


# -----------------------------------------------------------------------------
#  12 . LOGIN  (unchanged from v14.8 — cookie logic intact)
# -----------------------------------------------------------------------------
def render_login(spreadsheet_id: str, cookie_manager) -> None:
    st.markdown("""
    <style>
    [data-testid="stSidebar"], [data-testid="collapsedControl"], header {display: none !important;}
    .stApp {
        background: linear-gradient(-45deg, #0F172A, #1E3A8A, #3B82F6, #1E40AF);
        background-size: 400% 400%; animation: gradientBG 15s ease infinite;
    }
    @keyframes gradientBG {
        0%{background-position:0% 50%;} 50%{background-position:100% 50%;} 100%{background-position:0% 50%;}
    }
    .block-container {
        display:flex;flex-direction:column;justify-content:center;align-items:center;
        min-height:100vh;padding:1rem !important;
    }
    [data-testid="stForm"] {
        background:rgba(255,255,255,0.95)!important;backdrop-filter:blur(12px)!important;
        -webkit-backdrop-filter:blur(12px)!important;border:1px solid rgba(255,255,255,0.3)!important;
        border-radius:24px!important;padding:40px 30px!important;
        box-shadow:0 25px 50px -12px rgba(0,0,0,0.5)!important;
        max-width:420px!important;width:100%!important;margin:0 auto!important;
    }
    [data-testid="stFormSubmitButton"] button {
        background:linear-gradient(135deg,#1E3A8A 0%,#3B82F6 100%)!important;
        color:white!important;border:none!important;border-radius:12px!important;
        font-weight:bold!important;font-size:1rem!important;padding:0.6rem!important;
        width:100%!important;margin-top:10px!important;
        transition:transform 0.2s ease,box-shadow 0.2s ease!important;
    }
    [data-testid="stFormSubmitButton"] button:hover {
        transform:translateY(-2px)!important;
        box-shadow:0 10px 20px rgba(59,130,246,0.4)!important;color:white!important;
    }
    </style>""", unsafe_allow_html=True)

    st.markdown("""
    <script>
    (function () {
        function applyAutocomplete() {
            try {
                var doc = window.parent.document;
                var textInputs = doc.querySelectorAll('input[type="text"]');
                if (textInputs.length > 0) {
                    textInputs[0].setAttribute('autocomplete', 'username');
                    textInputs[0].setAttribute('name', 'username');
                }
                var pwInputs = doc.querySelectorAll('input[type="password"]');
                if (pwInputs.length > 0) {
                    pwInputs[0].setAttribute('autocomplete', 'current-password');
                    pwInputs[0].setAttribute('name', 'password');
                }
            } catch(e) {}
        }
        [150, 400, 800, 1500].forEach(function(ms) {
            setTimeout(applyAutocomplete, ms);
        });
    })();
    </script>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([8, 1, 1])
    with c2:
        if st.button("EN", key="lg_en"): st.session_state.lang = "en"; st.rerun()
    with c3:
        if st.button("KU", key="lg_ku"): st.session_state.lang = "ku"; st.rerun()

    with st.form("login_form", clear_on_submit=False):
        st.markdown(f"""
        <div style="text-align:center;font-size:3rem;margin-bottom:8px;line-height:1;">&#127963;</div>
        <div style="text-align:center;font-size:1.5rem;font-weight:800;color:#0F172A;margin-bottom:4px;">
            {_html.escape(t('portal_title'))}</div>
        <div style="text-align:center;font-size:.60rem;font-weight:700;color:#DC2626;
            background:#FEE2E2;padding:4px 10px;border-radius:99px;
            width:max-content;margin:0 auto 16px;letter-spacing:1px;">
            {_html.escape(t('classified'))}</div>
        <div style="text-align:center;font-size:.85rem;color:#475569;margin-bottom:20px;">
            {_html.escape(t('login_prompt'))}</div>""", unsafe_allow_html=True)
        st.text_input(t("email_field"), placeholder="user@mof.gov.iq", key="_login_email")
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
            display_email = "Admin" if role == "admin" else em.lower().strip()
            st.session_state.logged_in  = True
            st.session_state.user_email = display_email
            st.session_state.user_role  = role
            try:
                expires_at = datetime.now() + timedelta(days=1)
                cookie_manager.set(
                    _COOKIE_NAME,
                    f"{display_email}|{role}",
                    expires_at=expires_at,
                    key="login_set_cookie",
                )
            except Exception:
                pass
            st.rerun()
        else:
            st.error(t("bad_creds"))


# -----------------------------------------------------------------------------
#  13 . SIDEBAR  [2][3] dark mode toggle (admin only), language (admin/manager only)
# -----------------------------------------------------------------------------
def render_sidebar(headers, col_binder, col_license, is_admin, fetched_at,
                   cookie_manager):

    def clear_all_filters():
        for k in ("f_binder", "f_license"):
            st.session_state[k] = ""
        for pk in ("page_worklist", "page_archive", "page_logs"):
            st.session_state[pk] = 1

    role       = st.session_state.user_role
    role_label = {"admin": t("role_admin"), "manager": t("role_manager"),
                  "auditor": t("role_auditor")}.get(role, role.title())
    badge_cls  = {"admin": "role-badge-admin", "manager": "role-badge-manager",
                  "auditor": "role-badge-auditor"}.get(role, "role-badge-auditor")

    with st.sidebar:
        st.markdown(f"""
        <div class="sidebar-header">
          <div class="sidebar-logo-text">{t('portal_title')}</div>
          <div class="sidebar-ministry">{t('ministry')}</div>
        </div>
        <hr class="divider" style="margin:0;"/>""", unsafe_allow_html=True)

        # ── Refresh button — admin/manager only (unchanged) ──────────────────
        if st.session_state.get("user_role") in ("admin", "manager"):
            COOLDOWN = 600
            if "last_refresh_time" not in st.session_state:
                st.session_state.last_refresh_time = 0
            current_time = time.time()
            time_passed  = current_time - st.session_state.last_refresh_time
            time_left    = int((COOLDOWN - time_passed) / 60)
            can_refresh  = True
            if st.session_state.user_role == "manager" and time_passed < COOLDOWN:
                can_refresh = False

            def _do_refresh():
                _fetch_raw_sheet_cached.clear()
                _fetch_users_cached.clear()
                _fetch_sheet_metadata.clear()
                st.session_state.local_cache_key = None
                st.session_state.last_refresh_time = time.time()
                st.toast("Data refreshed for all users", icon="🔄")

            if can_refresh:
                st.button("🔄 Refresh Data", key="sb_refresh",
                          use_container_width=True, on_click=_do_refresh)
            else:
                st.button(f"⏳ Wait {max(1, time_left)} min", key="sb_refresh_disabled",
                          disabled=True, use_container_width=True)

            # [2] Dark Mode toggle — admin only
            if is_admin:
                st.toggle(
                    f"🌙 {t('dark_mode_label')}",
                    key="dark_mode",
                )

        st.markdown(f"""
        <div class="cache-strip">
          <span class="cache-badge">{t('local_mode')}</span>
          <div class="cache-info">{t('cache_age')}: {READ_TTL//60} min - Last sync: {fetched_at[-8:] if fetched_at else '-'}</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # [3] Language buttons — admin and manager only (auditors default to English)
        if st.session_state.user_role in ("admin", "manager"):
            st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
            lc1, lc2 = st.columns(2)
            if lc1.button("EN", use_container_width=True, key="sb_en"):
                st.session_state.lang = "en"; st.rerun()
            if lc2.button("KU", use_container_width=True, key="sb_ku"):
                st.session_state.lang = "ku"; st.rerun()

        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='adv-filter-header'>{t('adv_filters')}</div>",
                    unsafe_allow_html=True)

        for key, label, hint, disabled in [
            ("f_binder",  t("f_binder"),  col_binder  or "not detected", col_binder  is None),
            ("f_license", t("f_license"), col_license or "not detected", col_license is None),
        ]:
            st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{label}"
                        f"<span class='col-hint'> ({hint})</span></div>", unsafe_allow_html=True)
            st.text_input(label, key=key, disabled=disabled, label_visibility="collapsed")

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.button(f"X  {t('clear_filters')}", use_container_width=True,
                  key="clr_f", on_click=clear_all_filters)
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)

        st.markdown(f"""
        <div class="sb-user-card">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{_html.escape(st.session_state.user_email)}</div>
          <span class="{badge_cls}" style="margin-top:8px;">{role_label}</span>
        </div>""", unsafe_allow_html=True)

        # Self-service password change (unchanged)
        st.markdown("<hr class='divider' style='margin: 8px 0;'/>", unsafe_allow_html=True)
        if st.toggle(f"🔒 {t('update_pw')}", key="toggle_pw"):
            with st.form("change_my_pw_form_sidebar"):
                new_pw = st.text_input(t("password_field"), type="password")
                if st.form_submit_button(t("update_pw"), use_container_width=True):
                    if new_pw.strip():
                        try:
                            spr  = get_spreadsheet()
                            uws  = spr.worksheet(USERS_SHEET)
                            cell = _gsheets_call(uws.find, st.session_state.user_email)
                            if cell:
                                _gsheets_call(uws.update_cell, cell.row, 2, hash_pw(new_pw.strip()))
                                _fetch_users_cached.clear()
                                st.success("بە سەرکەوتوویی گۆڕدرا! 🔒")
                                time.sleep(1.5); st.rerun()
                            else:
                                st.error("ئەکاونتەکەت نەدۆزرایەوە.")
                        except Exception as e:
                            st.error(f"هەڵەیەک ڕوویدا: {e}")
                    else:
                        st.warning("تکایە پاسۆردی نوێ بنووسە.")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Sign-out — cookie delete (unchanged)
        if st.button(f"-> {t('sign_out')}", use_container_width=True, key="sb_logout"):
            try:
                cookie_manager.delete(_COOKIE_NAME, key="logout_delete_cookie")
            except Exception:
                pass
            for k, v in _DEFAULTS.items(): st.session_state[k] = v
            st.rerun()

    return (
        st.session_state.get("f_binder",  ""),
        st.session_state.get("f_license", ""),
    )

def render_filter_bar(total: int, filtered: int, f_binder: str, f_license: str) -> None:
    n = _n_active(f_binder, f_license)
    if n == 0: return
    badges = ""
    if f_binder.strip():  badges += f"<span class='filter-badge'>{_html.escape(f_binder.strip()[:20])}</span> "
    if f_license.strip(): badges += f"<span class='filter-badge'>{_html.escape(f_license.strip()[:20])}</span> "
    st.markdown(f"""<div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:800;color:var(--indigo-600);text-transform:uppercase;letter-spacing:.08em;">
        {t('active_filters')} ({n})</span> {badges}
      <span class="result-count"><strong style="color:var(--indigo-600);">{filtered}</strong>/{total}&nbsp;{t('results_shown')}</span>
    </div>""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
#  DEEP SEARCH WIDGET  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def render_deep_search_strip(key_prefix: str, col_binder, col_agent_email):
    def _clear():
        st.session_state[f"{key_prefix}_binder"] = ""
        st.session_state[f"{key_prefix}_agent"]  = ""
        for pk in ("page_worklist", "page_archive", "page_logs"):
            st.session_state[pk] = 1

    ph_binder = col_binder      or "column not detected in sheet"
    ph_agent  = col_agent_email or "column not detected in sheet"

    st.markdown(
        f"<div class='deep-search-strip'>"
        f"<div class='deep-search-title'>{t('deep_search')}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([1, 1, 0.32])
    with c1:
        st.text_input(t("ds_binder"), key=f"{key_prefix}_binder",
                      placeholder=ph_binder, disabled=(col_binder is None))
    with c2:
        st.text_input(t("ds_agent"), key=f"{key_prefix}_agent",
                      placeholder=ph_agent, disabled=(col_agent_email is None))
    with c3:
        st.markdown("<div style='margin-top:22px;'>", unsafe_allow_html=True)
        st.button(t("ds_clear"), key=f"{key_prefix}_clr",
                  use_container_width=True, on_click=_clear)
        st.markdown("</div>", unsafe_allow_html=True)

    return (
        st.session_state.get(f"{key_prefix}_binder", ""),
        st.session_state.get(f"{key_prefix}_agent",  ""),
    )


def apply_deep_search(df, srch_binder: str, srch_agent: str, col_binder, col_agent_email):
    r = df.copy()
    if srch_binder.strip() and col_binder      and col_binder      in r.columns:
        r = r[r[col_binder].astype(str).str.contains(srch_binder.strip(), case=False, na=False)]
    if srch_agent.strip()  and col_agent_email and col_agent_email in r.columns:
        r = r[r[col_agent_email].astype(str).str.contains(srch_agent.strip(), case=False, na=False)]
    return r


def _deep_search_active(b: str, a: str) -> bool:
    return any(x.strip() for x in (b, a))


# -----------------------------------------------------------------------------
#  14 . WORKLIST  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def render_worklist(pending_display, df, headers, col_map, ws_title, f_binder, f_license):
    p_count = len(pending_display)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">{t('worklist_title')}</div>
      <div class="worklist-sub">{t('worklist_sub')}</div></div>
      <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
    </div>""", unsafe_allow_html=True)

    if pending_display.empty:
        st.info(t("no_match") if _n_active(f_binder, f_license) else "All cases processed.")
        return

    render_paginated_table(pending_display, page_key="page_worklist")

    st.markdown(f"<div class='section-title'>{t('select_case')}</div>", unsafe_allow_html=True)
    label_col = next((h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row")
    opts = ["-"] + [
        f"Row {idx}{_ROW_SEP}{str(row.get(label_col, ''))[:55]}"
        for idx, row in pending_display.iterrows()
    ]
    row_sel = st.selectbox("", opts, key="row_sel", label_visibility="collapsed")
    if row_sel == "-": return

    sheet_row = int(row_sel.split(_ROW_SEP)[0].replace("Row ", "").strip())
    df_iloc   = sheet_row - 2
    if df_iloc < 0 or df_iloc >= len(df): st.error("Row index out of range."); return
    record = df.iloc[df_iloc].to_dict()

    with st.expander(t("audit_trail"), expanded=False):
        history = str(record.get(COL_LOG, "")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip():
                    st.markdown(f'<div class="log-line">{_html.escape(line)}</div>',
                                unsafe_allow_html=True)
        else:
            st.caption(t("no_history"))

    st.markdown(f"<div class='section-title'>{t('processing')} #{sheet_row}</div>",
                unsafe_allow_html=True)
    SKIP   = set(SYSTEM_COLS)
    fields = {k: v for k, v in record.items() if k not in SKIP}

    with st.form("audit_form"):
        new_vals = {}
        for fname, fval in fields.items():
            new_vals[fname] = st.text_input(fname, value=clean_cell(fval), key=f"field_{fname}")
        st.markdown("<hr style='border-top:1px dashed var(--border);margin:18px 0 14px;'/>",
                    unsafe_allow_html=True)
        eval_val     = st.selectbox(t("eval_label"), options=EVAL_OPTIONS, index=0, key="form_eval")
        manual_notes = st.text_area(t("feedback_label"), placeholder=t("feedback_placeholder"),
                                    key="form_feedback", height=100)
        do_submit    = st.form_submit_button(t("approve_save"), use_container_width=True)

    if do_submit:
        ts_now    = now_str()
        auditor   = st.session_state.user_email
        log_prefix = f"[x] {auditor} | {ts_now}"
        auto_diff  = build_auto_diff(record, new_vals)
        feedback_combined = (f"{manual_notes.strip()}\n{auto_diff}".strip()
                             if manual_notes.strip() else auto_diff)
        with st.spinner("Committing record to Google Sheets..."):
            try:
                is_success = write_approval_to_sheet(
                    ws_title, sheet_row, col_map, headers, new_vals, record,
                    auditor, ts_now, log_prefix,
                    eval_val=eval_val, feedback_val=feedback_combined)
                if not is_success:
                    st.toast("Another auditor already processed this case.")
                    st.session_state.local_df.at[df_iloc, COL_STATUS] = VAL_DONE
                    time.sleep(2); st.rerun(); return
            except gspread.exceptions.APIError as e:
                st.error(f"Write failed: {e}"); return
        _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix,
                                  eval_val=eval_val, feedback_val=feedback_combined)
        st.toast(t("saved_ok"), icon="✅")
        time.sleep(0.6); st.rerun()


# -----------------------------------------------------------------------------
#  15 . ARCHIVE  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def render_archive(done_view, df, col_map, ws_title, is_admin,
                   col_binder=None, col_license=None):

    def clear_arch_search():
        for k in ("arch_binder", "arch_license", "arch_auditor"):
            st.session_state[k] = ""
        st.session_state["page_archive"] = 1

    d_count = len(done_view)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">Processed Archive</div>
      <div class="worklist-sub">Completed and committed audit records</div></div>
      <span class="chip chip-done">{d_count} {t('processed')}</span>
    </div>""", unsafe_allow_html=True)

    st.markdown(
        f"<div style='margin-bottom:8px;font-size:.62rem;font-weight:800;"
        f"letter-spacing:.10em;text-transform:uppercase;color:var(--indigo-600);'>"
        f"{t('arch_search_title')}</div>",
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns([1, 1, 1, 0.28])
    with c1:
        s_binder  = st.text_input("Binder No.", key="arch_binder",
                                  placeholder=col_binder  or "column not in sheet",
                                  disabled=(col_binder  is None))
    with c2:
        s_license = st.text_input("License No.", key="arch_license",
                                  placeholder=col_license or "column not in sheet",
                                  disabled=(col_license is None))
    with c3:
        s_auditor = st.text_input("Auditor Email", key="arch_auditor",
                                  placeholder="e.g. auditor@mof.gov")
    with c4:
        st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
        st.button("X", key="arch_clr", on_click=clear_arch_search, use_container_width=True)

    filtered_view = done_view.copy()
    if s_binder.strip()  and col_binder  and col_binder  in filtered_view.columns:
        filtered_view = filtered_view[filtered_view[col_binder].astype(str).str.contains(
            s_binder.strip(), case=False, na=False)]
    if s_license.strip() and col_license and col_license in filtered_view.columns:
        filtered_view = filtered_view[filtered_view[col_license].astype(str).str.contains(
            s_license.strip(), case=False, na=False)]
    if s_auditor.strip() and COL_AUDITOR in filtered_view.columns:
        filtered_view = filtered_view[filtered_view[COL_AUDITOR].astype(str).str.contains(
            s_auditor.strip(), case=False, na=False)]

    st.markdown("<hr class='divider'/>", unsafe_allow_html=True)

    if filtered_view.empty:
        st.info("No processed records match the search.")
    else:
        if is_admin:
            st.markdown(
                f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
                f"border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);"
                f"padding:10px 16px;margin-bottom:14px;font-size:.78rem;"
                f"color:var(--indigo-600)!important;font-weight:600;'>"
                f"{t('archive_quality_note')}</div>", unsafe_allow_html=True)
        priority_cols = [COL_STATUS, COL_EVAL, COL_FEEDBACK, COL_AUDITOR, COL_DATE]
        other_cols    = [c for c in filtered_view.columns if c not in priority_cols and c != COL_LOG]
        ordered_cols  = [c for c in priority_cols if c in filtered_view.columns] + other_cols
        render_paginated_table(filtered_view[ordered_cols], page_key="page_archive")

    if is_admin and not done_view.empty:
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='section-title'>{t('reopen')}</div>", unsafe_allow_html=True)
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


# -----------------------------------------------------------------------------
#  16 . ANALYTICS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def render_analytics(df, col_agent_email=None, col_binder=None):
    pt = "plotly_white"; pb = "#FFFFFF"; pg = "#E4E7F0"; fc = "#0D1117"
    nvy = "#4F46E5"; blu = "#60A5FA"

    srch_binder, srch_agent = render_deep_search_strip("anal", col_binder, col_agent_email)
    work_df = apply_deep_search(df, srch_binder, srch_agent, col_binder, col_agent_email)

    if _deep_search_active(srch_binder, srch_agent):
        terms = [_html.escape(x) for x in (srch_binder, srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
            f"border-radius:var(--radius-md);padding:9px 16px;margin-bottom:14px;"
            f"font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>"
            f"{t('ds_showing')} <strong>{' &middot; '.join(terms)}</strong>"
            f" &mdash; <strong>{len(work_df)}</strong> records matched</div>",
            unsafe_allow_html=True)

    st.markdown(f"<div class='section-title'>{t('period')}</div>", unsafe_allow_html=True)
    periods = [("all", t("all_time")), ("today", t("today")),
               ("this_week", t("this_week")), ("this_month", t("this_month"))]
    for cw, (pk, pl) in zip(st.columns(len(periods)), periods):
        lbl = f"[{pl}]" if st.session_state.date_filter == pk else pl
        if cw.button(lbl, use_container_width=True, key=f"pf_{pk}"):
            st.session_state.date_filter = pk; st.rerun()

    done_base = work_df[work_df[COL_STATUS] == VAL_DONE].copy()
    done_f    = apply_period_filter(done_base, COL_DATE, st.session_state.date_filter)
    if done_f.empty: st.info(t("no_records")); return

    ma, mb, mc = st.columns(3)
    ma.metric(t("records_period"), len(done_f))
    active = 0
    if COL_DATE in done_f.columns:
        active = done_f[COL_DATE].apply(
            lambda s: parse_dt(s).date() if parse_dt(s) else None).nunique()
    mb.metric(t("active_days"), active)
    mc.metric(t("avg_per_day"), f"{len(done_f)/max(active,1):.1f}")

    left, right = st.columns([1, 1.6], gap="large")

    with left:
        st.markdown(f"<div class='section-title'>{t('leaderboard')}</div>", unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = done_f[COL_AUDITOR].replace("", "-").value_counts().reset_index()
            lb.columns = ["Auditor", "Count"]
            for i, r in lb.head(10).iterrows():
                st.markdown(
                    f'<div class="lb-row">'
                    f'<span class="lb-medal">{i+1}.</span>'
                    f'<span class="lb-name">{_html.escape(str(r["Auditor"]))}</span>'
                    f'<span class="lb-count">{r["Count"]}</span>'
                    f'</div>', unsafe_allow_html=True)
            fig = px.bar(lb.head(10), x="Count", y="Auditor", orientation="h",
                         color="Count", color_continuous_scale=[blu, nvy], template=pt)
            fig.update_traces(marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>Records: <b>%{x}</b><extra></extra>")
            fig.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                showlegend=False, coloraxis_showscale=False, margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color="#4B5563")),
                height=min(320, max(180, 36*len(lb.head(10)))))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown(f"<div class='section-title'>{t('daily_trend')}</div>", unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f["_date"] = done_f[COL_DATE].apply(
                lambda s: parse_dt(s).date() if parse_dt(s) else None)
            trend = done_f.dropna(subset=["_date"]).groupby("_date").size().reset_index(name="Records")
            trend.columns = ["Date", "Records"]
            if not trend.empty:
                if len(trend) > 1:
                    rng   = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = trend.set_index("Date").reindex(rng.date, fill_value=0).reset_index()
                    trend.columns = ["Date", "Records"]
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=trend["Date"], y=trend["Records"], mode="none",
                    fill="tozeroy", fillcolor="rgba(99,102,241,0.07)", showlegend=False))
                fig2.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"], mode="lines+markers",
                    line=dict(color=nvy, width=2.5),
                    marker=dict(color=blu, size=7, line=dict(color="#FFFFFF", width=2)),
                    hovertemplate="<b>%{x}</b><br>Records: <b>%{y}</b><extra></extra>"))
                fig2.update_layout(
                    template=pt, paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                    showlegend=False, margin=dict(l=8,r=8,t=10,b=8),
                    xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                    yaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                    height=380, hovermode="x unified")
                st.plotly_chart(fig2, use_container_width=True)
            else: st.info(t("no_records"))

    st.markdown(f"<div class='section-title'>{t('acc_ranking_title')}</div>", unsafe_allow_html=True)

    if col_agent_email and col_agent_email in done_f.columns and COL_EVAL in done_f.columns:
        eval_df = done_f[[col_agent_email, COL_EVAL]].copy()
        eval_df[col_agent_email] = eval_df[col_agent_email].replace("", "-")

        def _classify(v: str) -> str:
            n = _normalise_eval(v)
            if "Good" in n:                    return "good"
            if "Bad" in n or "Incorrect" in n: return "bad"
            if "Duplicate" in n:               return "dup"
            return "unrated"

        eval_df["_cls"] = eval_df[COL_EVAL].apply(_classify)
        acc = (eval_df.groupby(col_agent_email)["_cls"]
               .value_counts().unstack(fill_value=0).reset_index())
        for cn in ("good", "bad", "dup", "unrated"):
            if cn not in acc.columns: acc[cn] = 0
        acc["Total"]    = acc["good"] + acc["bad"] + acc["dup"] + acc["unrated"]
        acc["Accuracy"] = (acc["good"] / acc["Total"].replace(0, 1) * 100).round(1)
        acc = acc.sort_values("Accuracy", ascending=False).reset_index(drop=True)

        th_row = (f"<tr><th>#</th><th>{t('acc_agent')}</th><th>{t('acc_total')}</th>"
                  f"<th>{t('acc_good')}</th><th>{t('acc_bad')}</th><th>{t('acc_dup')}</th>"
                  f"<th>{t('acc_rate')}</th></tr>")
        td_rows = ""
        for i, row in acc.iterrows():
            pct = row["Accuracy"]
            if pct >= 80:   rc = "acc-rate-high"; bc = "#16A34A"
            elif pct >= 50: rc = "acc-rate-mid";  bc = "#B45309"
            else:           rc = "acc-rate-low";  bc = "#DC2626"
            bar = (f"<span class='acc-bar-wrap'>"
                   f"<span class='acc-bar-fill' style='width:{int(pct)}%;background:{bc};display:block;'></span>"
                   f"</span>")
            td_rows += (
                f"<tr>"
                f"<td style='color:var(--text-muted);font-family:var(--mono);font-size:.70rem;'>{i+1}</td>"
                f"<td style='font-weight:600;'>{_html.escape(str(row[col_agent_email]))}</td>"
                f"<td style='font-family:var(--mono);font-weight:700;'>{int(row['Total'])}</td>"
                f"<td><span class='s-chip s-eval-good'>{int(row['good'])}</span></td>"
                f"<td><span class='s-chip s-eval-bad'>{int(row['bad'])}</span></td>"
                f"<td><span class='s-chip s-eval-dup'>{int(row['dup'])}</span></td>"
                f"<td class='{rc}'>{pct}% {bar}</td>"
                f"</tr>"
            )
        st.markdown(
            f"<div class='gov-table-wrap'><table class='acc-table'>"
            f"<thead>{th_row}</thead><tbody>{td_rows}</tbody></table></div>",
            unsafe_allow_html=True)

        st.markdown(f"<div class='section-title'>{t('eval_breakdown')}</div>", unsafe_allow_html=True)
        st.caption(t("eval_breakdown_sub"))
        if not acc.empty:
            good_pct = (acc["good"]    / acc["Total"].replace(0,1) * 100).round(1)
            bad_pct  = (acc["bad"]     / acc["Total"].replace(0,1) * 100).round(1)
            dup_pct  = (acc["dup"]     / acc["Total"].replace(0,1) * 100).round(1)
            unr_pct  = (acc["unrated"] / acc["Total"].replace(0,1) * 100).round(1)
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name="Good",          x=acc[col_agent_email], y=acc["good"],    marker_color="#16A34A", hovertemplate="<b>%{x}</b><br>Good: <b>%{y}</b> (%{customdata[0]}%)<br>Total: %{customdata[1]}<extra></extra>", customdata=list(zip(good_pct, acc["Total"]))))
            fig3.add_trace(go.Bar(name="Bad/Incorrect", x=acc[col_agent_email], y=acc["bad"],     marker_color="#DC2626", hovertemplate="<b>%{x}</b><br>Bad: <b>%{y}</b> (%{customdata[0]}%)<br>Total: %{customdata[1]}<extra></extra>",  customdata=list(zip(bad_pct,  acc["Total"]))))
            fig3.add_trace(go.Bar(name="Duplicate",     x=acc[col_agent_email], y=acc["dup"],     marker_color="#F59E0B", hovertemplate="<b>%{x}</b><br>Dup: <b>%{y}</b> (%{customdata[0]}%)<br>Total: %{customdata[1]}<extra></extra>",  customdata=list(zip(dup_pct,  acc["Total"]))))
            if acc["unrated"].sum() > 0:
                fig3.add_trace(go.Bar(name="Unrated", x=acc[col_agent_email], y=acc["unrated"], marker_color="#9CA3AF", hovertemplate="<b>%{x}</b><br>Unrated: <b>%{y}</b> (%{customdata}%)<extra></extra>", customdata=unr_pct))
            fig3.update_layout(
                barmode="stack", template=pt, paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=11), bgcolor="rgba(255,255,255,0.8)",
                            bordercolor="#E4E7F0", borderwidth=1),
                margin=dict(l=8,r=8,t=40,b=60),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563"),
                           tickangle=-30, title=dict(text="Agent", font=dict(size=11, color="#4B5563"))),
                yaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563"),
                           title=dict(text="Records", font=dict(size=11, color="#4B5563"))),
                height=400, hovermode="x")
            fig3.update_traces(marker_line_width=0)
            st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info(t("acc_no_data") +
                ("" if col_agent_email else " (Agent Email column not detected - check sheet headers.)"))


# -----------------------------------------------------------------------------
#  17 . AUDITOR LOGS  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def render_auditor_logs(df, col_company, col_binder, col_agent_email=None):
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">{t('logs_title')}</div>
        <div class="worklist-sub">{t('logs_sub')}</div>
      </div>
      <span class="chip chip-admin">Admin / Manager</span>
    </div>""", unsafe_allow_html=True)

    srch_binder, srch_agent = render_deep_search_strip("logs", col_binder, col_agent_email)

    done_df = df[df[COL_STATUS] == VAL_DONE].copy()
    if done_df.empty: st.info(t("logs_no_data")); return

    done_df = apply_deep_search(done_df, srch_binder, srch_agent, col_binder, col_agent_email)

    if _deep_search_active(srch_binder, srch_agent):
        terms = [_html.escape(x) for x in (srch_binder, srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
            f"border-radius:var(--radius-md);padding:9px 16px;margin-bottom:14px;"
            f"font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>"
            f"{t('ds_showing')} <strong>{' &middot; '.join(terms)}</strong>"
            f" &mdash; <strong>{len(done_df)}</strong> records matched</div>",
            unsafe_allow_html=True)

    if done_df.empty: st.info(t("logs_no_data")); return

    display_cols: list[str] = [COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]
    if col_company     and col_company     in done_df.columns: display_cols.insert(1, col_company)
    if col_binder      and col_binder      in done_df.columns: display_cols.insert(1, col_binder)
    if col_agent_email and col_agent_email in done_df.columns: display_cols.insert(2, col_agent_email)
    seen_c: set = set()
    display_cols = [c for c in display_cols
                    if c in done_df.columns and not (c in seen_c or seen_c.add(c))]

    auditor_list = sorted(
        [a for a in done_df[COL_AUDITOR].unique() if str(a).strip() not in ("", "-")],
        key=str.lower)
    all_opt = t("logs_filter_all")
    sel_aud = st.selectbox(t("logs_auditor_sel"), options=[all_opt] + auditor_list,
                           key="logs_auditor_sel")
    view_df = (done_df[done_df[COL_AUDITOR] == sel_aud].copy()
               if sel_aud != all_opt else done_df.copy())

    total_p = len(view_df)
    uniq_a  = view_df[COL_AUDITOR].nunique()
    pdates  = view_df[COL_DATE].apply(parse_dt).dropna()
    dr_str  = (f"{pdates.min().strftime('%Y-%m-%d')} - {pdates.max().strftime('%Y-%m-%d')}"
               if not pdates.empty else "-")

    st.markdown(f"""
    <div class="log-summary-card">
      <div class="log-stat-row">
        <div class="log-stat"><span class="log-stat-value">{total_p}</span><span class="log-stat-label">{t('logs_total')}</span></div>
        <div class="log-stat-divider"></div>
        <div class="log-stat"><span class="log-stat-value">{uniq_a}</span><span class="log-stat-label">{t('logs_auditors')}</span></div>
        <div class="log-stat-divider"></div>
        <div class="log-stat"><span class="log-stat-value" style="font-size:1.05rem;">{dr_str}</span><span class="log-stat-label">{t('logs_date_range')}</span></div>
      </div>
    </div>""", unsafe_allow_html=True)

    shown = " - ".join(display_cols)
    st.markdown(f"<div class='section-title'>{t('logs_cols_shown')}: "
                f"<span style='font-weight:400;text-transform:none;letter-spacing:0;'>"
                f"{_html.escape(shown)}</span></div>", unsafe_allow_html=True)

    table_df = view_df[display_cols].copy()
    if COL_DATE in table_df.columns:
        table_df["_sort"] = table_df[COL_DATE].apply(parse_dt)
        table_df = table_df.sort_values("_sort", ascending=False, na_position="last").drop(columns=["_sort"])
    table_df = table_df.reset_index(drop=True)

    render_paginated_table(table_df, page_key="page_logs")

    csv_buf = io.StringIO()
    table_df.to_csv(csv_buf, index=False, encoding="utf-8-sig")
    csv_bytes = csv_buf.getvalue().encode("utf-8-sig")
    dtag = datetime.now(TZ).strftime("%Y%m%d")
    atag = sel_aud.replace("@","_").replace(".","_") if sel_aud != all_opt else "all_auditors"
    st.markdown(f"""
    <div class="export-strip">
      <div><div class="export-text">{t('logs_export_hdr')}</div>
      <div class="export-sub">{t('logs_export_sub')} - {total_p} rows - {len(display_cols)} columns</div></div>
    </div>""", unsafe_allow_html=True)
    st.download_button(label=t("logs_export_btn"), data=csv_bytes,
                       file_name=f"audit_log_{atag}_{dtag}.csv", mime="text/csv",
                       key="logs_csv_download")


# -----------------------------------------------------------------------------
#  18 . USER ADMIN  (unchanged from v14.8)
# -----------------------------------------------------------------------------
def _ensure_role_col(df_u: pd.DataFrame) -> pd.DataFrame:
    if "role" not in df_u.columns:
        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
        col_idx = len(df_u.columns) + 1
        try:
            _gsheets_call(uws.update_cell, 1, col_idx, "role")
            for i in range(2, len(df_u) + 2):
                _gsheets_call(uws.update_cell, i, col_idx, "auditor")
            _fetch_users_cached.clear()
        except Exception: pass
    return df_u

def render_user_admin(spreadsheet_id):
    staff_raw = _fetch_users_cached(spreadsheet_id)
    staff     = pd.DataFrame(staff_raw) if staff_raw else pd.DataFrame()
    if not staff.empty:
        staff = _ensure_role_col(staff)

    cl, cr = st.columns([1, 1], gap="large")

    with cl:
        st.markdown(f"<div class='section-title'>{t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_e = st.text_input("Email", placeholder="user@mof.gov")
            nu_p = st.text_input("Password", type="password")
            nu_r = st.selectbox(t("role_label"), VALID_ROLES, format_func=lambda r: r.title())
            if st.form_submit_button("Register User", use_container_width=True):
                if nu_e.strip() and nu_p.strip():
                    already = (not staff.empty and
                               nu_e.lower().strip() in staff.get("email", pd.Series()).values)
                    if already: st.error(t("dup_email"))
                    else:
                        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                        _gsheets_call(uws.append_row,
                                      [nu_e.lower().strip(), hash_pw(nu_p.strip()), nu_r, now_str()])
                        _fetch_users_cached.clear()
                        st.success(f"{nu_e} registered as {nu_r}.")
                        time.sleep(0.7); st.rerun()
                else: st.warning(t("fill_fields"))

        st.markdown(f"<div class='section-title'>{t('update_pw')}</div>", unsafe_allow_html=True)
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
                            st.success(f"Updated for {se}.")
                            time.sleep(0.7); st.rerun()

        st.markdown(f"<div class='section-title'>{t('change_role')}</div>", unsafe_allow_html=True)
        st.caption(t("change_role_sub"))
        if not staff.empty and "email" in staff.columns:
            with st.form("change_role_form"):
                cr_email = st.selectbox("Select user", staff["email"].tolist(), key="cr_email_sel")
                cr_role  = st.selectbox("New Role", VALID_ROLES,
                                        format_func=lambda r: r.title(), key="cr_role_sel")
                if st.form_submit_button("Update Role", use_container_width=True):
                    try:
                        spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                        header_row   = _gsheets_call(uws.row_values, 1)
                        role_col_idx = (header_row.index("role") + 1) if "role" in header_row \
                                       else len(header_row) + 1
                        if "role" not in header_row:
                            _gsheets_call(uws.update_cell, 1, role_col_idx, "role")
                        user_cell = _gsheets_call(uws.find, cr_email)
                        if user_cell:
                            _gsheets_call(uws.update_cell, user_cell.row, role_col_idx, cr_role)
                            _fetch_users_cached.clear()
                            st.success(f"{t('role_updated')} ({cr_email} -> {cr_role})")
                            time.sleep(0.7); st.rerun()
                        else:
                            st.error("User not found in sheet.")
                    except Exception as e:
                        st.error(f"Role update failed: {e}")

    with cr:
        st.markdown(f"<div class='section-title'>{t('staff_dir')}</div>", unsafe_allow_html=True)
        if not staff.empty and "email" in staff.columns:
            show_cols = [c for c in ["email", "role", "created_at"] if c in staff.columns]
            tbl = staff[show_cols].copy().reset_index()
            th_html = ("<tr><th class='row-idx'>#</th>" +
                       "".join(f"<th>{_html.escape(c)}</th>" for c in show_cols) + "</tr>")
            td_html = ""
            for _, row in tbl.iterrows():
                tr = f"<td class='row-idx'>{row['index']}</td>"
                for c in show_cols:
                    val = str(row.get(c, "")) or "-"
                    if c == "role":
                        safe_role = val if val in VALID_ROLES else "auditor"
                        tr += f"<td><span class='role-badge-{safe_role}'>{_html.escape(val.title())}</span></td>"
                    else:
                        tr += f"<td>{_html.escape(val[:40])}</td>"
                td_html += f"<tr>{tr}</tr>"
            st.markdown(f"<div class='gov-table-wrap'><table class='gov-table'>"
                        f"<thead><tr>{th_html}</tr></thead><tbody>{td_html}</tbody>"
                        f"</table></div>", unsafe_allow_html=True)

            st.markdown(f"<div class='section-title'>{t('remove_user')}</div>", unsafe_allow_html=True)
            de = st.selectbox("Select to revoke", ["-"] + staff["email"].tolist(), key="del_sel")
            if de != "-":
                if st.button(f"Revoke access - {_html.escape(de)}", key="del_btn"):
                    spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
                    cell = _gsheets_call(uws.find, de)
                    if cell:
                        _gsheets_call(uws.delete_rows, cell.row)
                        _fetch_users_cached.clear()
                        st.success(f"{de} revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered yet.")


# -----------------------------------------------------------------------------
#  19 . MAIN CONTROLLER  [1][4] dark mode call + auditor sees Worklist only
# -----------------------------------------------------------------------------
def main():
    cookie_manager = stx.CookieManager(key="portal_cm")

    if not st.session_state.logged_in:
        try:
            raw_cookie = cookie_manager.get(cookie=_COOKIE_NAME)
            if raw_cookie:
                parts = str(raw_cookie).split("|", 1)
                if len(parts) == 2:
                    c_email, c_role = parts[0].strip(), parts[1].strip()
                    if c_role in (VALID_ROLES + ["admin"]):
                        st.session_state.logged_in  = True
                        st.session_state.user_email = c_email
                        st.session_state.user_role  = c_role
        except Exception:
            pass

    try:
        # [1] Apply base CSS first, then dark mode override if toggled
        inject_css()
        if st.session_state.get("dark_mode"):
            inject_dark_mode()

        def _on_ws_change():
            for k in ("f_binder", "f_license"):
                st.session_state[k] = ""
            for k in ("arch_binder", "arch_license", "arch_auditor"):
                st.session_state[k] = ""
            for prefix in ("anal", "logs"):
                for suffix in ("_binder", "_agent"):
                    st.session_state[f"{prefix}{suffix}"] = ""
            for pk in ("page_worklist", "page_archive", "page_logs"):
                st.session_state[pk] = 1
            st.session_state["local_cache_key"] = None
            st.session_state["local_df"]        = None
            st.session_state["local_headers"]   = None
            st.session_state["local_col_map"]   = None
            st.session_state["active_ws_key"]   = None

        sid, all_titles = _fetch_sheet_metadata()

        if USERS_SHEET not in all_titles:
            spr = get_spreadsheet()
            uw = spr.add_worksheet(title=USERS_SHEET, rows="500", cols="4")
            _gsheets_call(uw.append_row, ["email", "password", "role", "created_at"])
            _fetch_sheet_metadata.clear()
            all_titles.append(USERS_SHEET)

        if not st.session_state.logged_in:
            render_login(sid, cookie_manager); return

        st.markdown("<style>[data-testid='stSidebar']{display:flex!important;}</style>",
                    unsafe_allow_html=True)

        role          = st.session_state.user_role
        is_admin      = (role == "admin")
        is_manager    = (role == "manager")
        can_analytics = is_admin or is_manager

        ts_str = datetime.now(TZ).strftime("%A, %d %B %Y  -  %H:%M")
        st.markdown(f"""
        <div class="page-header">
          <div>
            <div class="page-title">{_html.escape(t('portal_title'))}</div>
            <div class="page-subtitle">{_html.escape(t('ministry'))}</div>
          </div>
          <div class="page-timestamp">{ts_str}</div>
        </div>""", unsafe_allow_html=True)

        atm       = {title.strip().lower(): title for title in all_titles}
        available = [atm[s.strip().lower()] for s in VISIBLE_SHEETS if s.strip().lower() in atm]

        df = pd.DataFrame(); headers = []; col_map = {}; ws_title = None; fetched_at = "-"

        if not available:
            st.warning("None of the configured worksheets found. Expected: " + ", ".join(VISIBLE_SHEETS))
        else:
            ws_title = st.selectbox(
                t("workspace"), available, key="ws_sel", on_change=_on_ws_change)
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

        f_binder, f_license = render_sidebar(
            headers, col_binder, col_license, is_admin, fetched_at, cookie_manager)

        if not df.empty:
            st.markdown(f"<div class='section-title'>{t('overview')}</div>", unsafe_allow_html=True)
            total_n   = len(df)
            done_n    = int((df[COL_STATUS] == VAL_DONE).sum())
            pending_n = total_n - done_n
            pct       = done_n / total_n if total_n else 0
            m1, m2, m3 = st.columns(3)
            m1.metric(t("total"),       total_n)
            m2.metric(t("processed"),   done_n,    delta=f"{int(pct*100)}%")
            m3.metric(t("outstanding"), pending_n,
                      delta=f"{100-int(pct*100)}% remaining", delta_color="inverse")
            st.markdown(f"""
            <div class="prog-labels"><span>{t('processed')}</span><span>{int(pct*100)}%</span></div>
            <div class="prog-wrap"><div class="prog-fill" style="width:{int(pct*100)}%;"></div></div>""",
            unsafe_allow_html=True)

            filtered_df = apply_filters_locally(df, f_binder, f_license, col_binder, col_license)
            render_filter_bar(total_n, len(filtered_df), f_binder, f_license)
        else:
            filtered_df = pd.DataFrame()

        # ── RBAC tab construction [4] ─────────────────────────────────────────
        if is_admin:
            tabs = st.tabs([t("tab_worklist"), t("tab_archive"),
                            t("tab_analytics"), t("tab_logs"), t("tab_users")])
            t_work, t_arch, t_anal, t_logs, t_uadm = tabs

        elif is_manager:
            tabs = st.tabs([t("tab_worklist"), t("tab_archive"),
                            t("tab_analytics"), t("tab_logs")])
            t_work, t_arch, t_anal, t_logs = tabs
            t_uadm = None

        else:
            # [4] Auditors: Worklist ONLY — no Archive, no Analytics
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>",
                        unsafe_allow_html=True)
            tabs   = st.tabs([t("tab_worklist")])
            t_work = tabs[0]
            t_arch = None   # [4] explicitly None — guard below prevents errors
            t_anal = None
            t_logs = None
            t_uadm = None

        # ── Render tabs ───────────────────────────────────────────────────────
        with t_work:
            if not df.empty and ws_title:
                pv  = filtered_df[filtered_df[COL_STATUS] != VAL_DONE].copy()
                pd_ = pv.copy(); pd_.index = pd_.index + 2
                render_worklist(pd_, df, headers, col_map, ws_title, f_binder, f_license)

        # [4] Guard: only render Archive if the tab exists (admin / manager)
        if t_arch is not None:
            with t_arch:
                if not df.empty and ws_title:
                    dv = filtered_df[filtered_df[COL_STATUS] == VAL_DONE].copy()
                    dv.index = dv.index + 2
                    render_archive(dv, df, col_map, ws_title, is_admin,
                                   col_binder=col_binder, col_license=col_license)

        if can_analytics and t_anal is not None:
            with t_anal:
                if not df.empty:
                    render_analytics(df, col_agent_email=col_agent_email, col_binder=col_binder)

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
