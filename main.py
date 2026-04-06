# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  ·  v14.0
#  Architecture: Optimistic UI / Local-First Mutation
#
#  Rule 1 — READ ONCE, NEVER BUST      (@st.cache_data ttl=600, zero .clear())
#  Rule 2 — OPTIMISTIC LOCAL MUTATION  (session_state.local_df, no re-fetch)
#  Rule 3 — EXPONENTIAL BACKOFF        (tenacity on every gspread call)
#
#  v14 UPDATES (on top of v13):
#    [A] REDESIGN: Google/Microsoft-style login — single centered card, no
#                  column breakage, clean #ffffff + #1a73e8 palette
#    [B] RBAC: 3-tier roles (admin / manager / auditor) stored in UsersDB
#              + role-change UI in User Admin tab
#    [C] DEEP SEARCH: binder / company / agent-email search widgets inside
#                     Analytics and Auditor Logs tabs; charts update live
#    [D] CHARTS: Stacked-bar "Eval Breakdown per Agent" + richer tooltips
#
#  Requirements: pip install streamlit gspread oauth2client pandas plotly pytz tenacity
# =============================================================================

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
    page_icon="🏛️",
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

EVAL_OPTIONS = [
    "🟢 Good (باش)",
    "🔴 Bad / Incorrect (خراپ)",
    "⚠️ Duplicate (دووبارە)",
]

# [B] Valid roles
VALID_ROLES = ["auditor", "manager", "admin"]

READ_TTL    = 600
BACKOFF_MAX = 5

# ─────────────────────────────────────────────────────────────────────────────
#  4 · EXPONENTIAL BACKOFF DECORATOR  (unchanged — core engine)
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
#  5 · CSS  — [A] Google-style login + all portal styles
# ─────────────────────────────────────────────────────────────────────────────
def inject_css() -> None:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
:root {
  --bg:           #F7F8FC;
  --surface:      #FFFFFF;
  --surface-2:    #F0F2F9;
  --border:       #E4E7F0;
  --border-2:     #D0D5E8;
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
  /* Google palette for login */
  --g-blue:     #1a73e8;
  --g-blue-hov: #1557b0;
  --g-border:   #dadce0;
  --g-text:     #202124;
  --g-muted:    #5f6368;
  --g-bg:       #f1f3f4;
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

/* ── [A] GOOGLE-STYLE LOGIN ─────────────────────────────────────────────── */
.glogin-root {
  position: fixed; inset: 0; z-index: 9999;
  background: #f1f3f4;
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  padding: 24px 16px;
}
.glogin-card {
  background: #ffffff;
  border: 1px solid #dadce0;
  border-radius: 8px;
  padding: 48px 40px 36px;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 2px 10px rgba(0,0,0,0.08);
  text-align: center;
}
.glogin-logo {
  width: 56px; height: 56px; margin: 0 auto 20px;
  background: #1a73e8;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 1.5rem;
  box-shadow: 0 2px 8px rgba(26,115,232,0.35);
}
.glogin-org {
  font-size: 0.68rem; font-weight: 600; letter-spacing: 0.12em;
  text-transform: uppercase; color: #5f6368 !important; margin-bottom: 6px;
}
.glogin-title {
  font-size: 1.5rem; font-weight: 700; color: #202124 !important;
  letter-spacing: -0.02em; margin-bottom: 6px; line-height: 1.2;
}
.glogin-subtitle {
  font-size: 0.84rem; color: #5f6368 !important; margin-bottom: 28px; font-weight: 400;
}
.glogin-divider {
  height: 1px; background: #e0e0e0; margin: 24px 0;
}
.glogin-lang-row {
  display: flex; gap: 8px; justify-content: center; margin-bottom: 20px;
}
.glogin-footer {
  margin-top: 24px; font-size: 0.72rem; color: #80868b !important;
  text-align: center; line-height: 1.6;
}
/* Override Streamlit button ONLY inside glogin context */
.glogin-btn-wrap .stButton > button {
  background: #1a73e8 !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 4px !important;
  font-weight: 600 !important;
  font-size: 0.92rem !important;
  padding: 12px 24px !important;
  width: 100% !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.20) !important;
  letter-spacing: 0.01em !important;
  transition: background 0.18s ease, box-shadow 0.18s ease !important;
}
.glogin-btn-wrap .stButton > button:hover {
  background: #1557b0 !important;
  box-shadow: 0 2px 8px rgba(26,115,232,0.40) !important;
  transform: none !important;
}
/* lang mini-buttons */
.glogin-lang-btn .stButton > button {
  background: transparent !important;
  color: #1a73e8 !important;
  border: 1px solid #dadce0 !important;
  border-radius: 4px !important;
  font-weight: 600 !important;
  font-size: 0.78rem !important;
  padding: 6px 18px !important;
  box-shadow: none !important;
  transition: background 0.14s ease !important;
}
.glogin-lang-btn .stButton > button:hover {
  background: #e8f0fe !important;
  transform: none !important;
  box-shadow: none !important;
}

/* ── [B] Role chips ─────────────────────────────────────────────────────── */
.chip-manager { background:#FFF7ED;color:#C2410C!important;border:1px solid #FED7AA; }
.role-badge-admin   { background:#EDE9FE;color:#6D28D9!important;border:1px solid #DDD6FE;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }
.role-badge-manager { background:#FFF7ED;color:#C2410C!important;border:1px solid #FED7AA;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }
.role-badge-auditor { background:#F0FDF4;color:#15803D!important;border:1px solid #A7F3D0;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;display:inline-block; }

/* ── [C] Deep-search strip ───────────────────────────────────────────────── */
.deep-search-strip {
  background: var(--surface);
  border: 1px solid var(--border);
  border-left: 4px solid var(--indigo-500);
  border-radius: var(--radius-md);
  padding: 16px 20px 8px;
  margin-bottom: 20px;
  box-shadow: var(--shadow-sm);
}
.deep-search-title {
  font-size: .62rem; font-weight: 800; letter-spacing: .14em;
  text-transform: uppercase; color: var(--indigo-600) !important;
  margin-bottom: 10px; display: flex; align-items: center; gap: 6px;
}
.search-active-badge {
  display:inline-flex;align-items:center;gap:4px;background:var(--indigo-50);
  color:var(--indigo-600)!important;border:1px solid var(--indigo-100);
  border-radius:var(--radius-full);font-size:.60rem;font-weight:800;
  letter-spacing:.06em;text-transform:uppercase;padding:2px 10px;margin-left:8px;
}

/* ── PAGE / SECTION (unchanged) ─────────────────────────────────────────── */
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
.lb-medal { font-size:1.1rem;width:26px;text-align:center; }
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

inject_css()

# ─────────────────────────────────────────────────────────────────────────────
#  6 · TRANSLATIONS
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry":"Ministry of Finance & Customs",
        "portal_title":"Tax Audit & Compliance Portal",
        "portal_sub":"Authorised Access Only",
        "classified":"CLASSIFIED — GOVERNMENT USE ONLY",
        "login_prompt":"Use your authorised credentials to access the system.",
        "email_field":"Official Email / User ID","password_field":"Password",
        "sign_in":"Sign in","sign_out":"Sign Out",
        "bad_creds":"Authentication failed. Verify your credentials and try again.",
        "language":"Interface Language",
        "workspace":"Active Case Register","overview":"Case Overview",
        "total":"Total Cases","processed":"Processed","outstanding":"Outstanding",
        "worklist_title":"Audit Worklist","worklist_sub":"Active cases pending review",
        "tab_worklist":"📋  Worklist","tab_archive":"✅  Archive",
        "tab_analytics":"📊  Analytics","tab_logs":"🗂️  Auditor Logs","tab_users":"⚙️  User Admin",
        "select_case":"Select a case to inspect","audit_trail":"Audit Trail",
        "approve_save":"Approve & Commit Record","reopen":"Re-open Record (Admin)",
        "leaderboard":"Auditor Productivity Leaderboard","daily_trend":"Daily Processing Trend",
        "period":"Time Period","today":"Today","this_week":"This Week",
        "this_month":"This Month","all_time":"All Time",
        "add_auditor":"Register New User","update_pw":"Update Password",
        "remove_user":"Revoke Access","staff_dir":"Authorised Staff",
        "no_records":"No records found for this period.",
        "empty_sheet":"This register contains no data.",
        "saved_ok":"✅ Record approved and committed. View updated instantly — sheet syncs within 10 min.",
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
        "retry_warning":"⏳ Google Sheets quota reached — retrying with backoff…",
        "local_mode":"Optimistic UI Active","cache_age":"Cache TTL",
        "rbac_notice":"ℹ️  Your role only has access to the Worklist and Archive.",
        "logs_title":"Auditor Activity Logs",
        "logs_sub":"Full processing history from project start",
        "logs_filter_all":"All Auditors",
        "logs_auditor_sel":"Filter by Auditor",
        "logs_total":"Total Processed",
        "logs_auditors":"Unique Auditors",
        "logs_date_range":"Date Range",
        "logs_no_data":"No processed records found.",
        "logs_export_hdr":"Export Full Report",
        "logs_export_sub":"Download the complete audit log as a CSV file.",
        "logs_export_btn":"⬇  Download CSV Report",
        "logs_filename":"audit_log_report.csv",
        "logs_cols_shown":"Columns displayed",
        "eval_label":"Data Entry Quality (کوالێتی داتا)",
        "feedback_label":"Auditor Feedback / Notes for Agent (تێبینی)",
        "feedback_placeholder":"Optional notes, issues found, corrections made…",
        "acc_ranking_title":"🏆 Data Entry Accuracy Ranking",
        "acc_agent":"Agent Email",
        "acc_total":"Total",
        "acc_good":"✅ Good",
        "acc_bad":"❌ Bad",
        "acc_dup":"⚠️ Dup",
        "acc_rate":"Accuracy %",
        "acc_no_data":"No evaluation data available yet.",
        "archive_quality_note":"💡 Tip: Columns Data_Evaluation & Correction_Notes are highlighted.",
        # [B]
        "role_label":"Role",
        "change_role":"Change User Role",
        "change_role_sub":"Upgrade or downgrade any user's access level",
        "role_updated":"✅ Role updated successfully.",
        # [C]
        "deep_search":"Deep Search",
        "ds_binder":"Filter by Binder No.",
        "ds_company":"Filter by Company",
        "ds_agent":"Filter by Agent Email",
        "ds_clear":"Clear Search",
        "ds_showing":"Showing filtered results for",
        # [D]
        "eval_breakdown":"📊 Evaluation Breakdown per Agent",
        "eval_breakdown_sub":"Stacked view of Good / Bad / Duplicate per data-entry agent",
    },
    "ku": {
        "ministry":"وەزارەتی دارایی و گومرگ",
        "portal_title":"پۆرتەلی فەرمی وردبینی باج و پابەندبوون",
        "portal_sub":"تەنها دەستپێگەیشتنی مەرجدارکراو",
        "classified":"نهێنی — تەنها بەکارهێنانی حکومی",
        "login_prompt":"زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email_field":"ئیمەیڵی فەرمی / ناساندن","password_field":"پاسۆرد",
        "sign_in":"چوونەژوورەوە","sign_out":"چوونەدەرەوە",
        "bad_creds":"ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "language":"زمانی ڕووکار",
        "workspace":"تۆماری کیسە چالاکەکان","overview":"کورتەی کیسەکان",
        "total":"کۆی کیسەکان","processed":"کارکراوە","outstanding":"ماوە",
        "worklist_title":"لیستی کاری وردبینی","worklist_sub":"کیسە چالاکەکانی چاوەڕوان",
        "tab_worklist":"📋  لیستی کاری","tab_archive":"✅  ئەرشیف",
        "tab_analytics":"📊  ئەنالیتیکس","tab_logs":"🗂️  لۆگی ئۆدیتۆر","tab_users":"⚙️  بەکارهێنەر",
        "select_case":"کیسێک هەڵبژێرە بۆ پشکنین","audit_trail":"مێژووی گۆڕانکاری",
        "approve_save":"پەسەندکردن و پاشەکەوتکردن","reopen":"کردنەوەی دووبارەی کیس (ئەدمین)",
        "leaderboard":"تەختەی بەرهەمهێنانی ئۆدیتۆر","daily_trend":"ترەندی ڕۆژانە",
        "period":"ماوەی کات","today":"ئەمڕۆ","this_week":"ئەم هەفتەیە",
        "this_month":"ئەم مانگەیە","all_time":"هەموو کات",
        "add_auditor":"تۆمارکردنی بەکارهێنەری نوێ","update_pw":"نوێکردنەوەی پاسۆرد",
        "remove_user":"هەڵوەشاندنەوەی دەستپێگەیشتن","staff_dir":"کارمەندە مەرجداركراوەکان",
        "no_records":"هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet":"ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok":"✅ کیسەکە پەسەندکرا. دیمەن نوێکرایەوە.",
        "dup_email":"ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields":"هەموو خانەکان پەیوەندییانە.",
        "signed_as":"چووییتە ژوورەوە بەناوی","role_admin":"بەڕێوەبەری سیستەم",
        "role_auditor":"ئۆدیتۆری باج","role_manager":"بەڕێوەبەر",
        "processing":"پشکنینی کیسی",
        "no_history":"هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period":"تۆمارەکان (ماوە)","active_days":"ڕۆژی چالاک","avg_per_day":"تێکڕای ڕۆژانە",
        "adv_filters":"فلتەرە پێشکەوتووەکان","f_email":"ئیمەیڵی ئۆدیتۆر",
        "f_binder":"ژمارەی بایندەری کۆمپانیا","f_company":"ناوی کۆمپانیا","f_license":"ژمارەی مۆڵەتی",
        "f_status":"دەربار","clear_filters":"سڕینەوەی فلتەرەکان",
        "active_filters":"فلتەرە چالاکەکان","results_shown":"ئەنجامی پیشاندراو",
        "no_match":"هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "status_all":"هەموو","status_pending":"چاوەڕوان تەنها","status_done":"کارکراو تەنها",
        "retry_warning":"⏳ کووتای گووگڵ شیت گەیشت — دووبارە هەوڵدەدرێت…",
        "local_mode":"Optimistic UI چالاکە","cache_age":"Cache TTL",
        "rbac_notice":"ℹ️  ڕۆڵەکەت تەنها دەستپێگەیشتن بە لیستی کاری و ئەرشیف هەیە.",
        "logs_title":"لۆگی چالاکی ئۆدیتۆرەکان",
        "logs_sub":"مێژووی تەواوی پرۆسەکردن",
        "logs_filter_all":"هەموو ئۆدیتۆرەکان",
        "logs_auditor_sel":"فلتەر بە ئۆدیتۆر",
        "logs_total":"کۆی گشتی کارکراو",
        "logs_auditors":"ژمارەی ئۆدیتۆرەکان",
        "logs_date_range":"ماوەی بەروار",
        "logs_no_data":"هیچ تۆماری کارکراوی نییە.",
        "logs_export_hdr":"هەناردەکردنی ڕاپۆرتی تەواو",
        "logs_export_sub":"ئەم لۆگە وەک فایلی CSV داگرە.",
        "logs_export_btn":"⬇  داگرتنی ڕاپۆرتی CSV",
        "logs_filename":"audit_log_report.csv",
        "logs_cols_shown":"ستوونەکانی پیشاندراو",
        "eval_label":"کوالێتی داتا (Data Entry Quality)",
        "feedback_label":"تێبینی ئۆدیتۆر / تێبینی بۆ ئەجنت",
        "feedback_placeholder":"تێبینی ئارەزوومەندانە، کێشەکان، سەرەستکردنەکان…",
        "acc_ranking_title":"🏆 رێزبەندی شیازی داخلکردنی داتا",
        "acc_agent":"ئیمەیڵی ئەجنت",
        "acc_total":"کۆی گشتی",
        "acc_good":"✅ باش",
        "acc_bad":"❌ خراپ",
        "acc_dup":"⚠️ دووبارە",
        "acc_rate":"ڕێژەی شیازی %",
        "acc_no_data":"هیچ داتای هەڵسەنگاندنی بەردەست نییە.",
        "archive_quality_note":"💡 تێبینی: ستوونەکانی Data_Evaluation و Correction_Notes نیشاندراون.",
        "role_label":"ڕۆڵ",
        "change_role":"گۆڕینی ڕۆڵی بەکارهێنەر",
        "change_role_sub":"بەرزکردنەوە یان دابەزاندنی ئاستی دەستپێگەیشتن",
        "role_updated":"✅ ڕۆڵەکە بە سەرکەوتوویی نوێکرایەوە.",
        "deep_search":"گەڕانی قووڵ",
        "ds_binder":"فلتەر بە ژمارەی بایندەر",
        "ds_company":"فلتەر بە ناوی کۆمپانیا",
        "ds_agent":"فلتەر بە ئیمەیڵی ئەجنت",
        "ds_clear":"سڕینەوەی گەڕان",
        "ds_showing":"پیشاندانی ئەنجامی فلتەرکراو بۆ",
        "eval_breakdown":"📊 داڕشتنی هەڵسەنگاندن بەپێی ئەجنت",
        "eval_breakdown_sub":"دیمەنی خورەکی باش / خراپ / دووبارە بەپێی ئەجنتی داخلکردنی داتا",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)


# ─────────────────────────────────────────────────────────────────────────────
#  7 · HELPERS  (unchanged core engine)
# ─────────────────────────────────────────────────────────────────────────────
_COL_KEYWORDS: dict[str, list[str]] = {
    "binder":  ["رقم ملف الشركة","رقم_ملف_الشركة","رقم ملف","ملف الشركة",
                "ژمارەی بایندەری کۆمپانیا","ژمارەی بایندەری","بایندەری",
                "binder","file no","file_no"],
    "company": ["اسم الشركة","اسم_الشركة","اسم الشركه","کۆمپانیای","كومبانيا","شركة",
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

def _n_active(fe, fb, fc_, fl, fs):
    return sum([bool(fe.strip()), bool(fb.strip()), bool(fc_.strip()), bool(fl.strip()), fs != "all"])

def apply_filters_locally(df, f_email, f_binder, f_company, f_license, f_status,
                          col_binder, col_company, col_license):
    r = df.copy()
    if f_status == "pending": r = r[r[COL_STATUS] != VAL_DONE]
    elif f_status == "done":  r = r[r[COL_STATUS] == VAL_DONE]
    if f_email.strip():
        ecols = [c for c in r.columns if "auditor_email" in c.lower() or c == COL_AUDITOR]
        if ecols:
            mask = pd.Series(False, index=r.index)
            for ec in ecols: mask |= r[ec].str.contains(f_email.strip(), case=False, na=False)
            r = r[mask]
    if f_binder.strip()  and col_binder  and col_binder  in r.columns: r = r[r[col_binder].str.contains(f_binder.strip(),  case=False, na=False)]
    if f_company.strip() and col_company and col_company in r.columns: r = r[r[col_company].str.contains(f_company.strip(), case=False, na=False)]
    if f_license.strip() and col_license and col_license in r.columns: r = r[r[col_license].str.contains(f_license.strip(), case=False, na=False)]
    return r

def build_auto_diff(record: dict, new_vals: dict) -> str:
    lines = []
    for field, new_v in new_vals.items():
        old_v = clean_cell(record.get(field, ""))
        new_v_clean = clean_cell(new_v)
        if old_v != new_v_clean:
            ov = old_v[:60] + "…" if len(old_v) > 60 else old_v
            nv = new_v_clean[:60] + "…" if len(new_v_clean) > 60 else new_v_clean
            lines.append(f"[{field}]: '{ov}' → '{nv}'")
    if lines: return "Auto-Log:\n" + "\n".join(lines)
    return "Auto-Log: No field changes detected."


# ─────────────────────────────────────────────────────────────────────────────
#  8 · GOOGLE SHEETS  (unchanged core engine)
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
#  9 · OPTIMISTIC MUTATIONS  (unchanged core engine)
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
#  10 · WRITE HELPERS  (unchanged core engine)
# ─────────────────────────────────────────────────────────────────────────────
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
                            eval_val: str = "", feedback_val: str = ""):
    ws = get_spreadsheet().worksheet(ws_title)
    headers, col_map = ensure_system_cols_in_sheet(ws, headers, col_map)
    old = str(record.get(COL_LOG, "")).strip()
    new_log = f"{log_prefix}\n{old}".strip()
    batch = []
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
    if batch: _gsheets_call(ws.batch_update, batch)

def write_reopen_to_sheet(ws_title, sheet_row, col_map):
    ws = get_spreadsheet().worksheet(ws_title)
    if COL_STATUS in col_map:
        _gsheets_call(ws.update_cell, sheet_row, col_map[COL_STATUS], VAL_PENDING)


# ─────────────────────────────────────────────────────────────────────────────
#  [B] AUTHENTICATE — 3-tier RBAC
# ─────────────────────────────────────────────────────────────────────────────
def authenticate(email: str, password: str, spreadsheet_id: str):
    """Returns 'admin' | 'manager' | 'auditor' | None."""
    email = email.lower().strip()
    # hardcoded admin bypass (unchanged)
    if email == "admin" and password == st.secrets.get("admin_password", ""):
        return "admin"
    try:
        records = _fetch_users_cached(spreadsheet_id)
        df_u = pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return None
        row = df_u[df_u["email"] == email]
        if row.empty: return None
        if hash_pw(password) != str(row["password"].values[0]): return None
        # resolve role — default to 'auditor' for legacy rows without role col
        role = "auditor"
        if "role" in df_u.columns:
            r = str(row["role"].values[0]).strip().lower()
            if r in VALID_ROLES: role = r
        return role
    except:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  11 · HTML TABLE
# ─────────────────────────────────────────────────────────────────────────────
def _eval_chip(raw: str) -> str:
    if not raw or raw == "—": return "—"
    if "🟢" in raw or "Good" in raw:
        return f"<span class='s-chip s-eval-good'>{raw}</span>"
    if "🔴" in raw or "Bad" in raw or "Incorrect" in raw:
        return f"<span class='s-chip s-eval-bad'>{raw}</span>"
    if "⚠️" in raw or "Duplicate" in raw or "دووبارە" in raw:
        return f"<span class='s-chip s-eval-dup'>{raw}</span>"
    return f"<span class='s-chip s-pending'>{raw}</span>"

def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty: st.info("No records to display."); return
    display_df = df.head(max_rows)
    th = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col == COL_LOG: continue
        extra_cls = ""
        if col == COL_EVAL:       extra_cls = " class='col-eval'"
        elif col == COL_FEEDBACK: extra_cls = " class='col-feedback'"
        th += f"<th{extra_cls}>{col}</th>"
    rows = ""
    for idx, row in display_df.iterrows():
        r = f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col == COL_LOG: continue
            raw = str(row[col]) if row[col] != "" else ""; d = raw or "—"
            if col == COL_STATUS:
                d = ("<span class='s-chip s-done'>✓ Processed</span>" if raw == VAL_DONE
                     else "<span class='s-chip s-pending'>⏳ Pending</span>")
            elif col == COL_EVAL:
                d = _eval_chip(raw)
                r += f"<td class='col-eval'>{d}</td>"
                continue
            elif col == COL_FEEDBACK:
                d = raw[:160] + "…" if len(raw) > 160 else (raw or "—")
                r += f"<td class='col-feedback'>{d}</td>"
                continue
            elif len(raw) > 55:
                d = f"<span title='{raw}'>{raw[:52]}…</span>"
            r += f"<td>{d}</td>"
        rows += f"<tr>{r}</tr>"
    st.markdown(
        f"<div class='gov-table-wrap'><table class='gov-table'>"
        f"<thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>",
        unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  12 · LOGIN  — [A] Google Workspace style, single centered card
# ─────────────────────────────────────────────────────────────────────────────
def render_login(spreadsheet_id: str) -> None:
    # Hide sidebar and set clean gray background
    st.markdown("""<style>
    [data-testid="stSidebar"],[data-testid="collapsedControl"]{display:none!important;}
    html,body,.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],
    .main,.block-container{
      background:#f1f3f4!important;
      min-height:100vh!important;
      padding:0!important;
      max-width:100%!important;
    }
    .block-container{padding-top:0!important;padding-bottom:0!important;}
    </style>""", unsafe_allow_html=True)

    # Language toggles — rendered above the card via a narrow centered strip
    lang_c1, lang_c2, lang_c3 = st.columns([3, 0.18, 0.18])
    with lang_c2:
        st.markdown("<div class='glogin-lang-btn'>", unsafe_allow_html=True)
        if st.button("EN", key="lg_en"): st.session_state.lang = "en"; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    with lang_c3:
        st.markdown("<div class='glogin-lang-btn'>", unsafe_allow_html=True)
        if st.button("KU", key="lg_ku"): st.session_state.lang = "ku"; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # Single centered card
    _, mid, _ = st.columns([1, 1.4, 1])
    with mid:
        # Card shell (visual only)
        st.markdown(f"""
        <div style="padding: 32px 0 0;">
          <div class="glogin-card">
            <div class="glogin-logo">🏛️</div>
            <div class="glogin-org">{t('ministry')}</div>
            <div class="glogin-title">{t('sign_in')}</div>
            <div class="glogin-subtitle">{t('login_prompt')}</div>
            <div style="margin-bottom:4px;">
              <span style="display:inline-block;background:#fce8e6;color:#c5221f;
                border:1px solid #f5c6cb;border-radius:4px;font-size:.60rem;
                font-weight:700;letter-spacing:.12em;text-transform:uppercase;
                padding:4px 12px;">🔒 {t('classified')}</span>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)

        # Streamlit form — sits visually inside the card thanks to CSS
        with st.container():
            st.markdown("""
            <style>
            div[data-testid="stForm"] {
              border:none!important; background:transparent!important;
              box-shadow:none!important; padding:0 40px 36px!important;
              margin-top:-8px!important; border-radius:0 0 8px 8px!important;
            }
            </style>
            <div style="background:#ffffff; border:1px solid #dadce0; border-top:none;
                 border-radius:0 0 8px 8px; padding:0 40px 36px; margin-top:-4px;
                 box-shadow:0 2px 10px rgba(0,0,0,0.08);">
            """, unsafe_allow_html=True)

            with st.form("login_form", clear_on_submit=False):
                st.text_input(t("email_field"), placeholder="user@mof.gov.iq", key="_login_email")
                st.text_input(t("password_field"), type="password", placeholder="••••••••••", key="_login_pw")
                st.markdown("<div class='glogin-btn-wrap'>", unsafe_allow_html=True)
                submitted = st.form_submit_button(f"🔐  {t('sign_in')}", use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("""
              <div class="glogin-footer">
                Authorised personnel only · All access is logged and audited<br>
                Ministry of Finance & Customs — Internal System
              </div>
            </div>
            """, unsafe_allow_html=True)

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
        for k in ("f_email", "f_binder", "f_company", "f_license"):
            st.session_state[k] = ""
        st.session_state["f_status"] = "all"

    role        = st.session_state.user_role
    role_label  = {"admin": t("role_admin"), "manager": t("role_manager"),
                   "auditor": t("role_auditor")}.get(role, role.title())
    badge_cls   = {"admin": "role-badge-admin", "manager": "role-badge-manager",
                   "auditor": "role-badge-auditor"}.get(role, "role-badge-auditor")

    with st.sidebar:
        st.markdown(f"""
        <div class="sidebar-header">
          <div class="sidebar-logo-text">🏛️&nbsp; {t('portal_title')}</div>
          <div class="sidebar-ministry">{t('ministry')}</div>
        </div>
        <hr class="divider" style="margin:0;"/>""", unsafe_allow_html=True)
        st.markdown(f"""
        <div class="cache-strip">
          <span class="cache-badge">⚡ {t('local_mode')}</span>
          <div class="cache-info">{t('cache_age')}: {READ_TTL//60} min · Last sync: {fetched_at[-8:] if fetched_at else '—'}</div>
        </div>""", unsafe_allow_html=True)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"): st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"): st.session_state.lang = "ku"; st.rerun()
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='adv-filter-header'>🔍 {t('adv_filters')}</div>", unsafe_allow_html=True)
        status_opts = {"all": t("status_all"), "pending": t("status_pending"), "done": t("status_done")}
        st.selectbox(t("f_status"), options=list(status_opts.keys()),
                     format_func=lambda k: status_opts[k], key="f_status")
        for key, label, hint, disabled in [
            ("f_email",   t("f_email"),   COL_AUDITOR,           False),
            ("f_binder",  t("f_binder"),  col_binder  or "—",    col_binder  is None),
            ("f_company", t("f_company"), col_company or "—",    col_company is None),
            ("f_license", t("f_license"), col_license or "—",    col_license is None),
        ]:
            st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{label}"
                        f"<span class='col-hint'> ({hint})</span></div>", unsafe_allow_html=True)
            st.text_input(label, key=key, disabled=disabled, label_visibility="collapsed")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.button(f"✕  {t('clear_filters')}", use_container_width=True,
                  key="clr_f", on_click=clear_all_filters)
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"""
        <div class="sb-user-card">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="{badge_cls}" style="margin-top:8px;">{role_label}</span>
        </div>""", unsafe_allow_html=True)
        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k, v in _DEFAULTS.items(): st.session_state[k] = v
            st.rerun()
    return (st.session_state.get("f_email", ""), st.session_state.get("f_binder", ""),
            st.session_state.get("f_company", ""), st.session_state.get("f_license", ""),
            st.session_state.get("f_status", "all"))

def render_filter_bar(total, filtered, f_email, f_binder, f_company, f_license, f_status):
    n = _n_active(f_email, f_binder, f_company, f_license, f_status)
    if n == 0: return
    badges = ""
    if f_status != "all":   badges += f"<span class='filter-badge'>⚡ {f_status}</span> "
    if f_email.strip():     badges += f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span> "
    if f_binder.strip():    badges += f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span> "
    if f_company.strip():   badges += f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span> "
    if f_license.strip():   badges += f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span> "
    st.markdown(f"""<div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:800;color:var(--indigo-600);text-transform:uppercase;letter-spacing:.08em;">
        {t('active_filters')} ({n})</span> {badges}
      <span class="result-count"><strong style="color:var(--indigo-600);">{filtered}</strong>/{total}&nbsp;{t('results_shown')}</span>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  [C] DEEP SEARCH WIDGET — reusable strip used in Analytics & Logs
# ─────────────────────────────────────────────────────────────────────────────
def render_deep_search_strip(key_prefix: str, col_binder, col_company, col_agent_email):
    """Renders search inputs; returns (srch_binder, srch_company, srch_agent)."""
    
    # ⚡ 1. دروستکردنی فەنکشنی سڕینەوەکە (Callback) ⚡
    def clear_search():
        st.session_state[f"{key_prefix}_binder"] = ""
        st.session_state[f"{key_prefix}_company"] = ""
        st.session_state[f"{key_prefix}_agent"] = ""

    st.markdown(
        f"<div class='deep-search-strip'>"
        f"<div class='deep-search-title'>🔬 {t('deep_search')}"
        f"<span style='font-size:.60rem;font-weight:400;text-transform:none;letter-spacing:0;"
        f"color:var(--text-muted);'>— {t('ds_showing')} binder / company / agent</span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )
    
    c1, c2, c3, c4 = st.columns([1, 1, 1, 0.38])
    with c1:
        srch_binder = st.text_input(
            t("ds_binder"),
            key=f"{key_prefix}_binder",
            placeholder="e.g. 10234",
            disabled=(col_binder is None),
        )
    with c2:
        srch_company = st.text_input(
            t("ds_company"),
            key=f"{key_prefix}_company",
            placeholder="e.g. Baghdad Trading",
            disabled=(col_company is None),
        )
    with c3:
        srch_agent = st.text_input(
            t("ds_agent"),
            key=f"{key_prefix}_agent",
            placeholder="e.g. agent@example.com",
            disabled=(col_agent_email is None),
        )
    with c4:
        st.markdown("<div style='margin-top:22px;'>", unsafe_allow_html=True)
        # ⚡ 2. بەکارهێنانی on_click لەناو دوگمەکە ⚡
        st.button(f"✕ {t('ds_clear')}", key=f"{key_prefix}_clr", use_container_width=True, on_click=clear_search)
        st.markdown("</div>", unsafe_allow_html=True)

    return (
        st.session_state.get(f"{key_prefix}_binder", ""),
        st.session_state.get(f"{key_prefix}_company", ""),
        st.session_state.get(f"{key_prefix}_agent", ""),
    )

def apply_deep_search(df: pd.DataFrame, srch_binder: str, srch_company: str,
                      srch_agent: str, col_binder, col_company, col_agent_email) -> pd.DataFrame:
    """Filter df using the three deep-search inputs."""
    r = df.copy()
    if srch_binder.strip() and col_binder and col_binder in r.columns:
        r = r[r[col_binder].str.contains(srch_binder.strip(), case=False, na=False)]
    if srch_company.strip() and col_company and col_company in r.columns:
        r = r[r[col_company].str.contains(srch_company.strip(), case=False, na=False)]
    if srch_agent.strip() and col_agent_email and col_agent_email in r.columns:
        r = r[r[col_agent_email].str.contains(srch_agent.strip(), case=False, na=False)]
    return r


def _deep_search_active(b, c, a) -> bool:
    return any(x.strip() for x in (b, c, a))


# ─────────────────────────────────────────────────────────────────────────────
#  14 · WORKLIST
# ─────────────────────────────────────────────────────────────────────────────
def render_worklist(pending_display, df, headers, col_map, ws_title,
                    f_email, f_binder, f_company, f_license, f_status):
    p_count = len(pending_display)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">📋 {t('worklist_title')}</div>
      <div class="worklist-sub">{t('worklist_sub')}</div></div>
      <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
    </div>""", unsafe_allow_html=True)
    if pending_display.empty:
        msg = (t("no_match") if _n_active(f_email, f_binder, f_company, f_license, f_status)
               else "✅  All cases processed.")
        st.info(msg); return
    render_html_table(pending_display)
    st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>", unsafe_allow_html=True)
    label_col = next((h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row")
    opts = ["—"] + [f"Row {idx}  ·  {str(row.get(label_col,''))[:55]}"
                    for idx, row in pending_display.iterrows()]
    row_sel = st.selectbox("", opts, key="row_sel", label_visibility="collapsed")
    if row_sel == "—": return
    sheet_row = int(row_sel.split("  ·  ")[0].replace("Row ", "").strip())
    df_iloc   = sheet_row - 2
    if df_iloc < 0 or df_iloc >= len(df): st.error("Row index out of range."); return
    record = df.iloc[df_iloc].to_dict()
    with st.expander(f"📜  {t('audit_trail')}", expanded=False):
        history = str(record.get(COL_LOG, "")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip(): st.markdown(f'<div class="log-line">{line}</div>', unsafe_allow_html=True)
        else: st.caption(t("no_history"))
    st.markdown(f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>", unsafe_allow_html=True)
    SKIP = set(SYSTEM_COLS); fields = {k: v for k, v in record.items() if k not in SKIP}

    with st.form("audit_form"):
        new_vals = {}
        for fname, fval in fields.items():
            new_vals[fname] = st.text_input(fname, value=clean_cell(fval), key=f"field_{fname}")
        st.markdown("<hr style='border-top:1px dashed var(--border);margin:18px 0 14px;'/>",
                    unsafe_allow_html=True)
        eval_val = st.selectbox(t("eval_label"), options=EVAL_OPTIONS, index=0, key="form_eval")
        manual_notes = st.text_area(t("feedback_label"), placeholder=t("feedback_placeholder"),
                                    key="form_feedback", height=100)
        do_submit = st.form_submit_button(f"✅  {t('approve_save')}", use_container_width=True)

    if do_submit:
        ts_now     = now_str()
        auditor    = st.session_state.user_email
        log_prefix = f"✔  {auditor}  |  {ts_now}"
        auto_diff  = build_auto_diff(record, new_vals)
        feedback_combined = (f"{manual_notes.strip()}\n{auto_diff}".strip()
                             if manual_notes.strip() else auto_diff)
        with st.spinner("Committing record to Google Sheets…"):
            try:
                write_approval_to_sheet(ws_title, sheet_row, col_map, headers,
                    new_vals, record, auditor, ts_now, log_prefix,
                    eval_val=eval_val, feedback_val=feedback_combined)
            except gspread.exceptions.APIError as e:
                st.error(f"🚨 Write failed: {e}"); return
        _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix,
                                  eval_val=eval_val, feedback_val=feedback_combined)
        st.success(t("saved_ok")); time.sleep(0.6); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  15 · ARCHIVE
# ─────────────────────────────────────────────────────────────────────────────
def render_archive(done_view, df, col_map, ws_title, is_admin,
                   f_email, f_binder, f_company, f_license, f_status):
    d_count = len(done_view)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">✅ Processed Archive</div>
      <div class="worklist-sub">Completed and committed audit records</div></div>
      <span class="chip chip-done">{d_count} {t('processed')}</span>
    </div>""", unsafe_allow_html=True)
    if done_view.empty:
        st.info(t("no_match") if _n_active(f_email, f_binder, f_company, f_license, f_status)
                else "No processed records yet.")
    else:
        if is_admin:
            st.markdown(
                f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
                f"border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);"
                f"padding:10px 16px;margin-bottom:14px;font-size:.78rem;"
                f"color:var(--indigo-600)!important;font-weight:600;'>"
                f"{t('archive_quality_note')}</div>", unsafe_allow_html=True)
        priority_cols = [COL_STATUS, COL_EVAL, COL_FEEDBACK, COL_AUDITOR, COL_DATE]
        other_cols    = [c for c in done_view.columns if c not in priority_cols and c != COL_LOG]
        ordered_cols  = [c for c in priority_cols if c in done_view.columns] + other_cols
        render_html_table(done_view[ordered_cols], max_rows=500)

    if is_admin and not done_view.empty:
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='section-title'>↩️ {t('reopen')}</div>", unsafe_allow_html=True)
        ropts = ["—"] + [f"Row {idx}" for idx in done_view.index]
        rsel  = st.selectbox("Select record to re-open:", ropts, key="reopen_sel")
        if rsel != "—":
            ridx = int(rsel.split(" ")[1]); df_iloc = ridx - 2
            if st.button(t("reopen"), key="reopen_btn"):
                with st.spinner("Re-opening…"):
                    try:    write_reopen_to_sheet(ws_title, ridx, col_map)
                    except gspread.exceptions.APIError as e: st.error(f"🚨 {e}"); return
                _apply_optimistic_reopen(df_iloc); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  16 · ANALYTICS  — [C] deep search  [D] stacked bar + richer tooltips
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(
    df: pd.DataFrame,
    col_agent_email: str | None = None,
    col_binder: str | None = None,
    col_company: str | None = None,
) -> None:
    pt = "plotly_white"; pb = "#FFFFFF"; pg = "#E4E7F0"; fc = "#0D1117"
    nvy = "#4F46E5"; blu = "#60A5FA"

    # ── [C] Deep Search ───────────────────────────────────────────────────────
    srch_binder, srch_company, srch_agent = render_deep_search_strip(
        "anal", col_binder, col_company, col_agent_email)

    work_df = apply_deep_search(df, srch_binder, srch_company, srch_agent,
                                col_binder, col_company, col_agent_email)

    if _deep_search_active(srch_binder, srch_company, srch_agent):
        terms = [x for x in (srch_binder, srch_company, srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
            f"border-radius:var(--radius-md);padding:9px 16px;margin-bottom:14px;"
            f"font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>"
            f"🔍 {t('ds_showing')} <strong>{' · '.join(terms)}</strong> "
            f"— <strong>{len(work_df)}</strong> records matched"
            f"</div>", unsafe_allow_html=True)

    # ── Period filter ─────────────────────────────────────────────────────────
    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>", unsafe_allow_html=True)
    periods = [("all", t("all_time")), ("today", t("today")),
               ("this_week", t("this_week")), ("this_month", t("this_month"))]
    for cw, (pk, pl) in zip(st.columns(len(periods)), periods):
        lbl = f"✓  {pl}" if st.session_state.date_filter == pk else pl
        if cw.button(lbl, use_container_width=True, key=f"pf_{pk}"):
            st.session_state.date_filter = pk; st.rerun()

    done_base = work_df[work_df[COL_STATUS] == VAL_DONE].copy()
    done_f    = apply_period_filter(done_base, COL_DATE, st.session_state.date_filter)
    if done_f.empty: st.info(t("no_records")); return

    ma, mb, mc = st.columns(3); ma.metric(t("records_period"), len(done_f))
    active = 0
    if COL_DATE in done_f.columns:
        active = done_f[COL_DATE].apply(lambda s: parse_dt(s).date() if parse_dt(s) else None).nunique()
    mb.metric(t("active_days"), active)
    mc.metric(t("avg_per_day"), f"{len(done_f)/max(active,1):.1f}")

    left, right = st.columns([1, 1.6], gap="large")

    # ── Leaderboard ───────────────────────────────────────────────────────────
    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>", unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = done_f[COL_AUDITOR].replace("", "—").value_counts().reset_index()
            lb.columns = ["Auditor", "Count"]
            medals = ["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i, r in lb.head(10).iterrows():
                m = medals[i] if i < len(medals) else f"{i+1}."
                st.markdown(f'<div class="lb-row"><span class="lb-medal">{m}</span>'
                            f'<span class="lb-name">{r["Auditor"]}</span>'
                            f'<span class="lb-count">{r["Count"]}</span></div>',
                            unsafe_allow_html=True)
            fig = px.bar(lb.head(10), x="Count", y="Auditor", orientation="h",
                         color="Count", color_continuous_scale=[blu, nvy], template=pt,
                         hover_data={"Count": True, "Auditor": True})
            fig.update_traces(
                marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>Records processed: <b>%{x}</b><extra></extra>",
            )
            fig.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color="#4B5563")),
                height=min(320, max(180, 36*len(lb.head(10)))))
            st.plotly_chart(fig, use_container_width=True)

    # ── Daily trend ───────────────────────────────────────────────────────────
    with right:
        st.markdown(f"<div class='section-title'>📈 {t('daily_trend')}</div>", unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f = done_f.copy()
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
                fig2.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"], mode="none",
                    fill="tozeroy", fillcolor="rgba(99,102,241,0.07)", showlegend=False))
                fig2.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="lines+markers",
                    line=dict(color=nvy, width=2.5),
                    marker=dict(color=blu, size=7, line=dict(color="#FFFFFF", width=2)),
                    name=t("records_period"),
                    hovertemplate="<b>%{x}</b><br>Records: <b>%{y}</b><extra></extra>",
                ))
                fig2.update_layout(
                    template=pt, paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                    showlegend=False, margin=dict(l=8,r=8,t=10,b=8),
                    xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                    yaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#4B5563")),
                    height=380, hovermode="x unified")
                st.plotly_chart(fig2, use_container_width=True)
            else: st.info(t("no_records"))

    # ── Accuracy ranking table ────────────────────────────────────────────────
    st.markdown(f"<div class='section-title'>{t('acc_ranking_title')}</div>", unsafe_allow_html=True)

    if col_agent_email and col_agent_email in done_f.columns and COL_EVAL in done_f.columns:
        eval_df = done_f[[col_agent_email, COL_EVAL]].copy()
        eval_df[col_agent_email] = eval_df[col_agent_email].replace("", "—")

        def _classify(v: str) -> str:
            if "🟢" in v or "Good" in v:                          return "good"
            if "🔴" in v or "Bad" in v or "Incorrect" in v:       return "bad"
            if "⚠️" in v or "Duplicate" in v or "دووبارە" in v:   return "dup"
            return "unrated"

        eval_df["_cls"] = eval_df[COL_EVAL].apply(_classify)
        acc = (eval_df.groupby(col_agent_email)["_cls"]
               .value_counts().unstack(fill_value=0).reset_index())
        for col_need in ("good","bad","dup","unrated"):
            if col_need not in acc.columns: acc[col_need] = 0
        acc["Total"]    = acc["good"] + acc["bad"] + acc["dup"] + acc["unrated"]
        acc["Accuracy"] = (acc["good"] / acc["Total"].replace(0,1) * 100).round(1)
        acc = acc.sort_values("Accuracy", ascending=False).reset_index(drop=True)

        th_row = (
            f"<tr><th>#</th><th>{t('acc_agent')}</th><th>{t('acc_total')}</th>"
            f"<th>{t('acc_good')}</th><th>{t('acc_bad')}</th><th>{t('acc_dup')}</th>"
            f"<th>{t('acc_rate')}</th></tr>"
        )
        td_rows = ""
        for i, row in acc.iterrows():
            pct = row["Accuracy"]
            if pct >= 80:   rate_cls = "acc-rate-high"; bar_col = "#16A34A"
            elif pct >= 50: rate_cls = "acc-rate-mid";  bar_col = "#B45309"
            else:           rate_cls = "acc-rate-low";  bar_col = "#DC2626"
            bar_fill = int(pct)
            bar_html = (f"<span class='acc-bar-wrap'>"
                        f"<span class='acc-bar-fill' style='width:{bar_fill}%;background:{bar_col};display:block;'></span>"
                        f"</span>")
            td_rows += (
                f"<tr>"
                f"<td style='color:var(--text-muted);font-family:var(--mono);font-size:.70rem;'>{i+1}</td>"
                f"<td style='font-weight:600;'>{row[col_agent_email]}</td>"
                f"<td style='font-family:var(--mono);font-weight:700;'>{int(row['Total'])}</td>"
                f"<td><span class='s-chip s-eval-good'>{int(row['good'])}</span></td>"
                f"<td><span class='s-chip s-eval-bad'>{int(row['bad'])}</span></td>"
                f"<td><span class='s-chip s-eval-dup'>{int(row['dup'])}</span></td>"
                f"<td class='{rate_cls}'>{pct}% {bar_html}</td>"
                f"</tr>"
            )
        st.markdown(
            f"<div class='gov-table-wrap'><table class='acc-table'>"
            f"<thead>{th_row}</thead><tbody>{td_rows}</tbody></table></div>",
            unsafe_allow_html=True)

        # ── [D] Stacked bar chart — eval breakdown per agent ──────────────────
        st.markdown(f"<div class='section-title'>{t('eval_breakdown')}</div>",
                    unsafe_allow_html=True)
        st.caption(t("eval_breakdown_sub"))

        if not acc.empty:
            fig_stack = go.Figure()

            # Build rich hover info: show pct of each category
            good_pct  = (acc["good"]    / acc["Total"].replace(0, 1) * 100).round(1)
            bad_pct   = (acc["bad"]     / acc["Total"].replace(0, 1) * 100).round(1)
            dup_pct   = (acc["dup"]     / acc["Total"].replace(0, 1) * 100).round(1)
            unr_pct   = (acc["unrated"] / acc["Total"].replace(0, 1) * 100).round(1)

            fig_stack.add_trace(go.Bar(
                name="✅ Good",
                x=acc[col_agent_email],
                y=acc["good"],
                marker_color="#16A34A",
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "✅ Good: <b>%{y}</b> (%{customdata[0]}%)<br>"
                    "Total records: %{customdata[1]}<extra></extra>"
                ),
                customdata=list(zip(good_pct, acc["Total"])),
            ))
            fig_stack.add_trace(go.Bar(
                name="❌ Bad / Incorrect",
                x=acc[col_agent_email],
                y=acc["bad"],
                marker_color="#DC2626",
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "❌ Bad: <b>%{y}</b> (%{customdata[0]}%)<br>"
                    "Total records: %{customdata[1]}<extra></extra>"
                ),
                customdata=list(zip(bad_pct, acc["Total"])),
            ))
            fig_stack.add_trace(go.Bar(
                name="⚠️ Duplicate",
                x=acc[col_agent_email],
                y=acc["dup"],
                marker_color="#F59E0B",
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "⚠️ Duplicate: <b>%{y}</b> (%{customdata[0]}%)<br>"
                    "Total records: %{customdata[1]}<extra></extra>"
                ),
                customdata=list(zip(dup_pct, acc["Total"])),
            ))
            if acc["unrated"].sum() > 0:
                fig_stack.add_trace(go.Bar(
                    name="— Unrated",
                    x=acc[col_agent_email],
                    y=acc["unrated"],
                    marker_color="#9CA3AF",
                    hovertemplate=(
                        "<b>%{x}</b><br>"
                        "— Unrated: <b>%{y}</b> (%{customdata}%)<extra></extra>"
                    ),
                    customdata=unr_pct,
                ))

            fig_stack.update_layout(
                barmode="stack",
                template=pt,
                paper_bgcolor=pb,
                plot_bgcolor=pb,
                font=dict(family="Plus Jakarta Sans", color=fc, size=11),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1,
                    font=dict(size=11),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="#E4E7F0",
                    borderwidth=1,
                ),
                margin=dict(l=8, r=8, t=40, b=60),
                xaxis=dict(
                    gridcolor=pg, zeroline=False,
                    tickfont=dict(color="#4B5563"),
                    tickangle=-30,
                    title=dict(text="Agent", font=dict(size=11, color="#4B5563")),
                ),
                yaxis=dict(
                    gridcolor=pg, zeroline=False,
                    tickfont=dict(color="#4B5563"),
                    title=dict(text="Records", font=dict(size=11, color="#4B5563")),
                ),
                height=400,
                hovermode="x",
            )
            fig_stack.update_traces(marker_line_width=0)
            st.plotly_chart(fig_stack, use_container_width=True)

    else:
        st.info(t("acc_no_data")
                + ("" if col_agent_email else
                   " (Agent Email column not detected — check sheet headers.)"))


# ─────────────────────────────────────────────────────────────────────────────
#  17 · AUDITOR LOGS  — [C] deep search added
# ─────────────────────────────────────────────────────────────────────────────
def render_auditor_logs(
    df: pd.DataFrame,
    col_company: str | None,
    col_binder: str | None,
    col_agent_email: str | None = None,
) -> None:
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">🗂️ {t('logs_title')}</div>
        <div class="worklist-sub">{t('logs_sub')}</div>
      </div>
      <span class="chip chip-admin">Admin / Manager</span>
    </div>""", unsafe_allow_html=True)

    # ── [C] Deep Search ───────────────────────────────────────────────────────
    srch_binder, srch_company, srch_agent = render_deep_search_strip(
        "logs", col_binder, col_company, col_agent_email)

    done_df = df[df[COL_STATUS] == VAL_DONE].copy()
    if done_df.empty:
        st.info(t("logs_no_data")); return

    # apply deep search to the done set
    done_df = apply_deep_search(done_df, srch_binder, srch_company, srch_agent,
                                col_binder, col_company, col_agent_email)
    if _deep_search_active(srch_binder, srch_company, srch_agent):
        terms = [x for x in (srch_binder, srch_company, srch_agent) if x.strip()]
        st.markdown(
            f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);"
            f"border-radius:var(--radius-md);padding:9px 16px;margin-bottom:14px;"
            f"font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>"
            f"🔍 {t('ds_showing')} <strong>{' · '.join(terms)}</strong> "
            f"— <strong>{len(done_df)}</strong> records matched</div>",
            unsafe_allow_html=True)

    if done_df.empty:
        st.info(t("logs_no_data")); return

    display_cols: list[str] = [COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]
    if col_company and col_company in done_df.columns:
        display_cols.insert(1, col_company)
    if col_binder and col_binder in done_df.columns:
        display_cols.insert(1, col_binder)
    if col_agent_email and col_agent_email in done_df.columns:
        display_cols.insert(2, col_agent_email)
    seen_c: set = set()
    display_cols = [c for c in display_cols
                    if c in done_df.columns and not (c in seen_c or seen_c.add(c))]

    auditor_list = sorted(
        [a for a in done_df[COL_AUDITOR].unique() if str(a).strip() not in ("", "—")],
        key=str.lower)
    all_option  = t("logs_filter_all")
    sel_auditor = st.selectbox(t("logs_auditor_sel"),
                               options=[all_option] + auditor_list,
                               key="logs_auditor_sel")
    view_df = (done_df[done_df[COL_AUDITOR] == sel_auditor].copy()
               if sel_auditor != all_option else done_df.copy())

    total_processed = len(view_df)
    unique_auditors = view_df[COL_AUDITOR].nunique()
    parsed_dates    = view_df[COL_DATE].apply(parse_dt).dropna()
    date_range_str  = (f"{parsed_dates.min().strftime('%Y-%m-%d')} → "
                       f"{parsed_dates.max().strftime('%Y-%m-%d')}"
                       if not parsed_dates.empty else "—")

    st.markdown(f"""
    <div class="log-summary-card">
      <div class="log-stat-row">
        <div class="log-stat">
          <span class="log-stat-value">{total_processed}</span>
          <span class="log-stat-label">{t('logs_total')}</span>
        </div>
        <div class="log-stat-divider"></div>
        <div class="log-stat">
          <span class="log-stat-value">{unique_auditors}</span>
          <span class="log-stat-label">{t('logs_auditors')}</span>
        </div>
        <div class="log-stat-divider"></div>
        <div class="log-stat">
          <span class="log-stat-value" style="font-size:1.05rem;">{date_range_str}</span>
          <span class="log-stat-label">{t('logs_date_range')}</span>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    shown_label = " · ".join(display_cols)
    st.markdown(
        f"<div class='section-title'>📋 {t('logs_cols_shown')}: "
        f"<span style='font-weight:400;text-transform:none;letter-spacing:0;'>{shown_label}</span></div>",
        unsafe_allow_html=True)

    table_df = view_df[display_cols].copy()
    if COL_DATE in table_df.columns:
        table_df["_sort"] = table_df[COL_DATE].apply(parse_dt)
        table_df = table_df.sort_values("_sort", ascending=False, na_position="last")
        table_df = table_df.drop(columns=["_sort"])
    table_df = table_df.reset_index(drop=True)
    render_html_table(table_df, max_rows=1000)

    # Export
    export_df = table_df.copy()
    csv_buffer = io.StringIO()
    export_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    csv_bytes  = csv_buffer.getvalue().encode("utf-8-sig")
    date_tag   = datetime.now(TZ).strftime("%Y%m%d")
    auditor_tag = (sel_auditor.replace("@","_").replace(".","_")
                   if sel_auditor != all_option else "all_auditors")
    export_fname = f"audit_log_{auditor_tag}_{date_tag}.csv"

    st.markdown(f"""
    <div class="export-strip">
      <div>
        <div class="export-text">📥 {t('logs_export_hdr')}</div>
        <div class="export-sub">{t('logs_export_sub')} · {total_processed} rows · {len(display_cols)} columns</div>
      </div>
    </div>""", unsafe_allow_html=True)
    st.download_button(
        label=t("logs_export_btn"), data=csv_bytes,
        file_name=export_fname, mime="text/csv",
        key="logs_csv_download", use_container_width=False)


# ─────────────────────────────────────────────────────────────────────────────
#  18 · USER ADMIN  — [B] role column + role-change feature
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_role_col(uws, df_u: pd.DataFrame) -> pd.DataFrame:
    """Add 'role' column to UsersDB sheet if missing, default all rows to 'auditor'."""
    if "role" not in df_u.columns:
        # find the column count and append header
        col_idx = len(df_u.columns) + 1
        try:
            _gsheets_call(uws.update_cell, 1, col_idx, "role")
            # fill existing rows with 'auditor'
            for i in range(2, len(df_u) + 2):
                _gsheets_call(uws.update_cell, i, col_idx, "auditor")
        except Exception: pass
    return df_u

def render_user_admin(spreadsheet_id):
    spr = get_spreadsheet(); uws = spr.worksheet(USERS_SHEET)
    staff_raw = _fetch_users_cached(spreadsheet_id)
    staff     = pd.DataFrame(staff_raw) if staff_raw else pd.DataFrame()

    # Ensure role column exists
    if not staff.empty:
        staff = _ensure_role_col(uws, staff)

    cl, cr = st.columns([1, 1], gap="large")

    # ── Left: Add + Update PW ────────────────────────────────────────────────
    with cl:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_e = st.text_input("Email", placeholder="user@mof.gov")
            nu_p = st.text_input("Password", type="password")
            nu_r = st.selectbox(t("role_label"), VALID_ROLES,
                                format_func=lambda r: r.title())
            if st.form_submit_button("Register User", use_container_width=True):
                if nu_e.strip() and nu_p.strip():
                    already = (not staff.empty and
                               nu_e.lower().strip() in staff.get("email", pd.Series()).values)
                    if already: st.error(t("dup_email"))
                    else:
                        _gsheets_call(uws.append_row,
                                      [nu_e.lower().strip(), hash_pw(nu_p.strip()),
                                       nu_r, now_str()])
                        st.success(f"✅  {nu_e} registered as {nu_r}.")
                        time.sleep(0.7); st.rerun()
                else: st.warning(t("fill_fields"))

        st.markdown(f"<div class='section-title'>🔑 {t('update_pw')}</div>", unsafe_allow_html=True)
        if not staff.empty and "email" in staff.columns:
            with st.form("upd_pw_form"):
                se  = st.selectbox("Select staff", staff["email"].tolist(), key="upd_pw_sel")
                np_ = st.text_input("New Password", type="password")
                if st.form_submit_button("Update Password", use_container_width=True):
                    if np_.strip():
                        cell = _gsheets_call(uws.find, se)
                        if cell:
                            _gsheets_call(uws.update_cell, cell.row, 2, hash_pw(np_.strip()))
                            st.success(f"✅  Updated for {se}.")
                            time.sleep(0.7); st.rerun()

        # ── [B] Role Change ───────────────────────────────────────────────────
        st.markdown(
            f"<div class='section-title'>🎭 {t('change_role')}</div>", unsafe_allow_html=True)
        st.caption(t("change_role_sub"))
        if not staff.empty and "email" in staff.columns:
            with st.form("change_role_form"):
                cr_email = st.selectbox("Select user to change", staff["email"].tolist(),
                                        key="cr_email_sel")
                cr_role  = st.selectbox("New Role", VALID_ROLES,
                                        format_func=lambda r: r.title(), key="cr_role_sel")
                if st.form_submit_button("Update Role", use_container_width=True):
                    try:
                        # Find 'role' column index in sheet (1-based)
                        header_row = _gsheets_call(uws.row_values, 1)
                        if "role" in header_row:
                            role_col_idx = header_row.index("role") + 1
                        else:
                            # append role header if truly missing
                            role_col_idx = len(header_row) + 1
                            _gsheets_call(uws.update_cell, 1, role_col_idx, "role")
                        user_cell = _gsheets_call(uws.find, cr_email)
                        if user_cell:
                            _gsheets_call(uws.update_cell, user_cell.row, role_col_idx, cr_role)
                            st.success(f"{t('role_updated')} ({cr_email} → {cr_role})")
                            time.sleep(0.7); st.rerun()
                        else:
                            st.error("User not found in sheet.")
                    except Exception as e:
                        st.error(f"🚨 Role update failed: {e}")

    # ── Right: Staff directory + revoke ──────────────────────────────────────
    with cr:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>", unsafe_allow_html=True)
        staff_fresh = pd.DataFrame(_fetch_users_cached(spreadsheet_id)) if staff_raw else pd.DataFrame()
        if not staff_fresh.empty and "email" in staff_fresh.columns:
            # Build role-badge HTML inline in table
            show_cols = [c for c in ["email", "role", "created_at"] if c in staff_fresh.columns]
            tbl = staff_fresh[show_cols].copy().reset_index()

            # Render a custom table with role chips
            th_html = "<tr><th class='row-idx'>#</th>" + "".join(f"<th>{c}</th>" for c in show_cols) + "</tr>"
            td_html = ""
            for _, row in tbl.iterrows():
                tr = f"<td class='row-idx'>{row['index']}</td>"
                for c in show_cols:
                    val = str(row[c]) if row.get(c) else "—"
                    if c == "role":
                        badge = f"<span class='role-badge-{val}' style='font-size:.60rem;'>{val.title()}</span>"
                        tr += f"<td>{badge}</td>"
                    else:
                        tr += f"<td>{val[:40] if len(val)>40 else val}</td>"
                td_html += f"<tr>{tr}</tr>"
            st.markdown(
                f"<div class='gov-table-wrap'><table class='gov-table'>"
                f"<thead><tr>{th_html}</tr></thead><tbody>{td_html}</tbody>"
                f"</table></div>", unsafe_allow_html=True)

            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>",
                        unsafe_allow_html=True)
            de = st.selectbox("Select to revoke",
                              ["—"] + staff_fresh["email"].tolist(), key="del_sel")
            if de != "—":
                if st.button(f"Revoke access — {de}", key="del_btn"):
                    cell = _gsheets_call(uws.find, de)
                    if cell:
                        _gsheets_call(uws.delete_rows, cell.row)
                        st.success(f"✅  {de} revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered yet.")


# ─────────────────────────────────────────────────────────────────────────────
#  19 · MAIN CONTROLLER  — [B] 3-tier tab visibility
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        spr = get_spreadsheet(); sid = spr.id
        all_titles = [ws.title for ws in spr.worksheets()]
        if USERS_SHEET not in all_titles:
            uw = spr.add_worksheet(title=USERS_SHEET, rows="500", cols="4")
            _gsheets_call(uw.append_row, ["email", "password", "role", "created_at"])

        if not st.session_state.logged_in:
            render_login(sid); return

        st.markdown("<style>[data-testid='stSidebar']{display:flex!important;}</style>",
                    unsafe_allow_html=True)

        role     = st.session_state.user_role
        is_admin = (role == "admin")
        # [B] managers see analytics + logs but not user admin
        is_manager   = (role == "manager")
        can_analytics = is_admin or is_manager

        ts_str = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-header">
          <div>
            <div class="page-title">🏛️  {t('portal_title')}</div>
            <div class="page-subtitle">{t('ministry')}</div>
          </div>
          <div class="page-timestamp">{ts_str}</div>
        </div>""", unsafe_allow_html=True)

        atm       = {title.strip().lower(): title for title in all_titles}
        available = [atm[s.strip().lower()] for s in VISIBLE_SHEETS if s.strip().lower() in atm]

        df = pd.DataFrame(); headers = []; col_map = {}; ws_title = None; fetched_at = "—"

        if not available:
            st.warning("None of the configured worksheets found. Expected: " + ", ".join(VISIBLE_SHEETS))
            st.error(f"⚠️ Found: `{all_titles}`")
        else:
            ws_title = st.selectbox(t("workspace"), available, key="ws_sel")
            if ws_title:
                wck = f"ws_title::{ws_title}"
                if st.session_state.get("active_ws_key") != wck:
                    st.session_state.local_cache_key = None
                    st.session_state.active_ws_key   = wck
                try:
                    df, headers, col_map, fetched_at = get_local_data(sid, ws_title)
                except gspread.exceptions.WorksheetNotFound:
                    st.error(f"Worksheet '{ws_title}' not found.")
                except gspread.exceptions.APIError as e:
                    st.error(f"🚨 {t('retry_warning')}\n\n{e}")

        col_binder      = detect_column(headers, "binder")
        col_company     = detect_column(headers, "company")
        col_license     = detect_column(headers, "license")
        col_agent_email = detect_column(headers, "agent_email")

        f_email, f_binder, f_company, f_license, f_status = render_sidebar(
            headers, col_binder, col_company, col_license, is_admin, fetched_at)

        if not df.empty:
            st.markdown(f"<div class='section-title'>📊 {t('overview')}</div>",
                        unsafe_allow_html=True)
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
            filtered_df = apply_filters_locally(
                df, f_email, f_binder, f_company, f_license, f_status,
                col_binder, col_company, col_license)
            render_filter_bar(total_n, len(filtered_df),
                              f_email, f_binder, f_company, f_license, f_status)
        else:
            filtered_df = pd.DataFrame()

        # ── [B] Tab construction by role ──────────────────────────────────────
        if is_admin:
            tabs = st.tabs([
                t("tab_worklist"), t("tab_archive"),
                t("tab_analytics"), t("tab_logs"), t("tab_users"),
            ])
            t_work, t_arch, t_anal, t_logs, t_uadm = tabs
        elif is_manager:
            # manager sees worklist, archive, analytics, logs — no user admin
            tabs = st.tabs([
                t("tab_worklist"), t("tab_archive"),
                t("tab_analytics"), t("tab_logs"),
            ])
            t_work, t_arch, t_anal, t_logs = tabs
            t_uadm = None
        else:
            # auditor: worklist + archive only
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>",
                        unsafe_allow_html=True)
            tabs = st.tabs([t("tab_worklist"), t("tab_archive")])
            t_work, t_arch = tabs
            t_anal = t_logs = t_uadm = None

        # ── Worklist ──────────────────────────────────────────────────────────
        with t_work:
            if not df.empty and ws_title:
                pv  = filtered_df[filtered_df[COL_STATUS] != VAL_DONE].copy()
                pd_ = pv.copy(); pd_.index = pd_.index + 2
                render_worklist(pd_, df, headers, col_map, ws_title,
                                f_email, f_binder, f_company, f_license, f_status)

        # ── Archive ───────────────────────────────────────────────────────────
        with t_arch:
            if not df.empty and ws_title:
                dv = filtered_df[filtered_df[COL_STATUS] == VAL_DONE].copy()
                dv.index = dv.index + 2
                render_archive(dv, df, col_map, ws_title, is_admin,
                               f_email, f_binder, f_company, f_license, f_status)

        # ── Analytics (admin + manager) ───────────────────────────────────────
        if can_analytics and t_anal is not None:
            with t_anal:
                if not df.empty:
                    render_analytics(df,
                                     col_agent_email=col_agent_email,
                                     col_binder=col_binder,
                                     col_company=col_company)

        # ── Auditor Logs (admin + manager) ────────────────────────────────────
        if can_analytics and t_logs is not None:
            with t_logs:
                if df.empty: st.warning(t("empty_sheet"))
                else:
                    render_auditor_logs(df, col_company, col_binder, col_agent_email)

        # ── User Admin (admin only) ───────────────────────────────────────────
        if is_admin and t_uadm is not None:
            with t_uadm:
                render_user_admin(sid)

    except Exception as exc:
        st.error(f"🚨  System Error: {exc}")
        with st.expander("Technical Details", expanded=False):
            st.exception(exc)


if __name__ == "__main__":
    main()
