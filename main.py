# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  ·  v10.1 (Smart Sheet Matching)
#  Architecture: Optimistic UI / Local-First Mutation
#
#  CONCURRENCY MODEL (20 users · 9-hour shifts)
#  ─────────────────────────────────────────────────────────────────────────────
#  Rule 1 — READ ONCE, NEVER BUST
#    @st.cache_data(ttl=600) fetches the sheet once per 10-minute window.
#    ZERO calls to .clear() anywhere in the codebase.
#
#  Rule 2 — OPTIMISTIC LOCAL MUTATION
#    After a write, st.session_state.local_df is updated IN MEMORY.
#    No API read is triggered on rerun.
#
#  Rule 3 — EXPONENTIAL BACKOFF ON EVERY API CALL
#    tenacity wraps every gspread call. On a 429, the worker thread
#    waits 2 → 4 → 8 → 16 → 32 s before retrying silently.
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

# ── HARDCODED VISIBLE WORKSHEETS (no dynamic manager) ────────────────────────
VISIBLE_SHEETS = ["Registration", "Salary Tax", "Annual Filing"]

COL_STATUS  = "Status"
COL_LOG     = "Audit_Log"
COL_AUDITOR = "Auditor_ID"
COL_DATE    = "Update_Date"
SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE]
VAL_DONE    = "Processed"
VAL_PENDING = "Pending"

READ_TTL    = 600
BACKOFF_MAX = 5

# ─────────────────────────────────────────────────────────────────────────────
#  4 · EXPONENTIAL BACKOFF DECORATOR
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
#  5 · PREMIUM CORPORATE LIGHT THEME — CSS INJECTION
# ─────────────────────────────────────────────────────────────────────────────
def inject_css() -> None:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Reset & Base ─────────────────────────────────────────────────────────── */
*, *::before, *::after {
  box-sizing: border-box !important;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"], .main, .block-container {
  background-color: #F8FAFC !important;
  color: #0F172A !important;
}

p, span, div, li, label, h1, h2, h3, h4, h5, h6,
.stMarkdown, [data-testid="stMarkdownContainer"] {
  color: #0F172A !important;
}

/* ── Hide Streamlit chrome ────────────────────────────────────────────────── */
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"],
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"] {
  display: none !important;
}

/* ── Sidebar ──────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
  background-color: #FFFFFF !important;
  border-right: 1px solid #E2E8F0 !important;
  box-shadow: 2px 0 8px rgba(0, 0, 0, 0.04) !important;
}
[data-testid="stSidebar"] * { color: #0F172A !important; }

/* ── Text Inputs ──────────────────────────────────────────────────────────── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
[data-testid="stSidebar"] .stTextInput > div > div > input {
  background-color: #FFFFFF !important;
  color: #0F172A !important;
  border: 1.5px solid #CBD5E1 !important;
  border-radius: 8px !important;
  font-size: 0.875rem !important;
  padding: 10px 14px !important;
  transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04) !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
  border-color: #1E3A8A !important;
  box-shadow: 0 0 0 3px rgba(30, 58, 138, 0.1) !important;
  outline: none !important;
}
.stTextInput > div > div > input::placeholder,
.stTextArea > div > div > textarea::placeholder {
  color: #94A3B8 !important;
}

/* ── Field Labels ─────────────────────────────────────────────────────────── */
.stTextInput > label, .stTextArea > label,
.stSelectbox > label, .stMultiSelect > label {
  color: #475569 !important;
  font-size: 0.72rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
}

/* ── Selectbox ────────────────────────────────────────────────────────────── */
.stSelectbox > div > div, [data-baseweb="select"] > div {
  background-color: #FFFFFF !important;
  color: #0F172A !important;
  border-color: #CBD5E1 !important;
  border-radius: 8px !important;
  box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
[data-baseweb="menu"] li,
[data-baseweb="menu"] [role="option"] {
  background-color: #FFFFFF !important;
  color: #0F172A !important;
}
[data-baseweb="menu"] li:hover,
[data-baseweb="menu"] [aria-selected="true"] {
  background-color: #EFF6FF !important;
  color: #1E3A8A !important;
}

/* ── Metric Cards ─────────────────────────────────────────────────────────── */
[data-testid="stMetricContainer"] {
  background: #FFFFFF !important;
  border: 1px solid #E2E8F0 !important;
  border-radius: 12px !important;
  padding: 20px 24px !important;
  box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05), 0 2px 4px -1px rgba(0,0,0,0.03) !important;
  transition: transform 0.2s ease, box-shadow 0.2s ease !important;
}
[data-testid="stMetricContainer"]:hover {
  transform: translateY(-3px) !important;
  box-shadow: 0 10px 20px -5px rgba(30,58,138,0.12), 0 4px 8px -2px rgba(0,0,0,0.06) !important;
}
[data-testid="stMetricValue"] {
  font-size: 2rem !important;
  font-weight: 700 !important;
  color: #1E3A8A !important;
  letter-spacing: -0.02em !important;
}
[data-testid="stMetricLabel"] {
  font-size: 0.70rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.08em !important;
  text-transform: uppercase !important;
  color: #64748B !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem !important; font-weight: 500 !important; }

/* ── Buttons ──────────────────────────────────────────────────────────────── */
.stButton > button {
  background-color: #1E3A8A !important;
  color: #FFFFFF !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  font-size: 0.84rem !important;
  padding: 9px 18px !important;
  letter-spacing: 0.01em !important;
  transition: background-color 0.15s ease, transform 0.15s ease, box-shadow 0.15s ease !important;
  box-shadow: 0 1px 3px rgba(30,58,138,0.25) !important;
}
.stButton > button:hover {
  background-color: #1e40af !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 6px 12px rgba(30,58,138,0.25) !important;
}
.stButton > button:active { transform: translateY(0) !important; }

/* ── Forms ────────────────────────────────────────────────────────────────── */
div[data-testid="stForm"] {
  background-color: #FFFFFF !important;
  border: 1px solid #E2E8F0 !important;
  border-radius: 12px !important;
  padding: 24px 28px !important;
  box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05) !important;
}

/* ── Tabs ─────────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
  gap: 0 !important;
  background: #FFFFFF !important;
  border: 1px solid #E2E8F0 !important;
  border-radius: 10px !important;
  padding: 4px !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important;
  color: #64748B !important;
  border-radius: 7px !important;
  border: none !important;
  padding: 9px 20px !important;
  font-weight: 600 !important;
  font-size: 0.82rem !important;
  transition: color 0.15s ease !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #1E3A8A !important; }
.stTabs [aria-selected="true"] {
  background-color: #EFF6FF !important;
  color: #1E3A8A !important;
  border: none !important;
}

/* ── Expander ─────────────────────────────────────────────────────────────── */
.streamlit-expanderHeader {
  background-color: #F1F5F9 !important;
  color: #0F172A !important;
  border: 1px solid #E2E8F0 !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  font-size: 0.84rem !important;
}
.streamlit-expanderContent {
  background-color: #FFFFFF !important;
  border: 1px solid #E2E8F0 !important;
  border-top: none !important;
  border-radius: 0 0 8px 8px !important;
  padding: 16px !important;
}

/* ── Alerts ───────────────────────────────────────────────────────────────── */
[data-testid="stAlert"] {
  border-radius: 10px !important;
  border: 1px solid #E2E8F0 !important;
  background-color: #F8FAFC !important;
}
[data-testid="stAlert"] * { color: #0F172A !important; }

/* ═══════════════════════════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   ═══════════════════════════════════════════════════════════════════════════ */

/* ── Login Card ───────────────────────────────────────────────────────────── */
.login-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 80vh;
}
.login-card {
  width: 100%;
  max-width: 440px;
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-radius: 20px;
  padding: 48px 44px 40px;
  box-shadow: 0 20px 60px rgba(15,23,42,0.10), 0 8px 24px rgba(15,23,42,0.06);
}
.login-logo-ring {
  width: 68px; height: 68px;
  margin: 0 auto 20px;
  border-radius: 16px;
  background: linear-gradient(135deg, #1E3A8A 0%, #3B82F6 100%);
  display: flex; align-items: center; justify-content: center;
  font-size: 1.9rem;
  box-shadow: 0 8px 20px rgba(30,58,138,0.30);
}
.login-ministry {
  text-align: center;
  font-size: 0.60rem;
  font-weight: 700;
  letter-spacing: 0.20em;
  text-transform: uppercase;
  color: #64748B !important;
  margin-bottom: 6px;
}
.login-title {
  text-align: center;
  font-size: 1.30rem;
  font-weight: 700;
  color: #0F172A !important;
  letter-spacing: -0.02em;
  margin-bottom: 4px;
}
.login-subtitle {
  text-align: center;
  font-size: 0.78rem;
  color: #64748B !important;
  margin-bottom: 28px;
}
.login-divider {
  width: 40px; height: 3px;
  background: linear-gradient(90deg, #1E3A8A, #3B82F6);
  border-radius: 99px;
  margin: 0 auto 24px;
}
.classified-tag {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  background: #FEF2F2;
  color: #DC2626 !important;
  border: 1px solid #FECACA;
  border-radius: 6px;
  font-size: 0.58rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  padding: 4px 10px;
  margin: 0 auto 20px;
  display: block;
  text-align: center;
  width: fit-content;
}
.login-card .stButton > button {
  background: linear-gradient(135deg, #1E3A8A 0%, #2563EB 100%) !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  font-size: 0.92rem !important;
  border-radius: 10px !important;
  padding: 13px !important;
  width: 100% !important;
  border: none !important;
  box-shadow: 0 4px 14px rgba(30,58,138,0.35) !important;
  letter-spacing: 0.02em !important;
}
.login-card .stButton > button:hover {
  background: linear-gradient(135deg, #1e40af 0%, #1d4ed8 100%) !important;
  box-shadow: 0 8px 24px rgba(30,58,138,0.45) !important;
  transform: translateY(-2px) !important;
}

/* ── Page Header ──────────────────────────────────────────────────────────── */
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 0 20px 0;
  border-bottom: 1px solid #E2E8F0;
  margin-bottom: 24px;
}
.page-title {
  font-size: 1.50rem;
  font-weight: 700;
  color: #0F172A !important;
  letter-spacing: -0.025em;
  margin: 0;
}
.page-subtitle {
  font-size: 0.80rem;
  color: #64748B !important;
  margin-top: 3px;
}
.page-timestamp {
  font-size: 0.75rem;
  color: #94A3B8 !important;
  font-weight: 500;
  background: #F1F5F9;
  padding: 6px 14px;
  border-radius: 20px;
  border: 1px solid #E2E8F0;
}

/* ── Section Title ────────────────────────────────────────────────────────── */
.section-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.82rem;
  font-weight: 700;
  color: #1E3A8A !important;
  margin: 22px 0 12px;
  padding-left: 10px;
  border-left: 3px solid #3B82F6;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

/* ── Worklist Header ──────────────────────────────────────────────────────── */
.worklist-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-top: 3px solid #1E3A8A;
  border-radius: 12px;
  padding: 16px 20px;
  margin-bottom: 16px;
  box-shadow: 0 4px 6px -1px rgba(0,0,0,0.04);
}
.worklist-title { font-size: 1rem; font-weight: 700; color: #0F172A !important; }
.worklist-sub   { font-size: 0.74rem; color: #64748B !important; margin-top: 2px; }

/* ── Progress Bar ─────────────────────────────────────────────────────────── */
.prog-wrap {
  background: #E2E8F0;
  border-radius: 99px;
  height: 6px;
  overflow: hidden;
  margin: 6px 0 10px;
}
.prog-fill {
  height: 100%;
  border-radius: 99px;
  background: linear-gradient(90deg, #1E3A8A, #3B82F6);
  transition: width 0.7s cubic-bezier(0.4, 0, 0.2, 1);
}
.prog-labels {
  display: flex;
  justify-content: space-between;
  font-size: 0.72rem;
  color: #64748B !important;
  font-weight: 600;
  margin-bottom: 4px;
}

/* ── Status Chips ─────────────────────────────────────────────────────────── */
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px 10px;
  border-radius: 99px;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.chip-done    { background: #DCFCE7; color: #166534 !important; }
.chip-pending { background: #FEF3C7; color: #92400E !important; }
.chip-admin   { background: #EFF6FF; color: #1E40AF !important; }
.chip-audit   { background: #F0FDF4; color: #166534 !important; }
.s-chip { display: inline-flex; align-items: center; padding: 2px 9px; border-radius: 99px;
  font-size: 0.62rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; }
.s-done    { background: #DCFCE7; color: #166534 !important; }
.s-pending { background: #FEF3C7; color: #92400E !important; }

/* ── Data Table ───────────────────────────────────────────────────────────── */
.gov-table-wrap {
  overflow-x: auto;
  border: 1px solid #E2E8F0;
  border-radius: 12px;
  margin-bottom: 16px;
  box-shadow: 0 4px 6px -1px rgba(0,0,0,0.04);
}
.gov-table {
  width: 100%;
  border-collapse: collapse;
  background-color: #FFFFFF;
  font-size: 0.83rem;
}
.gov-table thead tr {
  background-color: #F8FAFC;
  border-bottom: 2px solid #E2E8F0;
}
.gov-table th {
  color: #475569 !important;
  background-color: #F8FAFC !important;
  font-weight: 700 !important;
  font-size: 0.65rem !important;
  letter-spacing: 0.08em !important;
  text-transform: uppercase !important;
  padding: 12px 16px !important;
  white-space: nowrap;
  text-align: left !important;
  border-right: 1px solid #E2E8F0;
}
.gov-table th:last-child { border-right: none; }
.gov-table td {
  color: #0F172A !important;
  background-color: #FFFFFF !important;
  padding: 10px 16px !important;
  font-size: 0.83rem !important;
  border-bottom: 1px solid #F1F5F9 !important;
  border-right: 1px solid #F1F5F9 !important;
  vertical-align: middle !important;
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.gov-table td:last-child { border-right: none; }
.gov-table tbody tr:hover td {
  background-color: #F8FAFC !important;
  color: #0F172A !important;
}
.gov-table tbody tr:last-child td { border-bottom: none !important; }
.gov-table td.row-idx, .gov-table th.row-idx {
  color: #94A3B8 !important;
  font-size: 0.70rem !important;
  min-width: 50px;
  text-align: center !important;
}

/* ── Leaderboard Rows ─────────────────────────────────────────────────────── */
.lb-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 11px 16px;
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-radius: 10px;
  margin-bottom: 6px;
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}
.lb-row:hover {
  transform: translateX(4px);
  border-color: #3B82F6;
  box-shadow: 0 4px 12px rgba(59,130,246,0.10);
}
.lb-medal { font-size: 1.1rem; width: 24px; text-align: center; }
.lb-name  { flex: 1; font-size: 0.84rem; font-weight: 600; color: #0F172A !important; }
.lb-count { font-size: 0.92rem; font-weight: 700; color: #1E3A8A !important; }

/* ── Audit Log Lines ──────────────────────────────────────────────────────── */
.log-line {
  font-family: 'Courier New', monospace !important;
  font-size: 0.74rem;
  color: #475569 !important;
  padding: 4px 0;
  border-bottom: 1px solid #F1F5F9;
}
.log-line:last-child { border-bottom: none; }

/* ── Sidebar Components ───────────────────────────────────────────────────── */
.sidebar-header {
  border-top: 3px solid #1E3A8A;
  padding: 20px 16px 16px;
}
.sidebar-logo-text {
  font-size: 1.05rem;
  font-weight: 700;
  color: #0F172A !important;
  margin-bottom: 2px;
}
.sidebar-ministry {
  font-size: 0.60rem;
  color: #64748B !important;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.sb-divider { margin: 0; border: none; border-top: 1px solid #E2E8F0; }
.sb-label {
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: #94A3B8 !important;
  margin-bottom: 5px;
}
.sb-email {
  font-size: 0.84rem;
  font-weight: 600;
  color: #0F172A !important;
  word-break: break-all;
}
.sb-user-card {
  background: #F8FAFC;
  border: 1px solid #E2E8F0;
  border-radius: 10px;
  padding: 13px 14px;
  margin-bottom: 10px;
}
.cache-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  background: #DCFCE7;
  color: #166534 !important;
  border: 1px solid #BBF7D0;
  border-radius: 5px;
  font-size: 0.60rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 3px 8px;
}
.cache-info {
  font-size: 0.62rem;
  color: #94A3B8 !important;
  margin-top: 5px;
}
.cache-strip {
  padding: 10px 16px 8px;
  background: #F8FAFC;
  border-bottom: 1px solid #E2E8F0;
}

/* ── Filter Bar ───────────────────────────────────────────────────────────── */
.adv-filter-header {
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: #1E3A8A !important;
  margin-bottom: 12px;
  padding-bottom: 8px;
  border-bottom: 1px solid #E2E8F0;
}
.col-hint { font-size: 0.58rem; font-weight: 400; opacity: 0.60; color: #64748B !important; }
.filter-result-bar {
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-left: 3px solid #1E3A8A;
  border-radius: 10px;
  padding: 11px 16px;
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  box-shadow: 0 2px 4px rgba(0,0,0,0.04);
}
.filter-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: #EFF6FF;
  color: #1E3A8A !important;
  border: 1px solid #BFDBFE;
  border-radius: 99px;
  font-size: 0.64rem;
  font-weight: 700;
  padding: 2px 9px;
}
.result-count {
  font-size: 0.76rem;
  color: #64748B !important;
  margin-left: auto;
}

/* ── RBAC Banner ──────────────────────────────────────────────────────────── */
.rbac-banner {
  background: #EFF6FF;
  border: 1px solid #BFDBFE;
  border-left: 3px solid #3B82F6;
  border-radius: 10px;
  padding: 11px 16px;
  margin-bottom: 16px;
  font-size: 0.78rem;
  color: #1E40AF !important;
  font-weight: 500;
}
</style>
""", unsafe_allow_html=True)

inject_css()

# ─────────────────────────────────────────────────────────────────────────────
#  6 · TRANSLATIONS
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry": "Ministry of Finance & Customs",
        "portal_title": "Tax Audit & Compliance Portal",
        "portal_sub": "Authorised Access Only",
        "classified": "CLASSIFIED — GOVERNMENT USE ONLY",
        "login_prompt": "Enter your authorised credentials to access the system",
        "email_field": "Official Email / User ID", "password_field": "Password",
        "sign_in": "Authenticate & Enter", "sign_out": "Sign Out",
        "bad_creds": "Authentication failed. Verify your credentials and try again.",
        "language": "Interface Language",
        "workspace": "Active Case Register", "overview": "Case Overview",
        "total": "Total Cases", "processed": "Processed", "outstanding": "Outstanding",
        "worklist_title": "Audit Worklist", "worklist_sub": "Active cases pending review",
        "tab_worklist": "📋  Worklist", "tab_archive": "✅  Archive",
        "tab_analytics": "📊  Analytics", "tab_users": "⚙️  User Admin",
        "select_case": "Select a case to inspect", "audit_trail": "Audit Trail",
        "approve_save": "Approve & Commit Record", "reopen": "Re-open Record (Admin)",
        "leaderboard": "Auditor Productivity Leaderboard",
        "daily_trend": "Daily Processing Trend",
        "period": "Time Period", "today": "Today", "this_week": "This Week",
        "this_month": "This Month", "all_time": "All Time",
        "add_auditor": "Register New Auditor", "update_pw": "Update Password",
        "remove_user": "Revoke Access", "staff_dir": "Authorised Staff",
        "no_records": "No records found for this period.",
        "empty_sheet": "This register contains no data.",
        "saved_ok": "✅ Record approved and committed. View updated instantly — sheet syncs within 10 min.",
        "dup_email": "This email address is already registered.",
        "fill_fields": "All fields are required.",
        "signed_as": "Authenticated as", "role_admin": "System Administrator",
        "role_auditor": "Tax Auditor", "processing": "Processing Case",
        "no_history": "No audit trail for this record.",
        "records_period": "Records (period)", "active_days": "Active Days",
        "avg_per_day": "Avg / Day",
        "adv_filters": "Advanced Filters", "f_email": "Auditor Email",
        "f_binder": "Company Binder No.", "f_company": "Company Name",
        "f_license": "License Number",
        "f_status": "Status", "clear_filters": "Clear Filters",
        "active_filters": "Active filters", "results_shown": "results shown",
        "no_match": "No records match the applied filters.",
        "status_all": "All Statuses", "status_pending": "Pending Only",
        "status_done": "Processed Only",
        "retry_warning": "⏳ Google Sheets quota reached — retrying with backoff…",
        "local_mode": "Optimistic UI", "cache_age": "Cache TTL",
    },
    "ku": {
        "ministry": "وەزارەتی دارایی و گومرگ",
        "portal_title": "پۆرتەلی فەرمی وردبینی باج و پابەندبوون",
        "portal_sub": "تەنها دەستپێگەیشتنی مەرجدارکراو",
        "classified": "نهێنی — تەنها بەکارهێنانی حکومی",
        "login_prompt": "زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email_field": "ئیمەیڵی فەرمی / ناساندن", "password_field": "پاسۆرد",
        "sign_in": "دەستپێبکە", "sign_out": "چوونەدەرەوە",
        "bad_creds": "ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "language": "زمانی ڕووکار",
        "workspace": "تۆماری کیسە چالاکەکان", "overview": "کورتەی کیسەکان",
        "total": "کۆی کیسەکان", "processed": "کارکراوە", "outstanding": "ماوە",
        "worklist_title": "لیستی کاری وردبینی",
        "worklist_sub": "کیسە چالاکەکانی چاوەڕوان",
        "tab_worklist": "📋  لیستی کاری", "tab_archive": "✅  ئەرشیف",
        "tab_analytics": "📊  ئەنالیتیکس", "tab_users": "⚙️  بەکارهێنەر",
        "select_case": "کیسێک هەڵبژێرە بۆ پشکنین", "audit_trail": "مێژووی گۆڕانکاری",
        "approve_save": "پەسەندکردن و پاشەکەوتکردن",
        "reopen": "کردنەوەی دووبارەی کیس (ئەدمین)",
        "leaderboard": "تەختەی بەرهەمهێنانی ئۆدیتۆر",
        "daily_trend": "ترەندی بەرپرسانەی ڕۆژانە",
        "period": "ماوەی کات", "today": "ئەمڕۆ", "this_week": "ئەم هەفتەیە",
        "this_month": "ئەم مانگەیە", "all_time": "هەموو کات",
        "add_auditor": "تۆمارکردنی ئۆدیتۆری نوێ", "update_pw": "نوێکردنەوەی پاسۆرد",
        "remove_user": "هەڵوەشاندنەوەی دەستپێگەیشتن",
        "staff_dir": "کارمەندە مەرجداركراوەکان",
        "no_records": "هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet": "ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok": "✅ کیسەکە پەسەندکرا. دیمەن نوێکرایەوە — شیت لەناو ١٠ خولەک هاوکێش دەبێت.",
        "dup_email": "ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields": "هەموو خانەکان پەیوەندییانە.",
        "signed_as": "چووییتە ژوورەوە بەناوی",
        "role_admin": "بەڕێوەبەری سیستەم", "role_auditor": "ئۆدیتۆری باج",
        "processing": "پشکنینی کیسی", "no_history": "هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period": "تۆمارەکان (ماوە)", "active_days": "ڕۆژی چالاک",
        "avg_per_day": "تێکڕای ڕۆژانە",
        "adv_filters": "فلتەرە پێشکەوتووەکان", "f_email": "ئیمەیڵی ئۆدیتۆر",
        "f_binder": "ژمارەی بایندەری کۆمپانیا", "f_company": "ناوی کۆمپانیا",
        "f_license": "ژمارەی مۆڵەتی",
        "f_status": "دەربار", "clear_filters": "سڕینەوەی فلتەرەکان",
        "active_filters": "فلتەرە چالاکەکان", "results_shown": "ئەنجامی پیشاندراو",
        "no_match": "هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "status_all": "هەموو", "status_pending": "چاوەڕوان تەنها",
        "status_done": "کارکراو تەنها",
        "retry_warning": "⏳ کووتای گووگڵ شیت گەیشت — دووبارە هەوڵدەدرێت…",
        "local_mode": "Optimistic UI", "cache_age": "Cache TTL",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)

# ─────────────────────────────────────────────────────────────────────────────
#  7 · HELPERS
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
}

def detect_column(headers: list[str], kind: str) -> str | None:
    keywords = sorted(_COL_KEYWORDS.get(kind, []), key=len, reverse=True)
    for h in headers:
        hl = h.lower().strip()
        for kw in keywords:
            if kw.lower() in hl:
                return h
    return None

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def parse_dt(s: str) -> datetime | None:
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except Exception:
        return None

def clean_cell(value) -> str:
    if value is None:
        return ""
    s = str(value)
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        s = s.replace(ch, "")
    return s.replace("\xa0", " ").strip()

def _raw_to_dataframe(raw: list[list]) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    if not raw:
        return pd.DataFrame(), [], {}
    seen: dict[str, int] = {}
    headers: list[str] = []
    for h in raw[0]:
        h = clean_cell(h) or "Unnamed"
        if h in seen:
            seen[h] += 1
            headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers.append(h)
    if not headers:
        return pd.DataFrame(), [], {}
    n_cols = len(headers)
    normalised = []
    for raw_row in raw[1:]:
        row = [clean_cell(c) for c in raw_row]
        row = (row + [""] * n_cols)[:n_cols]
        normalised.append(row)
    if not normalised:
        return pd.DataFrame(columns=headers), headers, {}
    df = pd.DataFrame(normalised, columns=headers)
    df = df[~(df == "").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns:
            df[sc] = ""
    df = df.fillna("").infer_objects(copy=False)
    col_map = {h: i + 1 for i, h in enumerate(headers)}
    return df, headers, col_map

def apply_period_filter(df: pd.DataFrame, col: str, period: str) -> pd.DataFrame:
    if period == "all" or col not in df.columns:
        return df
    now = datetime.now(TZ)
    if period == "today":
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_week":
        cutoff = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_month":
        cutoff = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return df
    return df[df[col].apply(parse_dt) >= cutoff]

def _n_active(f_email, f_binder, f_company, f_license, f_status) -> int:
    return sum([bool(f_email.strip()), bool(f_binder.strip()),
                bool(f_company.strip()), bool(f_license.strip()), f_status != "all"])

def apply_filters_locally(
    df: pd.DataFrame,
    f_email: str, f_binder: str, f_company: str, f_license: str, f_status: str,
    col_binder: str | None, col_company: str | None, col_license: str | None,
) -> pd.DataFrame:
    result = df.copy()
    if f_status == "pending":
        result = result[result[COL_STATUS] != VAL_DONE]
    elif f_status == "done":
        result = result[result[COL_STATUS] == VAL_DONE]
    if f_email.strip():
        email_cols = [c for c in result.columns if "auditor_email" in c.lower() or c == COL_AUDITOR]
        if email_cols:
            mask = pd.Series(False, index=result.index)
            for ec in email_cols:
                mask |= result[ec].str.contains(f_email.strip(), case=False, na=False)
            result = result[mask]
    if f_binder.strip() and col_binder and col_binder in result.columns:
        result = result[result[col_binder].str.contains(f_binder.strip(), case=False, na=False)]
    if f_company.strip() and col_company and col_company in result.columns:
        result = result[result[col_company].str.contains(f_company.strip(), case=False, na=False)]
    if f_license.strip() and col_license and col_license in result.columns:
        result = result[result[col_license].str.contains(f_license.strip(), case=False, na=False)]
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  8 · GOOGLE SHEETS CONNECTION
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = json.loads(st.secrets["json_key"], strict=False)
    pk  = raw["private_key"]
    pk  = pk.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
    pk  = pk.replace("\\n", "").replace("\n", "")
    pk  = "".join(pk.split())
    pk  = "\n".join(textwrap.wrap(pk, 64))
    raw["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(raw, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

# ─────────────────────────────────────────────────────────────────────────────
#  9 · CACHED READ LAYER  (Rule 1 — Read Once, Never Bust)
#  CRITICAL: .clear() is NEVER called anywhere in this codebase.
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_raw_sheet_cached(spreadsheet_id: str, ws_title: str) -> tuple[list[list], str]:
    spreadsheet = get_spreadsheet()
    ws          = spreadsheet.worksheet(ws_title)
    raw         = _gsheets_call(ws.get_all_values)
    return raw, now_str()

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_users_cached(spreadsheet_id: str) -> list[dict]:
    spreadsheet = get_spreadsheet()
    ws          = spreadsheet.worksheet(USERS_SHEET)
    return _gsheets_call(ws.get_all_records)

# ─────────────────────────────────────────────────────────────────────────────
#  10 · OPTIMISTIC LOCAL DATA STORE  (Rule 2 — Local-First Mutation)
# ─────────────────────────────────────────────────────────────────────────────
def _data_fingerprint(raw: list[list]) -> str:
    sample = str(raw[:20])
    return hashlib.md5(sample.encode("utf-8")).hexdigest()

def get_local_data(
    spreadsheet_id: str,
    ws_title: str,
) -> tuple[pd.DataFrame, list[str], dict[str, int], str]:
    raw, fetched_at = _fetch_raw_sheet_cached(spreadsheet_id, ws_title)
    fingerprint     = _data_fingerprint(raw)
    cache_key       = f"{ws_title}::{fingerprint}"

    if st.session_state.get("local_cache_key") != cache_key:
        df_fresh, headers_fresh, col_map_fresh = _raw_to_dataframe(raw)
        st.session_state.local_df         = df_fresh.copy()
        st.session_state.local_headers    = headers_fresh
        st.session_state.local_col_map    = col_map_fresh
        st.session_state.local_cache_key  = cache_key
        st.session_state.local_fetched_at = fetched_at

    return (
        st.session_state.local_df,
        st.session_state.local_headers,
        st.session_state.local_col_map,
        st.session_state.local_fetched_at or fetched_at,
    )

def _apply_optimistic_approve(
    df_iloc: int, new_vals: dict[str, str],
    auditor: str, ts_now: str, log_prefix: str,
) -> None:
    ldf = st.session_state.local_df
    if df_iloc < 0 or df_iloc >= len(ldf):
        return
    for fname, fval in new_vals.items():
        if fname in ldf.columns:
            ldf.at[df_iloc, fname] = fval
    old_log = str(ldf.at[df_iloc, COL_LOG]).strip() if COL_LOG in ldf.columns else ""
    ldf.at[df_iloc, COL_STATUS]  = VAL_DONE
    ldf.at[df_iloc, COL_AUDITOR] = auditor
    ldf.at[df_iloc, COL_DATE]    = ts_now
    if COL_LOG in ldf.columns:
        ldf.at[df_iloc, COL_LOG] = f"{log_prefix}\n{old_log}".strip()
    st.session_state.local_df = ldf

def _apply_optimistic_reopen(df_iloc: int) -> None:
    ldf = st.session_state.local_df
    if df_iloc < 0 or df_iloc >= len(ldf):
        return
    ldf.at[df_iloc, COL_STATUS] = VAL_PENDING
    st.session_state.local_df = ldf

# ─────────────────────────────────────────────────────────────────────────────
#  11 · GOOGLE SHEETS WRITE LAYER  (Rule 3 — Backoff on every call)
# ─────────────────────────────────────────────────────────────────────────────
def ensure_system_cols_in_sheet(
    ws, headers: list[str], col_map: dict[str, int]
) -> tuple[list[str], dict[str, int]]:
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers) + 1
            if new_pos > ws.col_count:
                _gsheets_call(ws.add_cols, max(4, new_pos - ws.col_count + 1))
            _gsheets_call(ws.update_cell, 1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map

def write_approval_to_sheet(
    ws_title: str, sheet_row: int, col_map: dict[str, int],
    headers: list[str], new_vals: dict[str, str], record: dict,
    auditor: str, ts_now: str, log_prefix: str,
) -> None:
    spreadsheet = get_spreadsheet()
    ws          = spreadsheet.worksheet(ws_title)
    headers, col_map = ensure_system_cols_in_sheet(ws, headers, col_map)
    old_log = str(record.get(COL_LOG, "")).strip()
    new_log = f"{log_prefix}\n{old_log}".strip()
    batch: list[dict] = []
    for fname, fval in new_vals.items():
        if fname in col_map and clean_cell(record.get(fname, "")) != fval:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[fname]), "values": [[fval]]})
    for col_name, value in [
        (COL_STATUS, VAL_DONE), (COL_AUDITOR, auditor),
        (COL_DATE, ts_now), (COL_LOG, new_log),
    ]:
        if col_name in col_map:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[col_name]), "values": [[value]]})
    if batch:
        _gsheets_call(ws.batch_update, batch)

def write_reopen_to_sheet(ws_title: str, sheet_row: int, col_map: dict) -> None:
    spreadsheet = get_spreadsheet()
    ws          = spreadsheet.worksheet(ws_title)
    if COL_STATUS in col_map:
        _gsheets_call(ws.update_cell, sheet_row, col_map[COL_STATUS], VAL_PENDING)

# ─────────────────────────────────────────────────────────────────────────────
#  12 · AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────
def authenticate(email: str, password: str, spreadsheet_id: str) -> str | None:
    email = email.lower().strip()
    if email == "admin" and password == st.secrets.get("admin_password", ""):
        return "admin"
    try:
        records = _fetch_users_cached(spreadsheet_id)
        df_u    = pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns:
            return None
        row = df_u[df_u["email"] == email]
        if not row.empty and hash_pw(password) == str(row["password"].values[0]):
            return "auditor"
    except Exception:
        pass
    return None

# ─────────────────────────────────────────────────────────────────────────────
#  13 · HTML TABLE RENDERER
# ─────────────────────────────────────────────────────────────────────────────
def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty:
        st.info("No records to display.")
        return
    display_df = df.head(max_rows)
    th_cells   = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col == COL_LOG:
            continue
        th_cells += f"<th>{col}</th>"
    rows_html = ""
    for idx, row in display_df.iterrows():
        row_html = f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col == COL_LOG:
                continue
            raw_val   = str(row[col]) if row[col] != "" else ""
            cell_disp = raw_val or "—"
            if col == COL_STATUS:
                if raw_val == VAL_DONE:
                    cell_disp = "<span class='s-chip s-done'>✓ Processed</span>"
                else:
                    cell_disp = "<span class='s-chip s-pending'>⏳ Pending</span>"
            elif len(raw_val) > 55:
                cell_disp = f"<span title='{raw_val}'>{raw_val[:52]}…</span>"
            row_html += f"<td>{cell_disp}</td>"
        rows_html += f"<tr>{row_html}</tr>"
    st.markdown(
        "<div class='gov-table-wrap'><table class='gov-table'>"
        f"<thead><tr>{th_cells}</tr></thead>"
        f"<tbody>{rows_html}</tbody></table></div>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
#  14 · UI COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────
def render_login(spreadsheet_id: str) -> None:
    st.markdown("""<style>
      [data-testid="stSidebar"],[data-testid="collapsedControl"]{display:none!important;}
    </style>""", unsafe_allow_html=True)

    # Language toggle — top right
    _g, c1, c2 = st.columns([8, .6, .6])
    with c1:
        if st.button("EN", key="lg_en"):
            st.session_state.lang = "en"; st.rerun()
    with c2:
        if st.button("KU", key="lg_ku"):
            st.session_state.lang = "ku"; st.rerun()

    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        st.markdown(f"""
        <div style="display:flex;flex-direction:column;align-items:center;padding:48px 0 32px;">
          <div class="login-card">
            <div class="login-logo-ring">🏛️</div>
            <div class="login-ministry">{t('ministry')}</div>
            <div class="login-title">{t('portal_title')}</div>
            <div class="login-divider"></div>
            <div style="text-align:center;margin-bottom:20px;">
              <span class="classified-tag">🔒 {t('classified')}</span>
            </div>
            <div class="login-subtitle">{t('login_prompt')}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Wrap the form in the login-card class via a container
        with st.container():
            st.markdown('<div class="login-card" style="margin-top:-32px;">', unsafe_allow_html=True)
            with st.form("login_form", clear_on_submit=False):
                email_in  = st.text_input(t("email_field"), placeholder="admin  ·  auditor@mof.gov")
                pass_in   = st.text_input(t("password_field"), type="password", placeholder="••••••••")
                submitted = st.form_submit_button(f"🔐  {t('sign_in')}", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        if submitted:
            role = authenticate(email_in, pass_in, spreadsheet_id)
            if role:
                st.session_state.logged_in  = True
                st.session_state.user_email = "Admin" if role == "admin" else email_in.lower().strip()
                st.session_state.user_role  = role
                st.rerun()
            else:
                st.error(t("bad_creds"))

def render_sidebar(
    headers: list, col_binder: str | None, col_company: str | None, col_license: str | None,
    is_admin: bool, fetched_at: str,
) -> tuple:
    with st.sidebar:
        # ── Header ───────────────────────────────────────────────────────────
        st.markdown(f"""
        <div class="sidebar-header">
          <div class="sidebar-logo-text">🏛️&nbsp; {t('portal_title')}</div>
          <div class="sidebar-ministry">{t('ministry')}</div>
        </div>
        <hr class="sb-divider"/>""", unsafe_allow_html=True)

        # ── Cache status strip ────────────────────────────────────────────────
        st.markdown(f"""
        <div class="cache-strip">
          <span class="cache-badge">⚡ {t('local_mode')}</span>
          <div class="cache-info">
            {t('cache_age')}: {READ_TTL//60} min · Last read: {fetched_at[-8:] if fetched_at else '—'}
          </div>
        </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # ── Language toggle ───────────────────────────────────────────────────
        st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"):
            st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang = "ku"; st.rerun()

        st.markdown("<hr style='border-top:1px solid #E2E8F0;margin:16px 0;'/>", unsafe_allow_html=True)

        # ── Advanced Filters ──────────────────────────────────────────────────
        st.markdown(f"<div class='adv-filter-header'>🔍 {t('adv_filters')}</div>",
                    unsafe_allow_html=True)

        status_opts = {
            "all":     t("status_all"),
            "pending": t("status_pending"),
            "done":    t("status_done"),
        }
        f_status = st.selectbox(
            t("f_status"),
            options=list(status_opts.keys()),
            format_func=lambda k: status_opts[k],
            key="f_status",
        )

        for key, label, hint, disabled in [
            ("f_email",   t("f_email"),   COL_AUDITOR,        False),
            ("f_binder",  t("f_binder"),  col_binder  or "—", col_binder  is None),
            ("f_company", t("f_company"), col_company or "—", col_company is None),
            ("f_license", t("f_license"), col_license or "—", col_license is None),
        ]:
            st.markdown(
                f"<div class='sb-label' style='margin-top:10px;'>{label}"
                f"<span class='col-hint'> ({hint})</span></div>",
                unsafe_allow_html=True)
            st.text_input(label, key=key, disabled=disabled, label_visibility="collapsed")

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button(f"✕  {t('clear_filters')}", use_container_width=True, key="clr_f"):
            for k in ("f_email", "f_binder", "f_company", "f_license"):
                st.session_state[k] = ""
            st.session_state["f_status"] = "all"
            st.rerun()

        st.markdown("<hr style='border-top:1px solid #E2E8F0;margin:16px 0;'/>", unsafe_allow_html=True)

        # ── User card ─────────────────────────────────────────────────────────
        role_label = t("role_admin") if is_admin else t("role_auditor")
        chip_cls   = "chip-admin"    if is_admin else "chip-audit"
        st.markdown(f"""
        <div class="sb-user-card">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="chip {chip_cls}" style="margin-top:7px;">{role_label}</span>
        </div>""", unsafe_allow_html=True)

        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k, v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()

    return (
        st.session_state.get("f_email",   ""),
        st.session_state.get("f_binder",  ""),
        st.session_state.get("f_company", ""),
        st.session_state.get("f_license", ""),
        st.session_state.get("f_status",  "all"),
    )

def render_filter_bar(
    total: int, filtered: int,
    f_email, f_binder, f_company, f_license, f_status,
) -> None:
    n = _n_active(f_email, f_binder, f_company, f_license, f_status)
    if n == 0:
        return
    badges = ""
    if f_status != "all":  badges += f"<span class='filter-badge'>⚡ {f_status}</span> "
    if f_email.strip():    badges += f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span> "
    if f_binder.strip():   badges += f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span> "
    if f_company.strip():  badges += f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span> "
    if f_license.strip():  badges += f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span> "
    st.markdown(f"""
    <div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:700;color:#1E3A8A;
                   text-transform:uppercase;letter-spacing:.08em;">
        {t('active_filters')} ({n})
      </span>
      {badges}
      <span class="result-count">
        <strong style="color:#1E3A8A;">{filtered}</strong>/{total}&nbsp;{t('results_shown')}
      </span>
    </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  15 · TAB: AUDIT WORKLIST
# ─────────────────────────────────────────────────────────────────────────────
def render_worklist(
    pending_display: pd.DataFrame, df: pd.DataFrame,
    headers: list, col_map: dict, ws_title: str,
    f_email, f_binder, f_company, f_license, f_status,
) -> None:
    p_count = len(pending_display)
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">📋 {t('worklist_title')}</div>
        <div class="worklist-sub">{t('worklist_sub')}</div>
      </div>
      <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
    </div>""", unsafe_allow_html=True)

    if pending_display.empty:
        msg = (t("no_match") if _n_active(f_email, f_binder, f_company, f_license, f_status)
               else "✅  All cases in this register have been processed.")
        st.info(msg)
        return

    render_html_table(pending_display)
    st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>", unsafe_allow_html=True)

    label_col = next((h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row")
    opts = ["—"] + [
        f"Row {idx}  ·  {str(row.get(label_col, ''))[:55]}"
        for idx, row in pending_display.iterrows()
    ]
    row_sel = st.selectbox("", opts, key="row_sel", label_visibility="collapsed")
    if row_sel == "—":
        return

    sheet_row = int(row_sel.split("  ·  ")[0].replace("Row ", "").strip())
    df_iloc   = sheet_row - 2
    if df_iloc < 0 or df_iloc >= len(df):
        st.error("Row index out of range. The sheet may have been updated — please wait for the next cache refresh.")
        return

    record = df.iloc[df_iloc].to_dict()

    with st.expander(f"📜  {t('audit_trail')}", expanded=False):
        history = str(record.get(COL_LOG, "")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip():
                    st.markdown(f'<div class="log-line">{line}</div>', unsafe_allow_html=True)
        else:
            st.caption(t("no_history"))

    st.markdown(f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>",
                unsafe_allow_html=True)

    SKIP   = set(SYSTEM_COLS)
    fields = {k: v for k, v in record.items() if k not in SKIP}

    with st.form("audit_form"):
        new_vals: dict[str, str] = {}
        for fname, fval in fields.items():
            new_vals[fname] = st.text_input(fname, value=clean_cell(fval), key=f"field_{fname}")
        do_submit = st.form_submit_button(f"✅  {t('approve_save')}", use_container_width=True)

    if do_submit:
        ts_now     = now_str()
        auditor    = st.session_state.user_email
        log_prefix = f"✔  {auditor}  |  {ts_now}"

        with st.spinner("Committing record to Google Sheets…"):
            try:
                write_approval_to_sheet(
                    ws_title, sheet_row, col_map, headers,
                    new_vals, record, auditor, ts_now, log_prefix,
                )
            except gspread.exceptions.APIError as e:
                st.error(f"🚨 Write failed after {BACKOFF_MAX} retries: {e}")
                return

        _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix)
        st.success(t("saved_ok"))
        time.sleep(0.6)
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
#  16 · TAB: PROCESSED ARCHIVE
# ─────────────────────────────────────────────────────────────────────────────
def render_archive(
    done_view: pd.DataFrame, df: pd.DataFrame, col_map: dict, ws_title: str,
    is_admin: bool, f_email, f_binder, f_company, f_license, f_status,
) -> None:
    d_count = len(done_view)
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">✅ Processed Archive</div>
        <div class="worklist-sub">Completed and committed audit records</div>
      </div>
      <span class="chip chip-done">{d_count} {t('processed')}</span>
    </div>""", unsafe_allow_html=True)

    if done_view.empty:
        msg = (t("no_match") if _n_active(f_email, f_binder, f_company, f_license, f_status)
               else "No processed records yet.")
        st.info(msg)
    else:
        render_html_table(done_view)

    if is_admin and not done_view.empty:
        st.markdown("---")
        st.markdown(f"<div class='section-title'>↩️ {t('reopen')}</div>", unsafe_allow_html=True)
        reopen_opts = ["—"] + [f"Row {idx}" for idx in done_view.index]
        reopen_sel  = st.selectbox("Select record to re-open:", reopen_opts, key="reopen_sel")
        if reopen_sel != "—":
            ridx    = int(reopen_sel.split(" ")[1])
            df_iloc = ridx - 2
            if st.button(t("reopen"), key="reopen_btn"):
                with st.spinner("Re-opening record…"):
                    try:
                        write_reopen_to_sheet(ws_title, ridx, col_map)
                    except gspread.exceptions.APIError as e:
                        st.error(f"🚨 Write failed: {e}")
                        return
                _apply_optimistic_reopen(df_iloc)
                st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
#  17 · TAB: ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df: pd.DataFrame) -> None:
    pt  = "plotly_white"
    pb  = "#FFFFFF"
    pg  = "#E2E8F0"
    fc  = "#0F172A"
    nvy = "#1E3A8A"
    blu = "#3B82F6"

    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>", unsafe_allow_html=True)
    periods = [("all", t("all_time")), ("today", t("today")),
               ("this_week", t("this_week")), ("this_month", t("this_month"))]
    for cw, (pk, pl) in zip(st.columns(len(periods)), periods):
        lbl = f"✓  {pl}" if st.session_state.date_filter == pk else pl
        if cw.button(lbl, use_container_width=True, key=f"pf_{pk}"):
            st.session_state.date_filter = pk; st.rerun()

    done_base = df[df[COL_STATUS] == VAL_DONE].copy()
    done_f    = apply_period_filter(done_base, COL_DATE, st.session_state.date_filter)

    if done_f.empty:
        st.info(t("no_records")); return

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
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>", unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = done_f[COL_AUDITOR].replace("", "—").value_counts().reset_index()
            lb.columns = ["Auditor", "Count"]
            medals = ["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i, r in lb.head(10).iterrows():
                m = medals[i] if i < len(medals) else f"{i+1}."
                st.markdown(
                    f'<div class="lb-row">'
                    f'<span class="lb-medal">{m}</span>'
                    f'<span class="lb-name">{r["Auditor"]}</span>'
                    f'<span class="lb-count">{r["Count"]}</span></div>',
                    unsafe_allow_html=True)
            fig_lb = px.bar(
                lb.head(10), x="Count", y="Auditor", orientation="h",
                color="Count", color_continuous_scale=[blu, nvy], template=pt,
            )
            fig_lb.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Inter", color=fc, size=11),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=8, r=8, t=10, b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#475569")),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color="#475569")),
                height=min(320, max(180, 36 * len(lb.head(10)))),
            )
            fig_lb.update_traces(marker_line_width=0)
            st.plotly_chart(fig_lb, use_container_width=True)

    with right:
        st.markdown(f"<div class='section-title'>📈 {t('daily_trend')}</div>", unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f = done_f.copy()
            done_f["_date"] = done_f[COL_DATE].apply(
                lambda s: parse_dt(s).date() if parse_dt(s) else None)
            trend = (done_f.dropna(subset=["_date"])
                     .groupby("_date").size()
                     .reset_index(name="Records"))
            trend.columns = ["Date", "Records"]
            if not trend.empty:
                if len(trend) > 1:
                    full_rng = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = (trend.set_index("Date")
                             .reindex(full_rng.date, fill_value=0)
                             .reset_index())
                    trend.columns = ["Date", "Records"]
                fig_line = go.Figure()
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="none", fill="tozeroy",
                    fillcolor="rgba(59,130,246,0.08)", showlegend=False,
                ))
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="lines+markers",
                    line=dict(color=nvy, width=2.5),
                    marker=dict(color=blu, size=7, line=dict(color="#FFFFFF", width=2)),
                    name=t("records_period"),
                ))
                fig_line.update_layout(
                    template=pt, paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="Inter", color=fc, size=11),
                    showlegend=False, margin=dict(l=8, r=8, t=10, b=8),
                    xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#475569")),
                    yaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color="#475569")),
                    height=380, hovermode="x unified",
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info(t("no_records"))

# ─────────────────────────────────────────────────────────────────────────────
#  18 · TAB: USER ADMINISTRATION
# ─────────────────────────────────────────────────────────────────────────────
def render_user_admin(spreadsheet_id: str) -> None:
    spreadsheet = get_spreadsheet()
    users_ws    = spreadsheet.worksheet(USERS_SHEET)

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_email = st.text_input("Email", placeholder="auditor@mof.gov")
            nu_pass  = st.text_input("Password", type="password")
            if st.form_submit_button("Register Auditor", use_container_width=True):
                if nu_email.strip() and nu_pass.strip():
                    recs    = _fetch_users_cached(spreadsheet_id)
                    df_u    = pd.DataFrame(recs)
                    already = (not df_u.empty and
                               nu_email.lower().strip() in df_u.get("email", pd.Series()).values)
                    if already:
                        st.error(t("dup_email"))
                    else:
                        _gsheets_call(users_ws.append_row,
                                      [nu_email.lower().strip(), hash_pw(nu_pass.strip()), now_str()])
                        st.success(f"✅  {nu_email} registered.")
                        time.sleep(0.7); st.rerun()
                else:
                    st.warning(t("fill_fields"))

        st.markdown(f"<div class='section-title'>🔑 {t('update_pw')}</div>", unsafe_allow_html=True)
        recs_pw  = _fetch_users_cached(spreadsheet_id)
        staff_df = pd.DataFrame(recs_pw)
        if not staff_df.empty and "email" in staff_df.columns:
            with st.form("upd_pw_form"):
                sel_email = st.selectbox("Select staff", staff_df["email"].tolist())
                new_pw    = st.text_input("New Password", type="password")
                if st.form_submit_button("Update Password", use_container_width=True):
                    if new_pw.strip():
                        cell = _gsheets_call(users_ws.find, sel_email)
                        if cell:
                            _gsheets_call(users_ws.update_cell, cell.row, 2, hash_pw(new_pw.strip()))
                            st.success(f"✅  Password updated for {sel_email}.")
                            time.sleep(0.7); st.rerun()

    with col_right:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>", unsafe_allow_html=True)
        recs_dir = _fetch_users_cached(spreadsheet_id)
        staff_df = pd.DataFrame(recs_dir)
        if not staff_df.empty and "email" in staff_df.columns:
            safe_cols = [c for c in ["email", "created_at"] if c in staff_df.columns]
            render_html_table(staff_df[safe_cols].reset_index())
            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>",
                        unsafe_allow_html=True)
            del_email = st.selectbox(
                "Select to revoke", ["—"] + staff_df["email"].tolist(), key="del_sel")
            if del_email != "—":
                if st.button(f"Revoke access — {del_email}", key="del_btn"):
                    cell = _gsheets_call(users_ws.find, del_email)
                    if cell:
                        _gsheets_call(users_ws.delete_rows, cell.row)
                        st.success(f"✅  {del_email} access revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered yet.")

# ─────────────────────────────────────────────────────────────────────────────
#  19 · MAIN CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        spreadsheet    = get_spreadsheet()
        spreadsheet_id = spreadsheet.id
        all_ws_titles  = [ws.title for ws in spreadsheet.worksheets()]

        # Ensure UsersDB exists
        if USERS_SHEET not in all_ws_titles:
            uw = spreadsheet.add_worksheet(title=USERS_SHEET, rows="500", cols="3")
            _gsheets_call(uw.append_row, ["email", "password", "created_at"])

        # ── AUTHENTICATION GATE ──────────────────────────────────────────────
        if not st.session_state.logged_in:
            render_login(spreadsheet_id)
            return

        st.markdown(
            "<style>[data-testid='stSidebar']{display:flex!important;}</style>",
            unsafe_allow_html=True)

        is_admin = st.session_state.user_role == "admin"

        # ── Page header ──────────────────────────────────────────────────────
        ts_str = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-header">
          <div>
            <div class="page-title">🏛️  {t('portal_title')}</div>
            <div class="page-subtitle">{t('ministry')}</div>
          </div>
          <div class="page-timestamp">{ts_str}</div>
        </div>""", unsafe_allow_html=True)

        # ── Workspace selection (HARDCODED — no dynamic manager) ─────────────
        df         = pd.DataFrame()
        headers    = []
        col_map    = {}
        ws_title   = None
        fetched_at = "—"

        # ⚡ گۆڕانکاری زیرەک بۆ دۆزینەوەی ناوەکان بێ گوێدانە سپەیس
        actual_titles_map = {t.strip().lower(): t for t in all_ws_titles}
        available = []
        for s in VISIBLE_SHEETS:
            s_clean = s.strip().lower()
            if s_clean in actual_titles_map:
                available.append(actual_titles_map[s_clean])

        if not available:
            st.warning("None of the configured worksheets were found in the spreadsheet. "
                       "Please ensure these sheets exist: " + ", ".join(VISIBLE_SHEETS))
            st.error(f"⚠️ **زانیاری بۆ ئەدمین:** گۆگڵ شیت ئەم ناوانەی خوارەوەی بۆ ناردووین، پێدەچێت ناوەکانت سپەیسی زیادەیان تێدابێت یان جیاواز بن:\n\n `{all_ws_titles}`")
        else:
            ws_title = st.selectbox(
                t("workspace"), available, key="ws_sel",
                format_func=lambda s: s,
            )
            if ws_title:
                # Invalidate local copy when user switches sheets
                ws_cache_key = f"ws_title::{ws_title}"
                if st.session_state.get("active_ws_key") != ws_cache_key:
                    st.session_state.local_cache_key = None
                    st.session_state.active_ws_key   = ws_cache_key

                try:
                    df, headers, col_map, fetched_at = get_local_data(
                        spreadsheet_id, ws_title)
                except gspread.exceptions.WorksheetNotFound:
                    st.error(f"Worksheet '{ws_title}' not found in the spreadsheet.")
                except gspread.exceptions.APIError as e:
                    st.error(f"🚨 {t('retry_warning')}\n\n{e}")

        col_binder  = detect_column(headers, "binder")
        col_company = detect_column(headers, "company")
        col_license = detect_column(headers, "license")

        f_email, f_binder, f_company, f_license, f_status = render_sidebar(
            headers, col_binder, col_company, col_license, is_admin, fetched_at,
        )

        # ── Case Overview Metrics ─────────────────────────────────────────────
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
            <div class="prog-labels">
              <span>{t('processed')}</span><span>{int(pct*100)}%</span>
            </div>
            <div class="prog-wrap">
              <div class="prog-fill" style="width:{int(pct*100)}%;"></div>
            </div>""", unsafe_allow_html=True)

            filtered_df = apply_filters_locally(
                df, f_email, f_binder, f_company, f_license, f_status,
                col_binder, col_company, col_license,
            )
            render_filter_bar(
                total_n, len(filtered_df),
                f_email, f_binder, f_company, f_license, f_status,
            )
        else:
            filtered_df = pd.DataFrame()

        # ── Role-based tabs ───────────────────────────────────────────────────
        if is_admin:
            tabs = st.tabs([t("tab_worklist"), t("tab_archive"),
                            t("tab_analytics"), t("tab_users")])
            t_work, t_arch, t_anal, t_uadm = tabs
        else:
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>",
                        unsafe_allow_html=True)
            tabs = st.tabs([t("tab_worklist"), t("tab_archive")])
            t_work, t_arch = tabs
            t_anal = t_uadm = None

        # ── Worklist ──────────────────────────────────────────────────────────
        with t_work:
            if df.empty or ws_title is None:
                pass # Warning is already shown above or handled
            else:
                pending_view    = filtered_df[filtered_df[COL_STATUS] != VAL_DONE].copy()
                pending_display = pending_view.copy()
                pending_display.index = pending_display.index + 2
                render_worklist(
                    pending_display, df, headers, col_map, ws_title,
                    f_email, f_binder, f_company, f_license, f_status,
                )

        # ── Archive ───────────────────────────────────────────────────────────
        with t_arch:
            if df.empty or ws_title is None:
                pass
            else:
                done_view = filtered_df[filtered_df[COL_STATUS] == VAL_DONE].copy()
                done_view.index = done_view.index + 2
                render_archive(
                    done_view, df, col_map, ws_title, is_admin,
                    f_email, f_binder, f_company, f_license, f_status,
                )

        if is_admin and t_anal is not None:
            with t_anal:
                if df.empty:
                    pass
                else:
                    render_analytics(df)

        if is_admin and t_uadm is not None:
            with t_uadm:
                render_user_admin(spreadsheet_id)

    except Exception as exc:
        st.error(f"🚨  System Error: {exc}")
        with st.expander("Technical Details", expanded=False):
            st.exception(exc)

# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
