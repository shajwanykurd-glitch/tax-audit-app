# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  ·  v11.0 (Premium SaaS UI)
#  Architecture: Optimistic UI / Local-First Mutation
#
#  Rule 1 — READ ONCE, NEVER BUST      (@st.cache_data ttl=600, zero .clear())
#  Rule 2 — OPTIMISTIC LOCAL MUTATION  (session_state.local_df, no re-fetch)
#  Rule 3 — EXPONENTIAL BACKOFF        (tenacity on every gspread call)
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
#  3 · CONSTANTS  (UNCHANGED — hardcoded sheets, no dynamic manager)
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_SHEETS  = {"UsersDB"}
USERS_SHEET    = "UsersDB"
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
#  4 · EXPONENTIAL BACKOFF DECORATOR  (UNCHANGED)
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
#  5 · PREMIUM SaaS CSS  (Stripe / Vercel / Linear aesthetic)
# ─────────────────────────────────────────────────────────────────────────────
def inject_css() -> None:
    st.markdown("""
<style>
/* ══════════════════════════════════════════════════════════════════════════
   FONTS
══════════════════════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

/* ══════════════════════════════════════════════════════════════════════════
   CSS TOKENS
══════════════════════════════════════════════════════════════════════════ */
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

/* ══════════════════════════════════════════════════════════════════════════
   GLOBAL RESET
══════════════════════════════════════════════════════════════════════════ */
*, *::before, *::after {
  box-sizing: border-box !important;
  font-family: var(--font) !important;
}
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"], .main, .block-container {
  background-color: var(--bg) !important;
  color: var(--text-primary) !important;
}
p, span, div, li, label, h1, h2, h3, h4, h5, h6,
.stMarkdown, [data-testid="stMarkdownContainer"] {
  color: var(--text-primary) !important;
}

/* ── Hide Streamlit chrome ────────────────────────────────────────────── */
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"],
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"] {
  display: none !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   SIDEBAR
══════════════════════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {
  background-color: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
  box-shadow: 4px 0 24px rgba(0,0,0,0.04) !important;
}
[data-testid="stSidebar"] * { color: var(--text-primary) !important; }

/* ══════════════════════════════════════════════════════════════════════════
   INPUTS
══════════════════════════════════════════════════════════════════════════ */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
  background: var(--surface) !important;
  color: var(--text-primary) !important;
  border: 1.5px solid var(--border-2) !important;
  border-radius: var(--radius-md) !important;
  font-size: 0.875rem !important;
  font-weight: 500 !important;
  padding: 11px 14px !important;
  box-shadow: var(--shadow-sm) !important;
  transition: border-color 0.18s ease, box-shadow 0.18s ease !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
  border-color: var(--indigo-500) !important;
  box-shadow: var(--ring) !important;
  outline: none !important;
}
.stTextInput > div > div > input::placeholder,
.stTextArea > div > div > textarea::placeholder {
  color: var(--text-muted) !important;
  font-weight: 400 !important;
}
.stTextInput > label, .stTextArea > label,
.stSelectbox > label, .stMultiSelect > label {
  color: var(--text-secondary) !important;
  font-size: 0.72rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.07em !important;
  text-transform: uppercase !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   SELECTBOX
══════════════════════════════════════════════════════════════════════════ */
.stSelectbox > div > div, [data-baseweb="select"] > div {
  background: var(--surface) !important;
  color: var(--text-primary) !important;
  border-color: var(--border-2) !important;
  border-radius: var(--radius-md) !important;
  box-shadow: var(--shadow-sm) !important;
  font-weight: 500 !important;
}
[data-baseweb="menu"] li,
[data-baseweb="menu"] [role="option"] {
  background: var(--surface) !important;
  color: var(--text-primary) !important;
  font-size: 0.875rem !important;
}
[data-baseweb="menu"] li:hover,
[data-baseweb="menu"] [aria-selected="true"] {
  background: var(--indigo-50) !important;
  color: var(--indigo-600) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   METRIC CARDS
══════════════════════════════════════════════════════════════════════════ */
[data-testid="stMetricContainer"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-top: 3px solid var(--indigo-500) !important;
  border-radius: var(--radius-lg) !important;
  padding: 22px 26px !important;
  box-shadow: var(--shadow-md) !important;
  transition: transform 0.22s cubic-bezier(0.34,1.56,0.64,1),
              box-shadow 0.22s ease !important;
}
[data-testid="stMetricContainer"]:hover {
  transform: translateY(-4px) !important;
  box-shadow: 0 16px 40px rgba(99,102,241,0.14), 0 4px 12px rgba(0,0,0,0.06) !important;
}
[data-testid="stMetricValue"] {
  font-size: 2.1rem !important;
  font-weight: 800 !important;
  color: var(--indigo-600) !important;
  letter-spacing: -0.03em !important;
  line-height: 1.1 !important;
}
[data-testid="stMetricLabel"] {
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.10em !important;
  text-transform: uppercase !important;
  color: var(--text-muted) !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem !important; font-weight: 600 !important; }

/* ══════════════════════════════════════════════════════════════════════════
   BUTTONS
══════════════════════════════════════════════════════════════════════════ */
.stButton > button {
  background: linear-gradient(135deg, var(--indigo-600) 0%, var(--blue-500) 100%) !important;
  color: #FFFFFF !important;
  border: none !important;
  border-radius: var(--radius-md) !important;
  font-weight: 700 !important;
  font-size: 0.84rem !important;
  padding: 10px 20px !important;
  letter-spacing: 0.01em !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.35) !important;
  transition: all 0.18s cubic-bezier(0.34,1.56,0.64,1) !important;
}
.stButton > button:hover {
  background: linear-gradient(135deg, var(--indigo-700) 0%, var(--indigo-600) 100%) !important;
  transform: translateY(-2px) scale(1.01) !important;
  box-shadow: 0 8px 24px rgba(99,102,241,0.45) !important;
}
.stButton > button:active {
  transform: translateY(0) scale(0.99) !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.25) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   FORMS
══════════════════════════════════════════════════════════════════════════ */
div[data-testid="stForm"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-lg) !important;
  padding: 28px 32px !important;
  box-shadow: var(--shadow-md) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   TABS  (pill style)
══════════════════════════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
  gap: 2px !important;
  background: var(--surface-2) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-full) !important;
  padding: 4px !important;
  width: fit-content !important;
  box-shadow: var(--shadow-sm) !important;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important;
  color: var(--text-muted) !important;
  border-radius: var(--radius-full) !important;
  border: none !important;
  padding: 8px 22px !important;
  font-weight: 600 !important;
  font-size: 0.82rem !important;
  transition: all 0.18s ease !important;
}
.stTabs [data-baseweb="tab"]:hover {
  color: var(--text-primary) !important;
  background: rgba(0,0,0,0.04) !important;
}
.stTabs [aria-selected="true"] {
  background: var(--surface) !important;
  color: var(--indigo-600) !important;
  border: none !important;
  box-shadow: var(--shadow-sm) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   EXPANDER
══════════════════════════════════════════════════════════════════════════ */
.streamlit-expanderHeader {
  background: var(--surface-2) !important;
  color: var(--text-primary) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  font-weight: 600 !important;
  font-size: 0.85rem !important;
}
.streamlit-expanderContent {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-top: none !important;
  border-radius: 0 0 var(--radius-md) var(--radius-md) !important;
  padding: 18px !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   ALERTS
══════════════════════════════════════════════════════════════════════════ */
[data-testid="stAlert"] {
  border-radius: var(--radius-md) !important;
  border: 1px solid var(--border) !important;
  background: var(--surface) !important;
  box-shadow: var(--shadow-sm) !important;
}
[data-testid="stAlert"] * { color: var(--text-primary) !important; }

/* ══════════════════════════════════════════════════════════════════════════
   LOGIN PAGE — animated mesh orbs + glassmorphism card
══════════════════════════════════════════════════════════════════════════ */
.login-bg {
  position: fixed;
  inset: 0;
  z-index: -1;
  background: linear-gradient(145deg, #EEF2FF 0%, #F0F4FF 40%, #EFF6FF 100%);
  overflow: hidden;
}
.login-bg::before {
  content: '';
  position: absolute;
  width: 900px; height: 900px;
  top: -200px; left: -200px;
  background: radial-gradient(circle, rgba(99,102,241,0.13) 0%, transparent 70%);
  animation: orb1 18s ease-in-out infinite alternate;
}
.login-bg::after {
  content: '';
  position: absolute;
  width: 700px; height: 700px;
  bottom: -150px; right: -100px;
  background: radial-gradient(circle, rgba(59,130,246,0.10) 0%, transparent 70%);
  animation: orb2 22s ease-in-out infinite alternate;
}
@keyframes orb1 {
  from { transform: translate(0,0) scale(1); }
  to   { transform: translate(80px,60px) scale(1.1); }
}
@keyframes orb2 {
  from { transform: translate(0,0) scale(1); }
  to   { transform: translate(-60px,-80px) scale(1.08); }
}

/* Glassmorphism card */
.login-card {
  width: 100%;
  max-width: 460px;
  background: rgba(255,255,255,0.88);
  backdrop-filter: blur(24px) saturate(1.8);
  -webkit-backdrop-filter: blur(24px) saturate(1.8);
  border: 1px solid rgba(255,255,255,0.72);
  border-radius: var(--radius-xl);
  padding: 52px 48px 44px;
  box-shadow:
    0 32px 80px rgba(15,23,42,0.10),
    0 8px 32px rgba(99,102,241,0.08),
    inset 0 1px 0 rgba(255,255,255,0.9);
}
.login-logo {
  width: 72px; height: 72px;
  margin: 0 auto 22px;
  border-radius: 20px;
  background: linear-gradient(135deg, var(--indigo-600) 0%, var(--blue-500) 100%);
  display: flex; align-items: center; justify-content: center;
  font-size: 2rem;
  box-shadow:
    0 12px 32px rgba(99,102,241,0.40),
    0 4px 12px rgba(99,102,241,0.20),
    inset 0 1px 0 rgba(255,255,255,0.25);
}
.login-eyebrow {
  text-align: center;
  font-size: 0.60rem;
  font-weight: 800;
  letter-spacing: 0.22em;
  text-transform: uppercase;
  color: var(--text-muted) !important;
  margin-bottom: 8px;
}
.login-title {
  text-align: center;
  font-size: 1.45rem;
  font-weight: 800;
  color: var(--text-primary) !important;
  letter-spacing: -0.03em;
  margin-bottom: 6px;
}
.login-sub {
  text-align: center;
  font-size: 0.82rem;
  color: var(--text-muted) !important;
  margin-bottom: 28px;
  font-weight: 400;
}
.login-rule {
  width: 44px; height: 3px;
  background: linear-gradient(90deg, var(--indigo-600), var(--blue-400));
  border-radius: 99px;
  margin: 0 auto 24px;
}
.classified-pill {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  background: var(--red-50);
  color: var(--red-600) !important;
  border: 1px solid var(--red-200);
  border-radius: var(--radius-full);
  font-size: 0.58rem;
  font-weight: 800;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  padding: 5px 14px;
  margin: 0 auto 22px;
  width: fit-content;
}
.login-card .stButton > button {
  background: linear-gradient(135deg, var(--indigo-600) 0%, var(--blue-500) 100%) !important;
  color: #FFFFFF !important;
  font-weight: 800 !important;
  font-size: 0.94rem !important;
  border-radius: var(--radius-md) !important;
  padding: 14px 20px !important;
  width: 100% !important;
  border: none !important;
  letter-spacing: 0.02em !important;
  box-shadow: 0 4px 16px rgba(99,102,241,0.40), 0 1px 4px rgba(99,102,241,0.20) !important;
  transition: all 0.20s cubic-bezier(0.34,1.56,0.64,1) !important;
}
.login-card .stButton > button:hover {
  background: linear-gradient(135deg, var(--indigo-700) 0%, var(--indigo-600) 100%) !important;
  transform: translateY(-3px) scale(1.01) !important;
  box-shadow: 0 12px 32px rgba(99,102,241,0.50), 0 4px 12px rgba(99,102,241,0.30) !important;
}
.login-card .stButton > button:active {
  transform: translateY(0) scale(0.99) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   PAGE HEADER
══════════════════════════════════════════════════════════════════════════ */
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 0 24px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 28px;
}
.page-title {
  font-size: 1.55rem;
  font-weight: 800;
  color: var(--text-primary) !important;
  letter-spacing: -0.03em;
  margin: 0;
}
.page-subtitle {
  font-size: 0.78rem;
  color: var(--text-muted) !important;
  margin-top: 4px;
  font-weight: 500;
}
.page-timestamp {
  font-size: 0.74rem;
  color: var(--text-muted) !important;
  font-weight: 600;
  background: var(--surface);
  padding: 7px 16px;
  border-radius: var(--radius-full);
  border: 1px solid var(--border);
  box-shadow: var(--shadow-sm);
  font-family: var(--mono) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   SECTION TITLE
══════════════════════════════════════════════════════════════════════════ */
.section-title {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 0.70rem;
  font-weight: 800;
  color: var(--indigo-600) !important;
  margin: 24px 0 14px;
  padding: 6px 14px 6px 10px;
  border-left: 3px solid var(--indigo-500);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  background: var(--indigo-50);
  text-transform: uppercase;
  letter-spacing: 0.07em;
}

/* ══════════════════════════════════════════════════════════════════════════
   WORKLIST HEADER
══════════════════════════════════════════════════════════════════════════ */
.worklist-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: var(--surface);
  border: 1px solid var(--border);
  border-top: 3px solid var(--indigo-500);
  border-radius: var(--radius-lg);
  padding: 18px 24px;
  margin-bottom: 18px;
  box-shadow: var(--shadow-md);
}
.worklist-title { font-size: 1.02rem; font-weight: 800; color: var(--text-primary) !important; }
.worklist-sub   { font-size: 0.76rem; color: var(--text-muted) !important; margin-top: 3px; }

/* ══════════════════════════════════════════════════════════════════════════
   PROGRESS BAR
══════════════════════════════════════════════════════════════════════════ */
.prog-wrap {
  background: var(--border);
  border-radius: var(--radius-full);
  height: 7px;
  overflow: hidden;
  margin: 6px 0 12px;
}
.prog-fill {
  height: 100%;
  border-radius: var(--radius-full);
  background: linear-gradient(90deg, var(--indigo-600), var(--blue-400));
  transition: width 0.8s cubic-bezier(0.4,0,0.2,1);
  box-shadow: 0 0 12px rgba(99,102,241,0.40);
}
.prog-labels {
  display: flex;
  justify-content: space-between;
  font-size: 0.72rem;
  color: var(--text-muted) !important;
  font-weight: 600;
  margin-bottom: 4px;
}

/* ══════════════════════════════════════════════════════════════════════════
   STATUS CHIPS
══════════════════════════════════════════════════════════════════════════ */
.chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 12px;
  border-radius: var(--radius-full);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}
.chip-done    { background: var(--green-50);  color: var(--green-700)  !important; border: 1px solid var(--green-200); }
.chip-pending { background: var(--amber-50);  color: var(--amber-700)  !important; border: 1px solid var(--amber-200); }
.chip-admin   { background: var(--indigo-50); color: var(--indigo-600) !important; border: 1px solid var(--indigo-100); }
.chip-audit   { background: var(--green-50);  color: var(--green-600)  !important; border: 1px solid var(--green-200); }
.s-chip { display: inline-flex; align-items: center; padding: 3px 10px;
  border-radius: var(--radius-full); font-size: 0.63rem; font-weight: 700;
  letter-spacing: 0.05em; text-transform: uppercase; }
.s-done    { background: var(--green-50);  color: var(--green-700)  !important; border: 1px solid var(--green-200); }
.s-pending { background: var(--amber-50);  color: var(--amber-700)  !important; border: 1px solid var(--amber-200); }

/* ══════════════════════════════════════════════════════════════════════════
   DATA TABLE
══════════════════════════════════════════════════════════════════════════ */
.gov-table-wrap {
  overflow-x: auto;
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  margin-bottom: 18px;
  box-shadow: var(--shadow-md);
}
.gov-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--surface);
  font-size: 0.84rem;
}
.gov-table thead tr {
  background: var(--surface-2);
  border-bottom: 1px solid var(--border);
}
.gov-table th {
  color: var(--text-muted) !important;
  background: var(--surface-2) !important;
  font-weight: 700 !important;
  font-size: 0.63rem !important;
  letter-spacing: 0.09em !important;
  text-transform: uppercase !important;
  padding: 13px 18px !important;
  white-space: nowrap;
  text-align: left !important;
}
.gov-table td {
  color: var(--text-primary) !important;
  background: var(--surface) !important;
  padding: 11px 18px !important;
  font-size: 0.84rem !important;
  font-weight: 500 !important;
  border-bottom: 1px solid var(--border) !important;
  vertical-align: middle !important;
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  transition: background 0.14s ease !important;
}
.gov-table tbody tr:nth-child(even) td { background: #FBFCFF !important; }
.gov-table tbody tr:hover td {
  background: var(--indigo-50) !important;
  color: var(--text-primary) !important;
}
.gov-table tbody tr:last-child td { border-bottom: none !important; }
.gov-table td.row-idx, .gov-table th.row-idx {
  color: var(--text-muted) !important;
  font-family: var(--mono) !important;
  font-size: 0.70rem !important;
  min-width: 50px;
  text-align: center !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   LEADERBOARD
══════════════════════════════════════════════════════════════════════════ */
.lb-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 18px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  margin-bottom: 8px;
  box-shadow: var(--shadow-sm);
  transition: all 0.18s cubic-bezier(0.34,1.56,0.64,1);
}
.lb-row:hover {
  transform: translateX(5px);
  border-color: var(--indigo-400);
  box-shadow: 0 6px 20px rgba(99,102,241,0.12);
}
.lb-medal { font-size: 1.1rem; width: 26px; text-align: center; }
.lb-name  { flex: 1; font-size: 0.85rem; font-weight: 600; color: var(--text-primary) !important; }
.lb-count {
  font-size: 0.88rem; font-weight: 800; color: var(--indigo-600) !important;
  font-family: var(--mono) !important;
  background: var(--indigo-50);
  padding: 3px 10px;
  border-radius: var(--radius-full);
  border: 1px solid var(--indigo-100);
}

/* ══════════════════════════════════════════════════════════════════════════
   AUDIT LOG
══════════════════════════════════════════════════════════════════════════ */
.log-line {
  font-family: var(--mono) !important;
  font-size: 0.74rem;
  color: var(--text-secondary) !important;
  padding: 6px 0;
  border-bottom: 1px dashed var(--border);
  line-height: 1.5;
}
.log-line:last-child { border-bottom: none; }

/* ══════════════════════════════════════════════════════════════════════════
   SIDEBAR COMPONENTS
══════════════════════════════════════════════════════════════════════════ */
.sidebar-header {
  border-top: 3px solid var(--indigo-500);
  padding: 20px 18px 16px;
}
.sidebar-logo-text {
  font-size: 0.98rem;
  font-weight: 800;
  color: var(--text-primary) !important;
  letter-spacing: -0.02em;
  margin-bottom: 3px;
}
.sidebar-ministry {
  font-size: 0.60rem;
  color: var(--text-muted) !important;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-weight: 600;
}
.sb-label {
  font-size: 0.62rem;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--text-muted) !important;
  margin-bottom: 5px;
}
.sb-email {
  font-size: 0.85rem;
  font-weight: 700;
  color: var(--text-primary) !important;
  word-break: break-all;
}
.sb-user-card {
  background: linear-gradient(135deg, var(--indigo-50) 0%, #F5F0FF 100%);
  border: 1px solid var(--indigo-100);
  border-radius: var(--radius-md);
  padding: 14px 16px;
  margin-bottom: 12px;
  box-shadow: var(--shadow-sm);
}
.cache-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  background: var(--green-50);
  color: var(--green-700) !important;
  border: 1px solid var(--green-200);
  border-radius: var(--radius-full);
  font-size: 0.58rem;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 3px 10px;
}
.cache-info {
  font-size: 0.62rem;
  color: var(--text-muted) !important;
  margin-top: 6px;
  font-family: var(--mono) !important;
}
.cache-strip {
  padding: 10px 18px;
  background: var(--surface-2);
  border-bottom: 1px solid var(--border);
}

/* ══════════════════════════════════════════════════════════════════════════
   FILTERS
══════════════════════════════════════════════════════════════════════════ */
.adv-filter-header {
  font-size: 0.62rem;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--indigo-600) !important;
  margin-bottom: 12px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border);
}
.col-hint { font-size: 0.58rem; font-weight: 400; opacity: 0.55; color: var(--text-muted) !important; }
.filter-result-bar {
  background: var(--surface);
  border: 1px solid var(--border);
  border-left: 3px solid var(--indigo-500);
  border-radius: var(--radius-md);
  padding: 12px 18px;
  margin-bottom: 18px;
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  box-shadow: var(--shadow-sm);
}
.filter-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: var(--indigo-50);
  color: var(--indigo-600) !important;
  border: 1px solid var(--indigo-100);
  border-radius: var(--radius-full);
  font-size: 0.64rem;
  font-weight: 700;
  padding: 3px 10px;
}
.result-count {
  font-size: 0.76rem;
  color: var(--text-muted) !important;
  margin-left: auto;
  font-family: var(--mono) !important;
}

/* ══════════════════════════════════════════════════════════════════════════
   RBAC BANNER
══════════════════════════════════════════════════════════════════════════ */
.rbac-banner {
  background: var(--indigo-50);
  border: 1px solid var(--indigo-100);
  border-left: 3px solid var(--indigo-500);
  border-radius: var(--radius-md);
  padding: 12px 18px;
  margin-bottom: 18px;
  font-size: 0.80rem;
  color: var(--indigo-600) !important;
  font-weight: 600;
}

/* ══════════════════════════════════════════════════════════════════════════
   UTILITY
══════════════════════════════════════════════════════════════════════════ */
.divider { border: none; border-top: 1px solid var(--border); margin: 14px 0; }
</style>""", unsafe_allow_html=True)

inject_css()


# ─────────────────────────────────────────────────────────────────────────────
#  6 · TRANSLATIONS  (UNCHANGED)
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
        "local_mode": "Optimistic UI Active", "cache_age": "Cache TTL",
        "rbac_notice": "ℹ️  Auditor mode — Analytics and management tools are restricted to administrators.",
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
        "local_mode": "Optimistic UI چالاکە", "cache_age": "Cache TTL",
        "rbac_notice": "ℹ️  دیمەنی ئۆدیتۆر — ئەنالیتیکس تەنها بۆ بەڕێوەبەرەکانە.",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)


# ─────────────────────────────────────────────────────────────────────────────
#  7–12 · ALL BACKEND HELPERS  (100% UNCHANGED)
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
def detect_column(headers, kind):
    keywords = sorted(_COL_KEYWORDS.get(kind, []), key=len, reverse=True)
    for h in headers:
        hl = h.lower().strip()
        for kw in keywords:
            if kw.lower() in hl:
                return h
    return None
def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def now_str(): return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
def parse_dt(s):
    try: return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except: return None
def clean_cell(value):
    if value is None: return ""
    s = str(value)
    for ch in ("\u200b","\u200c","\u200d","\ufeff"): s = s.replace(ch,"")
    return s.replace("\xa0"," ").strip()
def _raw_to_dataframe(raw):
    if not raw: return pd.DataFrame(), [], {}
    seen = {}; headers = []
    for h in raw[0]:
        h = clean_cell(h) or "Unnamed"
        if h in seen: seen[h]+=1; headers.append(f"{h}_{seen[h]}")
        else: seen[h]=0; headers.append(h)
    if not headers: return pd.DataFrame(), [], {}
    n = len(headers); rows = []
    for r in raw[1:]:
        row = [clean_cell(c) for c in r]; row = (row+[""]*n)[:n]; rows.append(row)
    if not rows: return pd.DataFrame(columns=headers), headers, {}
    df = pd.DataFrame(rows, columns=headers)
    df = df[~(df=="").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns: df[sc] = ""
    df = df.fillna("").infer_objects(copy=False)
    return df, headers, {h: i+1 for i,h in enumerate(headers)}
def apply_period_filter(df, col, period):
    if period=="all" or col not in df.columns: return df
    now = datetime.now(TZ)
    if period=="today": cutoff = now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif period=="this_week": cutoff = (now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
    elif period=="this_month": cutoff = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    else: return df
    return df[df[col].apply(parse_dt) >= cutoff]
def _n_active(fe,fb,fc_,fl,fs): return sum([bool(fe.strip()),bool(fb.strip()),bool(fc_.strip()),bool(fl.strip()),fs!="all"])
def apply_filters_locally(df,f_email,f_binder,f_company,f_license,f_status,col_binder,col_company,col_license):
    r = df.copy()
    if f_status=="pending": r=r[r[COL_STATUS]!=VAL_DONE]
    elif f_status=="done": r=r[r[COL_STATUS]==VAL_DONE]
    if f_email.strip():
        ecols=[c for c in r.columns if "auditor_email" in c.lower() or c==COL_AUDITOR]
        if ecols:
            mask=pd.Series(False,index=r.index)
            for ec in ecols: mask|=r[ec].str.contains(f_email.strip(),case=False,na=False)
            r=r[mask]
    if f_binder.strip() and col_binder and col_binder in r.columns:
        r=r[r[col_binder].str.contains(f_binder.strip(),case=False,na=False)]
    if f_company.strip() and col_company and col_company in r.columns:
        r=r[r[col_company].str.contains(f_company.strip(),case=False,na=False)]
    if f_license.strip() and col_license and col_license in r.columns:
        r=r[r[col_license].str.contains(f_license.strip(),case=False,na=False)]
    return r

@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    raw=json.loads(st.secrets["json_key"],strict=False)
    pk=raw["private_key"]
    pk=pk.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","")
    pk=pk.replace("\\n","").replace("\n","")
    pk="".join(pk.split()); pk="\n".join(textwrap.wrap(pk,64))
    raw["private_key"]=f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds=ServiceAccountCredentials.from_json_keyfile_dict(raw,scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_raw_sheet_cached(spreadsheet_id, ws_title):
    ws=get_spreadsheet().worksheet(ws_title)
    return _gsheets_call(ws.get_all_values), now_str()

@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_users_cached(spreadsheet_id):
    ws=get_spreadsheet().worksheet(USERS_SHEET)
    return _gsheets_call(ws.get_all_records)

def _data_fingerprint(raw): return hashlib.md5(str(raw[:20]).encode()).hexdigest()

def get_local_data(spreadsheet_id, ws_title):
    raw, fetched_at = _fetch_raw_sheet_cached(spreadsheet_id, ws_title)
    fp = _data_fingerprint(raw); ck = f"{ws_title}::{fp}"
    if st.session_state.get("local_cache_key") != ck:
        df, h, cm = _raw_to_dataframe(raw)
        st.session_state.local_df=df.copy(); st.session_state.local_headers=h
        st.session_state.local_col_map=cm; st.session_state.local_cache_key=ck
        st.session_state.local_fetched_at=fetched_at
    return st.session_state.local_df, st.session_state.local_headers, st.session_state.local_col_map, st.session_state.local_fetched_at or fetched_at

def _apply_optimistic_approve(df_iloc, new_vals, auditor, ts_now, log_prefix):
    ldf=st.session_state.local_df
    if df_iloc<0 or df_iloc>=len(ldf): return
    for f,v in new_vals.items():
        if f in ldf.columns: ldf.at[df_iloc,f]=v
    old=str(ldf.at[df_iloc,COL_LOG]).strip() if COL_LOG in ldf.columns else ""
    ldf.at[df_iloc,COL_STATUS]=VAL_DONE; ldf.at[df_iloc,COL_AUDITOR]=auditor
    ldf.at[df_iloc,COL_DATE]=ts_now
    if COL_LOG in ldf.columns: ldf.at[df_iloc,COL_LOG]=f"{log_prefix}\n{old}".strip()
    st.session_state.local_df=ldf

def _apply_optimistic_reopen(df_iloc):
    ldf=st.session_state.local_df
    if df_iloc<0 or df_iloc>=len(ldf): return
    ldf.at[df_iloc,COL_STATUS]=VAL_PENDING; st.session_state.local_df=ldf

def ensure_system_cols_in_sheet(ws, headers, col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            np_=len(headers)+1
            if np_>ws.col_count: _gsheets_call(ws.add_cols,max(4,np_-ws.col_count+1))
            _gsheets_call(ws.update_cell,1,np_,sc); headers.append(sc); col_map[sc]=np_
    return headers, col_map

def write_approval_to_sheet(ws_title,sheet_row,col_map,headers,new_vals,record,auditor,ts_now,log_prefix):
    ws=get_spreadsheet().worksheet(ws_title)
    headers,col_map=ensure_system_cols_in_sheet(ws,headers,col_map)
    old=str(record.get(COL_LOG,"")).strip(); new_log=f"{log_prefix}\n{old}".strip()
    batch=[]
    for f,v in new_vals.items():
        if f in col_map and clean_cell(record.get(f,""))!=v:
            batch.append({"range":rowcol_to_a1(sheet_row,col_map[f]),"values":[[v]]})
    for cn,v in [(COL_STATUS,VAL_DONE),(COL_AUDITOR,auditor),(COL_DATE,ts_now),(COL_LOG,new_log)]:
        if cn in col_map: batch.append({"range":rowcol_to_a1(sheet_row,col_map[cn]),"values":[[v]]})
    if batch: _gsheets_call(ws.batch_update,batch)

def write_reopen_to_sheet(ws_title,sheet_row,col_map):
    ws=get_spreadsheet().worksheet(ws_title)
    if COL_STATUS in col_map: _gsheets_call(ws.update_cell,sheet_row,col_map[COL_STATUS],VAL_PENDING)

def authenticate(email, password, spreadsheet_id):
    email=email.lower().strip()
    if email=="admin" and password==st.secrets.get("admin_password",""): return "admin"
    try:
        records=_fetch_users_cached(spreadsheet_id); df_u=pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return None
        row=df_u[df_u["email"]==email]
        if not row.empty and hash_pw(password)==str(row["password"].values[0]): return "auditor"
    except: pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  13 · HTML TABLE RENDERER  (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty: st.info("No records to display."); return
    display_df = df.head(max_rows)
    th = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col!=COL_LOG: th+=f"<th>{col}</th>"
    rows=""
    for idx,row in display_df.iterrows():
        r=f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col==COL_LOG: continue
            raw=str(row[col]) if row[col]!="" else ""; d=raw or "—"
            if col==COL_STATUS:
                d="<span class='s-chip s-done'>✓ Processed</span>" if raw==VAL_DONE else "<span class='s-chip s-pending'>⏳ Pending</span>"
            elif len(raw)>55: d=f"<span title='{raw}'>{raw[:52]}…</span>"
            r+=f"<td>{d}</td>"
        rows+=f"<tr>{r}</tr>"
    st.markdown(f"<div class='gov-table-wrap'><table class='gov-table'><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>",unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  14 · RENDER LOGIN  (Vercel-style glassmorphism)
# ─────────────────────────────────────────────────────────────────────────────
def render_login(spreadsheet_id: str) -> None:
    st.markdown("""<style>
    [data-testid="stSidebar"],[data-testid="collapsedControl"]{display:none!important;}
    html,body,.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],.main,.block-container{
      background:linear-gradient(145deg,#EEF2FF 0%,#F0F4FF 40%,#EFF6FF 100%) !important;
      min-height:100vh !important;
    }
    </style>
    <div class="login-bg"></div>""", unsafe_allow_html=True)

    _g, c1, c2 = st.columns([9, .55, .55])
    with c1:
        if st.button("EN", key="lg_en"): st.session_state.lang="en"; st.rerun()
    with c2:
        if st.button("KU", key="lg_ku"): st.session_state.lang="ku"; st.rerun()

    _, mid, _ = st.columns([1, 1.15, 1])
    with mid:
        st.markdown(f"""
        <div style="padding:40px 0 16px;">
          <div class="login-card">
            <div class="login-logo">🏛️</div>
            <div class="login-eyebrow">{t('ministry')}</div>
            <div class="login-title">{t('portal_title')}</div>
            <div class="login-rule"></div>
            <div style="text-align:center;margin-bottom:18px;">
              <span class="classified-pill">🔒 {t('classified')}</span>
            </div>
            <div class="login-sub">{t('login_prompt')}</div>
          </div>
        </div>""", unsafe_allow_html=True)

        with st.container():
            st.markdown('<div class="login-card" style="margin-top:-16px;">', unsafe_allow_html=True)
            with st.form("login_form", clear_on_submit=False):
                email_in = st.text_input(t("email_field"), placeholder="admin  ·  auditor@mof.gov")
                pass_in  = st.text_input(t("password_field"), type="password", placeholder="••••••••••")
                submitted = st.form_submit_button(f"🔐  {t('sign_in')}", use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        if submitted:
            role = authenticate(email_in, pass_in, spreadsheet_id)
            if role:
                st.session_state.logged_in  = True
                st.session_state.user_email = "Admin" if role=="admin" else email_in.lower().strip()
                st.session_state.user_role  = role
                st.rerun()
            else:
                st.error(t("bad_creds"))


# ─────────────────────────────────────────────────────────────────────────────
#  15 · RENDER SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(headers, col_binder, col_company, col_license, is_admin, fetched_at):
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
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"): st.session_state.lang="en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"): st.session_state.lang="ku"; st.rerun()
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)

        st.markdown(f"<div class='adv-filter-header'>🔍 {t('adv_filters')}</div>", unsafe_allow_html=True)
        status_opts={"all":t("status_all"),"pending":t("status_pending"),"done":t("status_done")}
        f_status=st.selectbox(t("f_status"),options=list(status_opts.keys()),format_func=lambda k:status_opts[k],key="f_status")

        for key,label,hint,disabled in [
            ("f_email",t("f_email"),COL_AUDITOR,False),
            ("f_binder",t("f_binder"),col_binder or "—",col_binder is None),
            ("f_company",t("f_company"),col_company or "—",col_company is None),
            ("f_license",t("f_license"),col_license or "—",col_license is None),
        ]:
            st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{label}<span class='col-hint'> ({hint})</span></div>",unsafe_allow_html=True)
            st.text_input(label,key=key,disabled=disabled,label_visibility="collapsed")

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button(f"✕  {t('clear_filters')}", use_container_width=True, key="clr_f"):
            for k in ("f_email","f_binder","f_company","f_license"): st.session_state[k]=""
            st.session_state["f_status"]="all"; st.rerun()

        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        role_label=t("role_admin") if is_admin else t("role_auditor")
        chip_cls="chip-admin" if is_admin else "chip-audit"
        st.markdown(f"""
        <div class="sb-user-card">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="chip {chip_cls}" style="margin-top:8px;">{role_label}</span>
        </div>""", unsafe_allow_html=True)
        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k,v in _DEFAULTS.items(): st.session_state[k]=v
            st.rerun()

    return (st.session_state.get("f_email",""), st.session_state.get("f_binder",""),
            st.session_state.get("f_company",""), st.session_state.get("f_license",""),
            st.session_state.get("f_status","all"))


def render_filter_bar(total, filtered, f_email, f_binder, f_company, f_license, f_status):
    n=_n_active(f_email,f_binder,f_company,f_license,f_status)
    if n==0: return
    badges=""
    if f_status!="all": badges+=f"<span class='filter-badge'>⚡ {f_status}</span> "
    if f_email.strip(): badges+=f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span> "
    if f_binder.strip(): badges+=f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span> "
    if f_company.strip(): badges+=f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span> "
    if f_license.strip(): badges+=f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span> "
    st.markdown(f"""<div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:800;color:var(--indigo-600);text-transform:uppercase;letter-spacing:.08em;">
        {t('active_filters')} ({n})</span> {badges}
      <span class="result-count"><strong style="color:var(--indigo-600);">{filtered}</strong>/{total}&nbsp;{t('results_shown')}</span>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  16 · TAB: AUDIT WORKLIST  (UNCHANGED logic)
# ─────────────────────────────────────────────────────────────────────────────
def render_worklist(pending_display, df, headers, col_map, ws_title, f_email, f_binder, f_company, f_license, f_status):
    p_count=len(pending_display)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">📋 {t('worklist_title')}</div>
      <div class="worklist-sub">{t('worklist_sub')}</div></div>
      <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
    </div>""", unsafe_allow_html=True)
    if pending_display.empty:
        msg=t("no_match") if _n_active(f_email,f_binder,f_company,f_license,f_status) else "✅  All cases processed."
        st.info(msg); return
    render_html_table(pending_display)
    st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>", unsafe_allow_html=True)
    label_col=next((h for h in headers if h not in SYSTEM_COLS),headers[0] if headers else "Row")
    opts=["—"]+[f"Row {idx}  ·  {str(row.get(label_col,''))[:55]}" for idx,row in pending_display.iterrows()]
    row_sel=st.selectbox("",opts,key="row_sel",label_visibility="collapsed")
    if row_sel=="—": return
    sheet_row=int(row_sel.split("  ·  ")[0].replace("Row ","").strip())
    df_iloc=sheet_row-2
    if df_iloc<0 or df_iloc>=len(df): st.error("Row index out of range."); return
    record=df.iloc[df_iloc].to_dict()
    with st.expander(f"📜  {t('audit_trail')}", expanded=False):
        history=str(record.get(COL_LOG,"")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip(): st.markdown(f'<div class="log-line">{line}</div>',unsafe_allow_html=True)
        else: st.caption(t("no_history"))
    st.markdown(f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>",unsafe_allow_html=True)
    SKIP=set(SYSTEM_COLS); fields={k:v for k,v in record.items() if k not in SKIP}
    with st.form("audit_form"):
        new_vals={}
        for fname,fval in fields.items(): new_vals[fname]=st.text_input(fname,value=clean_cell(fval),key=f"field_{fname}")
        do_submit=st.form_submit_button(f"✅  {t('approve_save')}",use_container_width=True)
    if do_submit:
        ts_now=now_str(); auditor=st.session_state.user_email; log_prefix=f"✔  {auditor}  |  {ts_now}"
        with st.spinner("Committing record to Google Sheets…"):
            try: write_approval_to_sheet(ws_title,sheet_row,col_map,headers,new_vals,record,auditor,ts_now,log_prefix)
            except gspread.exceptions.APIError as e: st.error(f"🚨 Write failed: {e}"); return
        _apply_optimistic_approve(df_iloc,new_vals,auditor,ts_now,log_prefix)
        st.success(t("saved_ok")); time.sleep(0.6); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  17 · TAB: PROCESSED ARCHIVE  (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
def render_archive(done_view, df, col_map, ws_title, is_admin, f_email, f_binder, f_company, f_license, f_status):
    d_count=len(done_view)
    st.markdown(f"""<div class="worklist-header">
      <div><div class="worklist-title">✅ Processed Archive</div>
      <div class="worklist-sub">Completed and committed audit records</div></div>
      <span class="chip chip-done">{d_count} {t('processed')}</span>
    </div>""", unsafe_allow_html=True)
    if done_view.empty:
        st.info(t("no_match") if _n_active(f_email,f_binder,f_company,f_license,f_status) else "No processed records yet.")
    else:
        render_html_table(done_view)
    if is_admin and not done_view.empty:
        st.markdown("<hr class='divider'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='section-title'>↩️ {t('reopen')}</div>",unsafe_allow_html=True)
        ropts=["—"]+[f"Row {idx}" for idx in done_view.index]
        rsel=st.selectbox("Select record to re-open:",ropts,key="reopen_sel")
        if rsel!="—":
            ridx=int(rsel.split(" ")[1]); df_iloc=ridx-2
            if st.button(t("reopen"),key="reopen_btn"):
                with st.spinner("Re-opening…"):
                    try: write_reopen_to_sheet(ws_title,ridx,col_map)
                    except gspread.exceptions.APIError as e: st.error(f"🚨 {e}"); return
                _apply_optimistic_reopen(df_iloc); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  18 · TAB: ANALYTICS  (Plotly colours updated for new theme)
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df):
    pt="plotly_white"; pb="#FFFFFF"; pg="#E4E7F0"; fc="#0D1117"
    nvy="#4F46E5"; blu="#60A5FA"
    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>",unsafe_allow_html=True)
    periods=[("all",t("all_time")),("today",t("today")),("this_week",t("this_week")),("this_month",t("this_month"))]
    for cw,(pk,pl) in zip(st.columns(len(periods)),periods):
        lbl=f"✓  {pl}" if st.session_state.date_filter==pk else pl
        if cw.button(lbl,use_container_width=True,key=f"pf_{pk}"): st.session_state.date_filter=pk; st.rerun()
    done_base=df[df[COL_STATUS]==VAL_DONE].copy()
    done_f=apply_period_filter(done_base,COL_DATE,st.session_state.date_filter)
    if done_f.empty: st.info(t("no_records")); return
    ma,mb,mc=st.columns(3); ma.metric(t("records_period"),len(done_f))
    active=0
    if COL_DATE in done_f.columns:
        active=done_f[COL_DATE].apply(lambda s:parse_dt(s).date() if parse_dt(s) else None).nunique()
    mb.metric(t("active_days"),active); mc.metric(t("avg_per_day"),f"{len(done_f)/max(active,1):.1f}")
    left,right=st.columns([1,1.6],gap="large")
    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>",unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb=done_f[COL_AUDITOR].replace("","—").value_counts().reset_index(); lb.columns=["Auditor","Count"]
            medals=["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i,r in lb.head(10).iterrows():
                m=medals[i] if i<len(medals) else f"{i+1}."
                st.markdown(f'<div class="lb-row"><span class="lb-medal">{m}</span><span class="lb-name">{r["Auditor"]}</span><span class="lb-count">{r["Count"]}</span></div>',unsafe_allow_html=True)
            fig=px.bar(lb.head(10),x="Count",y="Auditor",orientation="h",color="Count",color_continuous_scale=[blu,nvy],template=pt)
            fig.update_layout(paper_bgcolor=pb,plot_bgcolor=pb,font=dict(family="Plus Jakarta Sans",color=fc,size=11),
                showlegend=False,coloraxis_showscale=False,margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color="#4B5563")),
                yaxis=dict(gridcolor="rgba(0,0,0,0)",categoryorder="total ascending",tickfont=dict(color="#4B5563")),
                height=min(320,max(180,36*len(lb.head(10)))))
            fig.update_traces(marker_line_width=0); st.plotly_chart(fig,use_container_width=True)
    with right:
        st.markdown(f"<div class='section-title'>📈 {t('daily_trend')}</div>",unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f=done_f.copy()
            done_f["_date"]=done_f[COL_DATE].apply(lambda s:parse_dt(s).date() if parse_dt(s) else None)
            trend=done_f.dropna(subset=["_date"]).groupby("_date").size().reset_index(name="Records")
            trend.columns=["Date","Records"]
            if not trend.empty:
                if len(trend)>1:
                    rng=pd.date_range(trend["Date"].min(),trend["Date"].max())
                    trend=trend.set_index("Date").reindex(rng.date,fill_value=0).reset_index()
                    trend.columns=["Date","Records"]
                fig2=go.Figure()
                fig2.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],mode="none",fill="tozeroy",fillcolor="rgba(99,102,241,0.07)",showlegend=False))
                fig2.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],mode="lines+markers",
                    line=dict(color=nvy,width=2.5),marker=dict(color=blu,size=7,line=dict(color="#FFFFFF",width=2)),name=t("records_period")))
                fig2.update_layout(template=pt,paper_bgcolor=pb,plot_bgcolor=pb,
                    font=dict(family="Plus Jakarta Sans",color=fc,size=11),showlegend=False,
                    margin=dict(l=8,r=8,t=10,b=8),
                    xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color="#4B5563")),
                    yaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color="#4B5563")),
                    height=380,hovermode="x unified")
                st.plotly_chart(fig2,use_container_width=True)
            else: st.info(t("no_records"))


# ─────────────────────────────────────────────────────────────────────────────
#  19 · TAB: USER ADMIN  (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
def render_user_admin(spreadsheet_id):
    spr=get_spreadsheet(); uws=spr.worksheet(USERS_SHEET)
    cl,cr=st.columns([1,1],gap="large")
    with cl:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>",unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_e=st.text_input("Email",placeholder="auditor@mof.gov"); nu_p=st.text_input("Password",type="password")
            if st.form_submit_button("Register Auditor",use_container_width=True):
                if nu_e.strip() and nu_p.strip():
                    recs=_fetch_users_cached(spreadsheet_id); dfu=pd.DataFrame(recs)
                    already=not dfu.empty and nu_e.lower().strip() in dfu.get("email",pd.Series()).values
                    if already: st.error(t("dup_email"))
                    else:
                        _gsheets_call(uws.append_row,[nu_e.lower().strip(),hash_pw(nu_p.strip()),now_str()])
                        st.success(f"✅  {nu_e} registered."); time.sleep(0.7); st.rerun()
                else: st.warning(t("fill_fields"))
        st.markdown(f"<div class='section-title'>🔑 {t('update_pw')}</div>",unsafe_allow_html=True)
        staff=pd.DataFrame(_fetch_users_cached(spreadsheet_id))
        if not staff.empty and "email" in staff.columns:
            with st.form("upd_pw_form"):
                se=st.selectbox("Select staff",staff["email"].tolist()); np_=st.text_input("New Password",type="password")
                if st.form_submit_button("Update Password",use_container_width=True):
                    if np_.strip():
                        cell=_gsheets_call(uws.find,se)
                        if cell: _gsheets_call(uws.update_cell,cell.row,2,hash_pw(np_.strip())); st.success(f"✅  Updated for {se}."); time.sleep(0.7); st.rerun()
    with cr:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>",unsafe_allow_html=True)
        staff=pd.DataFrame(_fetch_users_cached(spreadsheet_id))
        if not staff.empty and "email" in staff.columns:
            sc=[c for c in ["email","created_at"] if c in staff.columns]; render_html_table(staff[sc].reset_index())
            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>",unsafe_allow_html=True)
            de=st.selectbox("Select to revoke",["—"]+staff["email"].tolist(),key="del_sel")
            if de!="—":
                if st.button(f"Revoke access — {de}",key="del_btn"):
                    cell=_gsheets_call(uws.find,de)
                    if cell: _gsheets_call(uws.delete_rows,cell.row); st.success(f"✅  {de} revoked."); time.sleep(0.7); st.rerun()
        else: st.info("No auditor accounts registered yet.")


# ─────────────────────────────────────────────────────────────────────────────
#  20 · MAIN CONTROLLER  (UNCHANGED — smart sheet matching preserved)
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        spr=get_spreadsheet(); sid=spr.id
        all_titles=[ws.title for ws in spr.worksheets()]
        if USERS_SHEET not in all_titles:
            uw=spr.add_worksheet(title=USERS_SHEET,rows="500",cols="3")
            _gsheets_call(uw.append_row,["email","password","created_at"])

        if not st.session_state.logged_in:
            render_login(sid); return

        st.markdown("<style>[data-testid='stSidebar']{display:flex!important;}</style>",unsafe_allow_html=True)
        is_admin=st.session_state.user_role=="admin"
        ts_str=datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""<div class="page-header">
          <div><div class="page-title">🏛️  {t('portal_title')}</div>
          <div class="page-subtitle">{t('ministry')}</div></div>
          <div class="page-timestamp">{ts_str}</div>
        </div>""", unsafe_allow_html=True)

        # Smart sheet matching (UNCHANGED)
        atm={title.strip().lower(): title for title in all_titles}
        available=[atm[s.strip().lower()] for s in VISIBLE_SHEETS if s.strip().lower() in atm]
        df=pd.DataFrame(); headers=[]; col_map={}; ws_title=None; fetched_at="—"

        if not available:
            st.warning("None of the configured worksheets found. Expected: "+", ".join(VISIBLE_SHEETS))
            st.error(f"⚠️ Found: `{all_titles}`")
        else:
            ws_title=st.selectbox(t("workspace"),available,key="ws_sel")
            if ws_title:
                wck=f"ws_title::{ws_title}"
                if st.session_state.get("active_ws_key")!=wck:
                    st.session_state.local_cache_key=None; st.session_state.active_ws_key=wck
                try: df,headers,col_map,fetched_at=get_local_data(sid,ws_title)
                except gspread.exceptions.WorksheetNotFound: st.error(f"Worksheet '{ws_title}' not found.")
                except gspread.exceptions.APIError as e: st.error(f"🚨 {t('retry_warning')}\n\n{e}")

        col_binder=detect_column(headers,"binder")
        col_company=detect_column(headers,"company")
        col_license=detect_column(headers,"license")
        f_email,f_binder,f_company,f_license,f_status=render_sidebar(headers,col_binder,col_company,col_license,is_admin,fetched_at)

        if not df.empty:
            st.markdown(f"<div class='section-title'>📊 {t('overview')}</div>",unsafe_allow_html=True)
            total_n=len(df); done_n=int((df[COL_STATUS]==VAL_DONE).sum())
            pending_n=total_n-done_n; pct=done_n/total_n if total_n else 0
            m1,m2,m3=st.columns(3)
            m1.metric(t("total"),total_n)
            m2.metric(t("processed"),done_n,delta=f"{int(pct*100)}%")
            m3.metric(t("outstanding"),pending_n,delta=f"{100-int(pct*100)}% remaining",delta_color="inverse")
            st.markdown(f"""<div class="prog-labels"><span>{t('processed')}</span><span>{int(pct*100)}%</span></div>
            <div class="prog-wrap"><div class="prog-fill" style="width:{int(pct*100)}%;"></div></div>""",unsafe_allow_html=True)
            filtered_df=apply_filters_locally(df,f_email,f_binder,f_company,f_license,f_status,col_binder,col_company,col_license)
            render_filter_bar(total_n,len(filtered_df),f_email,f_binder,f_company,f_license,f_status)
        else:
            filtered_df=pd.DataFrame()

        if is_admin:
            tabs=st.tabs([t("tab_worklist"),t("tab_archive"),t("tab_analytics"),t("tab_users")])
            t_work,t_arch,t_anal,t_uadm=tabs
        else:
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>",unsafe_allow_html=True)
            tabs=st.tabs([t("tab_worklist"),t("tab_archive")])
            t_work,t_arch=tabs; t_anal=t_uadm=None

        with t_work:
            if not df.empty and ws_title:
                pv=filtered_df[filtered_df[COL_STATUS]!=VAL_DONE].copy()
                pd_=pv.copy(); pd_.index=pd_.index+2
                render_worklist(pd_,df,headers,col_map,ws_title,f_email,f_binder,f_company,f_license,f_status)
        with t_arch:
            if not df.empty and ws_title:
                dv=filtered_df[filtered_df[COL_STATUS]==VAL_DONE].copy(); dv.index=dv.index+2
                render_archive(dv,df,col_map,ws_title,is_admin,f_email,f_binder,f_company,f_license,f_status)
        if is_admin and t_anal:
            with t_anal:
                if not df.empty: render_analytics(df)
        if is_admin and t_uadm:
            with t_uadm: render_user_admin(sid)

    except Exception as exc:
        st.error(f"🚨  System Error: {exc}")
        with st.expander("Technical Details",expanded=False): st.exception(exc)


if __name__ == "__main__":
    main()
