# =============================================================================
#  OFFICIAL TAX AUDIT PORTAL  ·  v4.0
#  Governmental Tax & Customs Data Audit Platform
#  Stack: Streamlit · gspread · Pandas · Plotly · pytz
#  Requirements: pip install streamlit gspread oauth2client pandas plotly pytz
# =============================================================================

import streamlit as st
import gspread
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

# ─────────────────────────────────────────────────────────────────────────────
#  0 · PAGE CONFIG  — must be the very first Streamlit call
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Official Tax Audit Portal",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
#  1 · TIMEZONE & SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
TZ = pytz.timezone("Asia/Baghdad")

_DEFAULTS: dict = dict(
    logged_in   = False,
    user_email  = "",
    user_role   = "",          # "admin" | "auditor"
    theme       = "dark",
    lang        = "en",
    date_filter = "all",
)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─────────────────────────────────────────────────────────────────────────────
#  2 · THEME PALETTES  (Deep Navy / Slate / Gold  ×  Light Ivory / Steel / Gold)
# ─────────────────────────────────────────────────────────────────────────────
_PALETTES: dict = {
    # ── Deep-Space Government Dark ──────────────────────────────────────────
    "dark": {
        # backgrounds
        "page_bg"      : "#080C14",
        "surface"      : "#0D1523",
        "surface2"     : "#111D30",
        "card"         : "#0F1A2E",
        "card2"        : "#142236",
        # borders & dividers
        "border"       : "#1C2E4A",
        "border2"      : "#243A5E",
        # typography
        "text_primary" : "#EDF2FF",   # near-white — used for ALL table text
        "text_secondary": "#8BA3C7",
        "text_muted"   : "#4A6588",
        # brand accent — gold
        "gold"         : "#C9A84C",
        "gold_light"   : "#E8C97A",
        "gold_bg"      : "rgba(201,168,76,0.10)",
        # semantic colors
        "blue_accent"  : "#3B7DD8",
        "blue_bg"      : "rgba(59,125,216,0.12)",
        "green"        : "#28A878",
        "green_bg"     : "rgba(40,168,120,0.12)",
        "amber"        : "#E8A020",
        "amber_bg"     : "rgba(232,160,32,0.12)",
        "red"          : "#E05555",
        "red_bg"       : "rgba(224,85,85,0.12)",
        # inputs / buttons
        "input_bg"     : "#070C18",
        "input_border" : "#1C3055",
        "btn_primary"  : "#C9A84C",
        "btn_text"     : "#080C14",
        # progress
        "prog_track"   : "#1C2E4A",
        "prog_fill_a"  : "#C9A84C",
        "prog_fill_b"  : "#3B7DD8",
        # plotly
        "plotly_theme" : "plotly_dark",
        "plot_bg"      : "#0D1523",
        "plot_grid"    : "#1C2E4A",
    },
    # ── Ivory Governmental Light ─────────────────────────────────────────────
    "light": {
        "page_bg"      : "#F4F6FA",
        "surface"      : "#FFFFFF",
        "surface2"     : "#EEF2FA",
        "card"         : "#FFFFFF",
        "card2"        : "#F7F9FD",
        "border"       : "#D4DCF0",
        "border2"      : "#B8C8E8",
        "text_primary" : "#0B1A30",   # near-black — used for ALL table text
        "text_secondary": "#3A5070",
        "text_muted"   : "#7A90B0",
        "gold"         : "#A07828",
        "gold_light"   : "#C09040",
        "gold_bg"      : "rgba(160,120,40,0.08)",
        "blue_accent"  : "#1A5FBE",
        "blue_bg"      : "rgba(26,95,190,0.08)",
        "green"        : "#1A7A58",
        "green_bg"     : "rgba(26,122,88,0.08)",
        "amber"        : "#B06010",
        "amber_bg"     : "rgba(176,96,16,0.08)",
        "red"          : "#B03030",
        "red_bg"       : "rgba(176,48,48,0.08)",
        "input_bg"     : "#FFFFFF",
        "input_border" : "#C0CCE0",
        "btn_primary"  : "#0F2D5E",
        "btn_text"     : "#FFFFFF",
        "prog_track"   : "#DDE5F5",
        "prog_fill_a"  : "#A07828",
        "prog_fill_b"  : "#1A5FBE",
        "plotly_theme" : "plotly_white",
        "plot_bg"      : "#FFFFFF",
        "plot_grid"    : "#DDE5F5",
    },
}

P = _PALETTES[st.session_state.theme]   # active palette shorthand

# ─────────────────────────────────────────────────────────────────────────────
#  3 · FULL CSS INJECTION
#      All rules use CSS variables fed from the active palette.
#      Text colours are set forcefully with !important to prevent Streamlit
#      or Google-Sheets encoding quirks from making content invisible.
# ─────────────────────────────────────────────────────────────────────────────
def inject_css(P: dict) -> None:
    st.markdown(f"""
<style>
/* ══════════════════════════════════════════════════════════════════
   FONTS
══════════════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ══════════════════════════════════════════════════════════════════
   CSS VARIABLES
══════════════════════════════════════════════════════════════════ */
:root {{
  --page-bg       : {P['page_bg']};
  --surface       : {P['surface']};
  --surface2      : {P['surface2']};
  --card          : {P['card']};
  --card2         : {P['card2']};
  --border        : {P['border']};
  --border2       : {P['border2']};
  --text-primary  : {P['text_primary']};
  --text-secondary: {P['text_secondary']};
  --text-muted    : {P['text_muted']};
  --gold          : {P['gold']};
  --gold-light    : {P['gold_light']};
  --gold-bg       : {P['gold_bg']};
  --blue          : {P['blue_accent']};
  --blue-bg       : {P['blue_bg']};
  --green         : {P['green']};
  --green-bg      : {P['green_bg']};
  --amber         : {P['amber']};
  --amber-bg      : {P['amber_bg']};
  --red           : {P['red']};
  --red-bg        : {P['red_bg']};
  --input-bg      : {P['input_bg']};
  --input-border  : {P['input_border']};
  --btn-primary   : {P['btn_primary']};
  --btn-text      : {P['btn_text']};
  --prog-track    : {P['prog_track']};
}}

/* ══════════════════════════════════════════════════════════════════
   GLOBAL RESET & BASE
══════════════════════════════════════════════════════════════════ */
*, *::before, *::after {{ box-sizing: border-box !important; }}

html, body, .stApp, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main, .block-container {{
  font-family: 'Inter', sans-serif !important;
  background-color: var(--page-bg) !important;
  color: var(--text-primary) !important;
}}

/* Force ALL paragraph/span/div text to be visible */
p, span, div, li, td, th, label {{
  color: var(--text-primary) !important;
}}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"] {{ display: none !important; }}

/* ══════════════════════════════════════════════════════════════════
   SIDEBAR
══════════════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {{
  background: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
  padding-top: 0 !important;
}}
[data-testid="stSidebar"] > div {{
  padding: 0 !important;
}}
[data-testid="stSidebar"] * {{
  color: var(--text-primary) !important;
  font-family: 'Inter', sans-serif !important;
}}
[data-testid="stSidebarCollapseButton"] {{ display: none !important; }}
[data-testid="collapsedControl"]        {{ display: none !important; }}

/* ══════════════════════════════════════════════════════════════════
   METRIC CARDS  — lift on hover
══════════════════════════════════════════════════════════════════ */
[data-testid="stMetricContainer"] {{
  background    : var(--card) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 14px !important;
  padding       : 20px 24px !important;
  box-shadow    : 0 2px 8px rgba(0,0,0,0.15) !important;
  transition    : transform .22s cubic-bezier(.34,1.56,.64,1),
                  box-shadow .22s ease !important;
}}
[data-testid="stMetricContainer"]:hover {{
  transform  : translateY(-5px) !important;
  box-shadow : 0 12px 28px rgba(0,0,0,0.22),
               0 0 0 1px var(--gold) !important;
}}
[data-testid="stMetricValue"] {{
  font-family : 'JetBrains Mono', monospace !important;
  font-size   : 2.25rem !important;
  font-weight : 600 !important;
  color       : var(--gold) !important;
}}
[data-testid="stMetricLabel"] {{
  font-size      : 0.70rem !important;
  font-weight    : 700 !important;
  letter-spacing : 0.10em !important;
  text-transform : uppercase !important;
  color          : var(--text-muted) !important;
}}
[data-testid="stMetricDelta"] {{ font-size: 0.80rem !important; }}

/* ══════════════════════════════════════════════════════════════════
   FORMS
══════════════════════════════════════════════════════════════════ */
div[data-testid="stForm"] {{
  background    : var(--card) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 16px !important;
  padding       : 28px 32px !important;
}}

/* ══════════════════════════════════════════════════════════════════
   INPUTS  — high-contrast, explicitly colored
══════════════════════════════════════════════════════════════════ */
.stTextInput > div > div > input,
.stTextArea  > div > div > textarea,
.stSelectbox > div > div > div,
.stMultiSelect > div > div > div {{
  background    : var(--input-bg)     !important;
  color         : var(--text-primary) !important;  /* ← critical fix */
  border        : 1px solid var(--input-border) !important;
  border-radius : 8px !important;
  font-family   : 'Inter', sans-serif !important;
  font-size     : 0.875rem !important;
  caret-color   : var(--gold) !important;
  transition    : border-color .18s ease, box-shadow .18s ease !important;
}}
.stTextInput > div > div > input:focus,
.stTextArea  > div > div > textarea:focus {{
  border-color : var(--gold) !important;
  box-shadow   : 0 0 0 3px var(--gold-bg) !important;
  outline      : none !important;
}}
/* Labels */
.stTextInput label, .stTextArea label,
.stSelectbox label, .stMultiSelect label {{
  color          : var(--text-muted)  !important;
  font-size      : 0.70rem !important;
  font-weight    : 700 !important;
  letter-spacing : 0.09em !important;
  text-transform : uppercase !important;
}}
/* Placeholder text */
.stTextInput > div > div > input::placeholder,
.stTextArea  > div > div > textarea::placeholder {{
  color   : var(--text-muted) !important;
  opacity : 0.7 !important;
}}

/* ══════════════════════════════════════════════════════════════════
   BUTTONS
══════════════════════════════════════════════════════════════════ */
.stButton > button {{
  background    : var(--btn-primary) !important;
  color         : var(--btn-text)    !important;
  border        : none !important;
  border-radius : 8px !important;
  font-family   : 'Inter', sans-serif !important;
  font-weight   : 700 !important;
  font-size     : 0.85rem !important;
  letter-spacing: 0.04em !important;
  padding       : 10px 20px !important;
  transition    : opacity .15s ease, transform .15s ease !important;
}}
.stButton > button:hover  {{ opacity: .85 !important; transform: translateY(-1px) !important; }}
.stButton > button:active {{ transform: translateY(0) !important; }}

/* ══════════════════════════════════════════════════════════════════
   TABS
══════════════════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {{
  gap            : 2px !important;
  background     : transparent !important;
  border-bottom  : 2px solid var(--border) !important;
  padding-bottom : 0 !important;
}}
.stTabs [data-baseweb="tab"] {{
  background     : transparent !important;
  color          : var(--text-muted) !important;
  border-radius  : 8px 8px 0 0 !important;
  border         : 1px solid transparent !important;
  border-bottom  : none !important;
  padding        : 10px 22px !important;
  font-weight    : 600 !important;
  font-size      : 0.83rem !important;
  letter-spacing : 0.04em !important;
  transition     : color .15s !important;
}}
.stTabs [data-baseweb="tab"]:hover {{
  color: var(--text-primary) !important;
}}
.stTabs [aria-selected="true"] {{
  background         : var(--card) !important;
  color              : var(--gold) !important;
  border-color       : var(--border) !important;
  border-bottom-color: var(--card) !important;
  margin-bottom      : -2px !important;
}}

/* ══════════════════════════════════════════════════════════════════
   DATAFRAME / TABLE  — THE CRITICAL TEXT VISIBILITY FIX
   We target every layer of the Streamlit data-grid render tree.
══════════════════════════════════════════════════════════════════ */
/* Outer wrapper */
[data-testid="stDataFrame"],
[data-testid="stDataFrameContainer"] {{
  border        : 1px solid var(--border) !important;
  border-radius : 12px !important;
  overflow      : hidden !important;
}}
/* Canvas / scroller background */
.dvn-scroller,
.dvn-scroller > div,
[class*="glideDataEditor"],
[class*="dvn-stack"] {{
  background-color : var(--card) !important;
}}
/* All cell text — the root cause of invisible text */
[class*="cell"],
[class*="gdg-cell"],
[class*="dvn-cell"],
canvas,
.dvn-scroller * {{
  color       : var(--text-primary) !important;
  font-family : 'Inter', sans-serif !important;
  font-size   : 0.83rem !important;
}}
/* Header row */
[class*="header"],
[class*="gdg-header"] {{
  background-color : var(--surface2) !important;
  color            : var(--text-muted) !important;
  font-weight      : 700 !important;
  font-size        : 0.72rem !important;
  letter-spacing   : 0.08em !important;
  text-transform   : uppercase !important;
  border-bottom    : 1px solid var(--border2) !important;
}}

/* ── Fallback: st.table (HTML table) ── */
table {{
  width            : 100% !important;
  border-collapse  : collapse !important;
  background-color : var(--card) !important;
  color            : var(--text-primary) !important;
}}
table th {{
  background-color : var(--surface2) !important;
  color            : var(--text-muted) !important;
  font-size        : 0.72rem !important;
  font-weight      : 700 !important;
  letter-spacing   : 0.08em !important;
  text-transform   : uppercase !important;
  padding          : 10px 14px !important;
  border-bottom    : 2px solid var(--border2) !important;
  text-align       : left !important;
}}
table td {{
  color         : var(--text-primary) !important;
  padding       : 9px 14px !important;
  font-size     : 0.83rem !important;
  border-bottom : 1px solid var(--border) !important;
  vertical-align: middle !important;
}}
table tr:hover td {{
  background-color: var(--surface2) !important;
}}

/* ══════════════════════════════════════════════════════════════════
   EXPANDER
══════════════════════════════════════════════════════════════════ */
.streamlit-expanderHeader {{
  background    : var(--surface2) !important;
  color         : var(--text-primary) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 8px !important;
  font-weight   : 600 !important;
  font-size     : 0.85rem !important;
}}
.streamlit-expanderContent {{
  background  : var(--card) !important;
  border      : 1px solid var(--border) !important;
  border-top  : none !important;
  border-radius: 0 0 8px 8px !important;
  padding     : 16px !important;
}}

/* ══════════════════════════════════════════════════════════════════
   ALERTS / INFO / SUCCESS / ERROR
══════════════════════════════════════════════════════════════════ */
[data-testid="stAlert"] {{
  border-radius : 10px !important;
  border        : 1px solid var(--border) !important;
}}

/* ══════════════════════════════════════════════════════════════════
   CUSTOM COMPONENTS  (class-based, used via st.markdown)
══════════════════════════════════════════════════════════════════ */

/* ── Government Seal / Login Card ── */
.gov-login-wrap {{
  display         : flex;
  flex-direction  : column;
  align-items     : center;
  justify-content : center;
  min-height      : 82vh;
}}
.gov-login-card {{
  width           : 100%;
  max-width       : 460px;
  background      : var(--card);
  border          : 1px solid var(--border2);
  border-top      : 4px solid var(--gold);
  border-radius   : 18px;
  padding         : 46px 44px 38px;
  box-shadow      : 0 24px 64px rgba(0,0,0,0.28);
}}
.gov-seal {{
  font-size        : 3.4rem;
  text-align       : center;
  margin-bottom    : 8px;
  filter           : drop-shadow(0 2px 8px rgba(201,168,76,0.35));
}}
.gov-ministry {{
  font-size        : 0.68rem;
  font-weight      : 700;
  letter-spacing   : 0.18em;
  text-transform   : uppercase;
  text-align       : center;
  color            : var(--text-muted);
  margin-bottom    : 4px;
}}
.gov-portal-title {{
  font-size        : 1.40rem;
  font-weight      : 800;
  text-align       : center;
  color            : var(--text-primary);
  letter-spacing   : -0.02em;
  margin-bottom    : 4px;
}}
.gov-portal-sub {{
  font-size        : 0.80rem;
  text-align       : center;
  color            : var(--text-muted);
  margin-bottom    : 28px;
}}
.gold-divider {{
  width      : 48px;
  height     : 3px;
  background : var(--gold);
  border-radius: 99px;
  margin     : 10px auto 24px;
}}

/* ── Page / Section headers ── */
.page-title {{
  font-size      : 1.55rem;
  font-weight    : 800;
  color          : var(--text-primary);
  letter-spacing : -0.025em;
  margin-bottom  : 2px;
}}
.page-sub {{
  font-size      : 0.82rem;
  color          : var(--text-muted);
  margin-bottom  : 24px;
}}
.section-title {{
  display        : flex;
  align-items    : center;
  gap            : 10px;
  font-size      : 0.88rem;
  font-weight    : 700;
  color          : var(--text-primary);
  letter-spacing : 0.01em;
  margin         : 22px 0 12px;
  padding-left   : 12px;
  border-left    : 3px solid var(--gold);
}}

/* ── Progress bar ── */
.gov-progress-wrap {{
  background    : var(--prog-track);
  border-radius : 99px;
  height        : 7px;
  overflow      : hidden;
  margin        : 6px 0 10px;
}}
.gov-progress-fill {{
  height         : 100%;
  border-radius  : 99px;
  background     : linear-gradient(90deg, {P['prog_fill_a']}, {P['prog_fill_b']});
  transition     : width .7s cubic-bezier(.4,0,.2,1);
}}
.prog-labels {{
  display         : flex;
  justify-content : space-between;
  font-size       : 0.72rem;
  color           : var(--text-muted);
  font-weight     : 600;
  margin-bottom   : 3px;
}}

/* ── Status chips ── */
.chip {{
  display        : inline-flex;
  align-items    : center;
  gap            : 5px;
  padding        : 3px 10px;
  border-radius  : 99px;
  font-size      : 0.70rem;
  font-weight    : 700;
  letter-spacing : 0.07em;
  text-transform : uppercase;
}}
.chip-done    {{ background: var(--green-bg); color: var(--green); }}
.chip-pending {{ background: var(--amber-bg); color: var(--amber); }}
.chip-admin   {{ background: var(--gold-bg);  color: var(--gold);  }}
.chip-audit   {{ background: var(--blue-bg);  color: var(--blue);  }}

/* ── Sidebar user badge ── */
.sb-user-badge {{
  background    : var(--surface2);
  border        : 1px solid var(--border);
  border-radius : 10px;
  padding       : 12px 14px;
  margin        : 0 0 10px;
}}
.sb-label {{
  font-size      : 0.60rem;
  font-weight    : 700;
  letter-spacing : 0.12em;
  text-transform : uppercase;
  color          : var(--text-muted);
  margin-bottom  : 3px;
}}
.sb-email {{ font-size: 0.875rem; font-weight: 700; color: var(--text-primary); }}

/* ── Leaderboard rows ── */
.lb-row {{
  display       : flex;
  align-items   : center;
  gap           : 12px;
  padding       : 11px 16px;
  background    : var(--card2);
  border        : 1px solid var(--border);
  border-radius : 10px;
  margin-bottom : 7px;
  transition    : transform .18s ease, border-color .18s ease;
}}
.lb-row:hover {{ transform: translateX(5px); border-color: var(--gold); }}
.lb-medal {{ font-size: 1.15rem; width: 26px; text-align: center; }}
.lb-name  {{ flex: 1; font-size: 0.875rem; font-weight: 600; color: var(--text-primary); }}
.lb-count {{
  font-family : 'JetBrains Mono', monospace;
  font-size   : 0.95rem;
  font-weight : 700;
  color       : var(--gold);
}}

/* ── Audit-trail log lines ── */
.log-line {{
  font-family  : 'JetBrains Mono', monospace;
  font-size    : 0.76rem;
  color        : var(--text-secondary);
  padding      : 3px 0;
  border-bottom: 1px solid var(--border);
}}
.log-line:last-child {{ border-bottom: none; }}
</style>
""", unsafe_allow_html=True)

inject_css(P)

# ─────────────────────────────────────────────────────────────────────────────
#  4 · TRANSLATIONS  (English & Kurdish Sorani)
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry"      : "Ministry of Finance & Customs",
        "portal_title"  : "Official Tax Audit Portal",
        "portal_sub"    : "Secure · Classified · Government Use Only",
        "login_prompt"  : "Enter your authorised credentials to proceed",
        "email"         : "Official Email / User ID",
        "password"      : "Password",
        "sign_in"       : "Authenticate",
        "sign_out"      : "Sign Out",
        "bad_creds"     : "Authentication failed. Verify your credentials.",
        "theme"         : "Display Theme",
        "language"      : "Interface Language",
        "workspace"     : "Select Case Register",
        "overview"      : "Case Overview",
        "total"         : "Total Cases",
        "processed"     : "Processed",
        "outstanding"   : "Outstanding",
        "search"        : "Search records (company, TIN, auditor…)",
        "tab_queue"     : "📋  Task Queue",
        "tab_archive"   : "✅  Processed Archive",
        "tab_analytics" : "📊  Analytics",
        "tab_users"     : "⚙️  User Admin",
        "select_case"   : "Select a case to inspect",
        "audit_trail"   : "Audit Trail",
        "approve_save"  : "Approve & Commit Record",
        "reopen"        : "Re-open Record (Admin)",
        "leaderboard"   : "Auditor Productivity Leaderboard",
        "daily_trend"   : "Daily Processing Trend",
        "period"        : "Time Period",
        "today"         : "Today",
        "this_week"     : "This Week",
        "this_month"    : "This Month",
        "all_time"      : "All Time",
        "add_auditor"   : "Register New Auditor",
        "update_pw"     : "Update Password",
        "remove_user"   : "Revoke Access",
        "staff_dir"     : "Authorised Staff",
        "no_records"    : "No records found for this period.",
        "empty_sheet"   : "This register contains no data.",
        "saved_ok"      : "Record approved and committed to the register.",
        "dup_email"     : "This email is already registered.",
        "fill_fields"   : "All fields are required.",
        "signed_as"     : "Authenticated as",
        "role_admin"    : "System Administrator",
        "role_auditor"  : "Tax Auditor",
        "processing"    : "Inspecting Case",
        "no_history"    : "No audit trail for this record.",
        "records_period": "Records (period)",
        "active_days"   : "Active Days",
        "avg_per_day"   : "Avg / Day",
    },
    "ku": {
        "ministry"      : "وەزارەتی دارایی و گومرگ",
        "portal_title"  : "پۆرتەلی وردبینی باجی فەرمی",
        "portal_sub"    : "پارێزراو · نهێنی · تەنها بەکارهێنانی حکومی",
        "login_prompt"  : "زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email"         : "ئیمەیڵی فەرمی / ناساندن",
        "password"      : "پاسۆرد",
        "sign_in"       : "دەستپێبکە",
        "sign_out"      : "چوونەدەرەوە",
        "bad_creds"     : "ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "theme"         : "تیمی پیشاندان",
        "language"      : "زمانی ڕووکار",
        "workspace"     : "تۆماری کیسەکان هەڵبژێرە",
        "overview"      : "کورتەی کیسەکان",
        "total"         : "کۆی کیسەکان",
        "processed"     : "کارکراوە",
        "outstanding"   : "ماوە",
        "search"        : "گەڕان (کۆمپانیا، TIN، ئۆدیتۆر…)",
        "tab_queue"     : "📋  ڕیزی کارەکان",
        "tab_archive"   : "✅  ئەرشیفی کارکراو",
        "tab_analytics" : "📊  ئەنالیتیکس",
        "tab_users"     : "⚙️  بەڕێوەبردنی بەکارهێنەر",
        "select_case"   : "کیسێک هەڵبژێرە بۆ پشکنین",
        "audit_trail"   : "مێژووی گۆڕانکاری",
        "approve_save"  : "پەسەندکردن و پاشەکەوتکردن",
        "reopen"        : "کردنەوەی دووبارەی کیس (ئەدمین)",
        "leaderboard"   : "تەختەی بەرهەمهێنانی ئۆدیتۆر",
        "daily_trend"   : "ترەندی بەرپرسانەی ڕۆژانە",
        "period"        : "ماوەی کات",
        "today"         : "ئەمڕۆ",
        "this_week"     : "ئەم هەفتەیە",
        "this_month"    : "ئەم مانگەیە",
        "all_time"      : "هەموو کات",
        "add_auditor"   : "تۆمارکردنی ئۆدیتۆری نوێ",
        "update_pw"     : "نوێکردنەوەی پاسۆرد",
        "remove_user"   : "هەڵوەشاندنەوەی دەستپێگەیشتن",
        "staff_dir"     : "کارمەندە مەرجداركراوەکان",
        "no_records"    : "هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet"   : "ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok"      : "کیسەکە پەسەندکرا و پاشەکەوتکرا.",
        "dup_email"     : "ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields"   : "هەموو خانەکان پەیوەندییانە.",
        "signed_as"     : "چووییتە ژوورەوە بەناوی",
        "role_admin"    : "بەڕێوەبەری سیستەم",
        "role_auditor"  : "ئۆدیتۆری باج",
        "processing"    : "پشکنینی کیسی",
        "no_history"    : "هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period": "تۆمارەکان (ماوە)",
        "active_days"   : "ڕۆژی چالاک",
        "avg_per_day"   : "تێکڕای ڕۆژانە",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)

# ─────────────────────────────────────────────────────────────────────────────
#  5 · SYSTEM COLUMN CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
COL_STATUS  = "Status"
COL_LOG     = "Audit_Log"
COL_AUDITOR = "Auditor_ID"
COL_DATE    = "Update_Date"
SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE]
VAL_DONE    = "Processed"
VAL_PENDING = "Pending"

# ─────────────────────────────────────────────────────────────────────────────
#  6 · HELPERS
# ─────────────────────────────────────────────────────────────────────────────
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
    """
    Sanitise a single cell value from Google Sheets.
    Handles None, NaN, special characters, and encoding issues
    that can make text invisible or break DataFrames.
    """
    if value is None:
        return ""
    s = str(value)
    # Replace zero-width and control characters that cause invisible text
    s = s.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    s = s.replace("\ufeff", "").replace("\xa0", " ")
    # Strip surrounding whitespace
    return s.strip()

def apply_period_filter(df: pd.DataFrame, col: str, period: str) -> pd.DataFrame:
    if period == "all" or col not in df.columns:
        return df
    now = datetime.now(TZ)
    if period == "today":
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_week":
        cutoff = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
    elif period == "this_month":
        cutoff = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return df
    dates = df[col].apply(parse_dt)
    return df[dates >= cutoff]

# ─────────────────────────────────────────────────────────────────────────────
#  7 · GOOGLE SHEETS CONNECTION  (cached for performance)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = json.loads(st.secrets["json_key"], strict=False)
    pk  = raw["private_key"]
    pk  = pk.replace("-----BEGIN PRIVATE KEY-----", "")
    pk  = pk.replace("-----END PRIVATE KEY-----", "")
    pk  = pk.replace("\\n", "").replace("\n", "")
    pk  = "".join(pk.split())
    pk  = "\n".join(textwrap.wrap(pk, 64))
    raw["private_key"] = (
        f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    )
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(raw, scope)
    client = gspread.authorize(creds)
    return client.open("site CIT QA - Tranche 4")

# ─────────────────────────────────────────────────────────────────────────────
#  8 · ROBUST DATA LOADER  (fixes invisible-text bug at the source)
# ─────────────────────────────────────────────────────────────────────────────
def load_worksheet(
    ws,
) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    """
    Fetch and sanitise a worksheet.

    Returns
    -------
    df        : fully cleaned DataFrame; every cell is a plain Python str
    headers   : ordered column names matching the Google Sheet
    col_map   : {col_name: 1-based Google Sheets column index}

    Fixes applied
    -------------
    • Header deduplication  →  no silent column-overlap
    • Row length normalisation  →  prevents shape-mismatch ValueError
    • clean_cell() on every value  →  removes zero-width / control chars
      that make text invisible while it technically exists in the DOM
    • Fully-empty rows dropped  →  clean display
    • System columns injected in-memory if missing
    """
    raw = ws.get_all_values()
    if not raw:
        return pd.DataFrame(), [], {}

    # ── Deduplicate headers ──────────────────────────────────────────────────
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

    # ── Normalise every data row ─────────────────────────────────────────────
    normalised: list[list[str]] = []
    for raw_row in raw[1:]:
        row = [clean_cell(c) for c in raw_row]
        # Pad short rows / truncate long rows  →  always n_cols wide
        if len(row) < n_cols:
            row += [""] * (n_cols - len(row))
        else:
            row = row[:n_cols]
        normalised.append(row)

    if not normalised:
        return pd.DataFrame(columns=headers), headers, {}

    df = pd.DataFrame(normalised, columns=headers)

    # ── Drop rows that are entirely empty ────────────────────────────────────
    df = df[~(df == "").all(axis=1)].reset_index(drop=True)

    # ── Inject system columns (in-memory placeholder) ────────────────────────
    for sc in SYSTEM_COLS:
        if sc not in df.columns:
            df[sc] = ""

    # Fill any remaining NaN / None with empty string
    df = df.fillna("").infer_objects(copy=False)

    col_map = {h: i + 1 for i, h in enumerate(headers)}
    return df, headers, col_map


def ensure_system_cols(
    ws,
    headers: list[str],
    col_map: dict[str, int],
) -> tuple[list[str], dict[str, int]]:
    """
    Guarantee every SYSTEM_COL exists in the actual Google Sheet.
    Automatically expands the grid before writing to avoid GridLimit errors.
    """
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers) + 1
            if new_pos > ws.col_count:
                # Add a safe buffer of 4 columns
                ws.add_cols(max(4, new_pos - ws.col_count + 1))
            ws.update_cell(1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map

# ─────────────────────────────────────────────────────────────────────────────
#  9 · AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────
def authenticate(email: str, password: str, users_ws) -> str | None:
    """Return 'admin', 'auditor', or None."""
    email = email.lower().strip()
    if email == "admin" and password == st.secrets.get("admin_password", ""):
        return "admin"
    try:
        recs = users_ws.get_all_records()
        df_u = pd.DataFrame(recs)
        if df_u.empty or "email" not in df_u.columns:
            return None
        row = df_u[df_u["email"] == email]
        if not row.empty and hash_pw(password) == str(row["password"].values[0]):
            return "auditor"
    except Exception:
        pass
    return None

# ─────────────────────────────────────────────────────────────────────────────
#  10 · LOGIN PAGE  (full-screen, no sidebar)
# ─────────────────────────────────────────────────────────────────────────────
def render_login(users_ws) -> None:
    # Completely hide sidebar & collapse button during login
    st.markdown("""
    <style>
      [data-testid="stSidebar"],
      [data-testid="collapsedControl"]
      { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    # Top-right micro-controls (language & theme only)
    gap, c1, c2, c3, c4 = st.columns([5, .55, .55, .55, .55])
    with c1:
        if st.button("EN", key="lg_en"): st.session_state.lang="en";  st.rerun()
    with c2:
        if st.button("KU", key="lg_ku"): st.session_state.lang="ku";  st.rerun()
    with c3:
        if st.button("☀️", key="lg_lgt"):st.session_state.theme="light";st.rerun()
    with c4:
        if st.button("🌙", key="lg_drk"):st.session_state.theme="dark"; st.rerun()

    # Centre the login card
    _, mid, _ = st.columns([1, 1.15, 1])
    with mid:
        st.markdown(f"""
        <div class="gov-login-card">
          <div class="gov-seal">🏛️</div>
          <div class="gov-ministry">{t('ministry')}</div>
          <div class="gov-portal-title">{t('portal_title')}</div>
          <div class="gold-divider"></div>
          <div class="gov-portal-sub">{t('portal_sub')}</div>
        </div>
        """, unsafe_allow_html=True)

        # The form is rendered outside the HTML block so Streamlit can handle it
        with st.form("login_form", clear_on_submit=False):
            st.markdown(
                f"<p style='font-size:.80rem;color:var(--text-muted);text-align:center;"
                f"margin-bottom:18px;'>{t('login_prompt')}</p>",
                unsafe_allow_html=True,
            )
            email_in = st.text_input(
                t("email"),
                placeholder="admin  ·  or  ·  auditor@mof.gov",
            )
            pass_in = st.text_input(
                t("password"),
                type="password",
                placeholder="••••••••",
            )
            submitted = st.form_submit_button(
                f"🔐  {t('sign_in')}",
                use_container_width=True,
            )

        if submitted:
            role = authenticate(email_in, pass_in, users_ws)
            if role:
                st.session_state.logged_in  = True
                st.session_state.user_email = "Admin" if role == "admin" else email_in.lower().strip()
                st.session_state.user_role  = role
                st.rerun()
            else:
                st.error(t("bad_creds"))

# ─────────────────────────────────────────────────────────────────────────────
#  11 · SIDEBAR (post-login only)
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar() -> None:
    with st.sidebar:
        # Brand strip with gold top-border
        st.markdown(f"""
        <div style="border-top:3px solid var(--gold);padding:20px 18px 16px;">
          <div style="font-size:1.1rem;font-weight:800;letter-spacing:-.01em;
                      color:var(--text-primary);margin-bottom:2px;">
            🏛️&nbsp; {t('portal_title')}
          </div>
          <div style="font-size:0.68rem;color:var(--text-muted);
                      letter-spacing:.12em;text-transform:uppercase;">
            {t('ministry')}
          </div>
        </div>
        <hr style="margin:0;border-color:var(--border);"/>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # Language
        st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"):
            st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang = "ku"; st.rerun()

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # Theme
        st.markdown(f"<div class='sb-label'>{t('theme')}</div>", unsafe_allow_html=True)
        tc1, tc2 = st.columns(2)
        if tc1.button("☀️  Light", use_container_width=True, key="sb_lgt"):
            st.session_state.theme = "light"; st.rerun()
        if tc2.button("🌙  Dark",  use_container_width=True, key="sb_drk"):
            st.session_state.theme = "dark";  st.rerun()

        st.markdown("<hr style='border-color:var(--border);margin:16px 0;'/>",
                    unsafe_allow_html=True)

        # User badge
        role_label = t("role_admin") if st.session_state.user_role == "admin" else t("role_auditor")
        chip_cls   = "chip-admin" if st.session_state.user_role == "admin" else "chip-audit"
        st.markdown(f"""
        <div class="sb-user-badge">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="chip {chip_cls}" style="margin-top:6px;">
            {role_label}
          </span>
        </div>
        """, unsafe_allow_html=True)

        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k, v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
#  12 · ANALYTICS TAB
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df: pd.DataFrame) -> None:
    pt = P["plotly_theme"]
    pb = P["plot_bg"]
    pg = P["plot_grid"]
    fc = P["text_primary"]

    # ── Period filter ────────────────────────────────────────────────────────
    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>",
                unsafe_allow_html=True)
    periods = [
        ("all",        t("all_time")),
        ("today",      t("today")),
        ("this_week",  t("this_week")),
        ("this_month", t("this_month")),
    ]
    btn_cols = st.columns(len(periods))
    for col_w, (pkey, plabel) in zip(btn_cols, periods):
        label = f"✓  {plabel}" if st.session_state.date_filter == pkey else plabel
        if col_w.button(label, use_container_width=True, key=f"pf_{pkey}"):
            st.session_state.date_filter = pkey; st.rerun()

    done_base = df[df[COL_STATUS] == VAL_DONE].copy()
    done_f    = apply_period_filter(done_base, COL_DATE, st.session_state.date_filter)

    if done_f.empty:
        st.info(t("no_records"))
        return

    # ── Summary metrics row ──────────────────────────────────────────────────
    ma, mb, mc = st.columns(3)
    ma.metric(t("records_period"), len(done_f))

    # active days
    active = 0
    if COL_DATE in done_f.columns:
        active = done_f[COL_DATE].apply(
            lambda s: parse_dt(s).date() if parse_dt(s) else None
        ).nunique()
    mb.metric(t("active_days"), active)

    avg_val = f"{len(done_f)/max(active,1):.1f}"
    mc.metric(t("avg_per_day"), avg_val)

    # ── Two-column chart layout ──────────────────────────────────────────────
    left, right = st.columns([1, 1.6], gap="large")

    # ── Leaderboard ──────────────────────────────────────────────────────────
    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>",
                    unsafe_allow_html=True)

        if COL_AUDITOR in done_f.columns:
            lb = (
                done_f[COL_AUDITOR]
                .replace("", "—")
                .value_counts()
                .reset_index()
            )
            lb.columns = ["Auditor", "Count"]

            medals = ["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i, row_lb in lb.head(10).iterrows():
                medal = medals[i] if i < len(medals) else f"{i+1}."
                st.markdown(f"""
                <div class="lb-row">
                  <span class="lb-medal">{medal}</span>
                  <span class="lb-name">{row_lb['Auditor']}</span>
                  <span class="lb-count">{row_lb['Count']}</span>
                </div>
                """, unsafe_allow_html=True)

            # Horizontal bar
            fig_lb = px.bar(
                lb.head(10), x="Count", y="Auditor",
                orientation="h",
                color="Count",
                color_continuous_scale=[P["blue_accent"], P["gold"]],
                template=pt,
            )
            fig_lb.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Inter", color=fc, size=11),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=8, r=8, t=10, b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color=fc)),
                yaxis=dict(gridcolor="rgba(0,0,0,0)",
                           categoryorder="total ascending",
                           tickfont=dict(color=fc)),
                height=min(320, max(180, 36 * len(lb.head(10)))),
            )
            fig_lb.update_traces(marker_line_width=0)
            st.plotly_chart(fig_lb, use_container_width=True)

    # ── Daily trend ──────────────────────────────────────────────────────────
    with right:
        st.markdown(f"<div class='section-title'>📈 {t('daily_trend')}</div>",
                    unsafe_allow_html=True)

        if COL_DATE in done_f.columns:
            done_f = done_f.copy()
            done_f["_date"] = done_f[COL_DATE].apply(
                lambda s: parse_dt(s).date() if parse_dt(s) else None
            )
            trend = (
                done_f.dropna(subset=["_date"])
                .groupby("_date")
                .size()
                .reset_index(name="Records")
            )
            trend.columns = ["Date", "Records"]

            if not trend.empty:
                # Fill date gaps for a continuous line
                if len(trend) > 1:
                    full_rng = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = (
                        trend.set_index("Date")
                        .reindex(full_rng.date, fill_value=0)
                        .reset_index()
                    )
                    trend.columns = ["Date", "Records"]

                fig_line = go.Figure()
                # Shaded area
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="none", fill="tozeroy",
                    fillcolor=P["gold_bg"], showlegend=False,
                ))
                # Main line
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="lines+markers",
                    line=dict(color=P["gold"], width=2.5),
                    marker=dict(
                        color=P["blue_accent"], size=7,
                        line=dict(color=P["card"], width=2)
                    ),
                    name=t("records_period"),
                ))
                fig_line.update_layout(
                    template=pt,
                    paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="Inter", color=fc, size=11),
                    showlegend=False,
                    margin=dict(l=8, r=8, t=10, b=8),
                    xaxis=dict(
                        gridcolor=pg, zeroline=False,
                        tickfont=dict(color=P["text_secondary"]),
                    ),
                    yaxis=dict(
                        gridcolor=pg, zeroline=False,
                        tickfont=dict(color=P["text_secondary"]),
                    ),
                    height=380,
                    hovermode="x unified",
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info(t("no_records"))

# ─────────────────────────────────────────────────────────────────────────────
#  13 · USER ADMINISTRATION TAB (admin only)
# ─────────────────────────────────────────────────────────────────────────────
def render_user_admin(users_ws) -> None:
    col_left, col_right = st.columns([1, 1], gap="large")

    # ── Register new auditor ─────────────────────────────────────────────────
    with col_left:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>",
                    unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_email = st.text_input("Email", placeholder="auditor@mof.gov")
            nu_pass  = st.text_input("Password", type="password")
            if st.form_submit_button("Register Auditor", use_container_width=True):
                if nu_email.strip() and nu_pass.strip():
                    recs  = pd.DataFrame(users_ws.get_all_records())
                    if not recs.empty and nu_email.lower().strip() in recs.get("email", pd.Series()).values:
                        st.error(t("dup_email"))
                    else:
                        users_ws.append_row(
                            [nu_email.lower().strip(), hash_pw(nu_pass.strip()), now_str()]
                        )
                        st.success(f"✅  {nu_email} registered.")
                        time.sleep(0.7); st.rerun()
                else:
                    st.warning(t("fill_fields"))

        # ── Update password ──────────────────────────────────────────────────
        st.markdown(f"<div class='section-title'>🔑 {t('update_pw')}</div>",
                    unsafe_allow_html=True)
        staff_df = pd.DataFrame(users_ws.get_all_records())
        if not staff_df.empty and "email" in staff_df.columns:
            with st.form("upd_pw_form"):
                sel_email = st.selectbox("Select staff member", staff_df["email"].tolist())
                new_pw    = st.text_input("New Password", type="password")
                if st.form_submit_button("Update Password", use_container_width=True):
                    if new_pw.strip():
                        cell = users_ws.find(sel_email)
                        if cell:
                            users_ws.update_cell(cell.row, 2, hash_pw(new_pw.strip()))
                            st.success(f"✅  Password updated for {sel_email}.")
                            time.sleep(0.7); st.rerun()

    # ── Staff directory & revoke ─────────────────────────────────────────────
    with col_right:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>",
                    unsafe_allow_html=True)
        staff_df = pd.DataFrame(users_ws.get_all_records())
        if not staff_df.empty and "email" in staff_df.columns:
            safe_cols = [c for c in ["email", "created_at"] if c in staff_df.columns]
            st.dataframe(staff_df[safe_cols], use_container_width=True, hide_index=True)

            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>",
                        unsafe_allow_html=True)
            del_email = st.selectbox(
                "Select account to revoke",
                ["—"] + staff_df["email"].tolist(),
                key="del_sel",
            )
            if del_email != "—":
                if st.button(f"Revoke access — {del_email}", key="del_btn"):
                    cell = users_ws.find(del_email)
                    if cell:
                        users_ws.delete_rows(cell.row)
                        st.success(f"✅  {del_email} access revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered.")

# ─────────────────────────────────────────────────────────────────────────────
#  14 · MAIN APPLICATION CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        spreadsheet   = get_spreadsheet()
        all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]

        # Ensure UsersDB exists
        if "UsersDB" not in all_ws_titles:
            users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="500", cols="3")
            users_ws.append_row(["email", "password", "created_at"])
        else:
            users_ws = spreadsheet.worksheet("UsersDB")

        # ── AUTHENTICATION GATE ──────────────────────────────────────────────
        if not st.session_state.logged_in:
            render_login(users_ws)
            return    # ← nothing below runs until authenticated

        # ── AUTHENTICATED SHELL ──────────────────────────────────────────────
        # Make sidebar visible
        st.markdown(
            "<style>[data-testid='stSidebar']{display:flex!important;}</style>",
            unsafe_allow_html=True,
        )
        render_sidebar()

        # Page header
        ts = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-title">🏛️  {t('portal_title')}</div>
        <div class="page-sub">{ts}</div>
        """, unsafe_allow_html=True)

        # Workspace selector
        data_sheets = [n for n in all_ws_titles if n != "UsersDB"]
        ws_name = st.selectbox(
            t("workspace"), data_sheets, key="ws_sel",
            label_visibility="visible",
        )
        current_ws           = spreadsheet.worksheet(ws_name)
        df, headers, col_map = load_worksheet(current_ws)

        if df.empty:
            st.warning(t("empty_sheet"))
            return

        is_admin = (st.session_state.user_role == "admin")

        # ── OVERVIEW METRICS ─────────────────────────────────────────────────
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
                  delta=f"{100-int(pct*100)}% remaining",
                  delta_color="inverse")

        st.markdown(f"""
        <div class="prog-labels">
          <span>{t('processed')}</span>
          <span>{int(pct*100)}%</span>
        </div>
        <div class="gov-progress-wrap">
          <div class="gov-progress-fill" style="width:{int(pct*100)}%;"></div>
        </div>
        """, unsafe_allow_html=True)

        # ── GLOBAL SEARCH ────────────────────────────────────────────────────
        q = st.text_input(t("search"), key="gsearch").strip()
        if q:
            mask   = df.astype(str).apply(
                lambda col: col.str.contains(q, case=False, na=False)
            ).any(axis=1)
            view_df = df[mask].copy()
        else:
            view_df = df.copy()

        # ── TABS ─────────────────────────────────────────────────────────────
        tab_names = [t("tab_queue"), t("tab_archive"), t("tab_analytics")]
        if is_admin:
            tab_names.append(t("tab_users"))
        tabs = st.tabs(tab_names)

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 1 — TASK QUEUE
        # ══════════════════════════════════════════════════════════════════════
        with tabs[0]:
            pending_view = view_df[view_df[COL_STATUS] != VAL_DONE].copy()
            # Align index with Google Sheets row numbers (row 1 = header)
            pending_view.index = pending_view.index + 2

            # ── Table display with explicit dtype string cast ─────────────────
            # Convert every column to str before displaying to prevent Arrow
            # serialisation issues that can cause invisible / broken text.
            display_pending = pending_view.astype(str)
            st.dataframe(
                display_pending,
                use_container_width=True,
                height=340,
            )

            st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>",
                        unsafe_allow_html=True)

            # Build option list using first non-system column as label
            label_col = next(
                (h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row"
            )
            opts = ["—"] + [
                f"Row {idx}  ·  {str(row.get(label_col, ''))[:55]}"
                for idx, row in pending_view.iterrows()
            ]
            row_sel = st.selectbox(
                "", opts, key="row_sel", label_visibility="collapsed"
            )

            if row_sel != "—":
                sheet_row = int(row_sel.split("  ·  ")[0].replace("Row ", "").strip())
                record    = df.iloc[sheet_row - 2].to_dict()

                # ── Audit trail ───────────────────────────────────────────────
                with st.expander(f"📜  {t('audit_trail')}", expanded=False):
                    history = str(record.get(COL_LOG, "")).strip()
                    if history:
                        for line in history.split("\n"):
                            if line.strip():
                                st.markdown(
                                    f'<div class="log-line">{line}</div>',
                                    unsafe_allow_html=True,
                                )
                    else:
                        st.caption(t("no_history"))

                # ── Vertical edit form ────────────────────────────────────────
                st.markdown(
                    f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>",
                    unsafe_allow_html=True,
                )
                SKIP   = set(SYSTEM_COLS)
                fields = {k: v for k, v in record.items() if k not in SKIP}

                with st.form("audit_form"):
                    new_vals: dict[str, str] = {}
                    for fname, fval in fields.items():
                        # One full-width input per row  → vertical layout
                        new_vals[fname] = st.text_input(
                            fname,
                            value=clean_cell(fval),
                            key=f"field_{fname}",
                        )

                    do_submit = st.form_submit_button(
                        f"✅  {t('approve_save')}",
                        use_container_width=True,
                    )

                if do_submit:
                    with st.spinner("Committing to register…"):
                        # Ensure system columns exist in the sheet
                        headers, col_map = ensure_system_cols(
                            current_ws, headers, col_map
                        )
                        # Write only changed user fields
                        for fname, fval in new_vals.items():
                            if fname in col_map and clean_cell(record.get(fname, "")) != fval:
                                current_ws.update_cell(sheet_row, col_map[fname], fval)

                        # Automated identity & timestamp — never entered by user
                        ts_now   = now_str()
                        auditor  = st.session_state.user_email
                        old_log  = str(record.get(COL_LOG, "")).strip()
                        new_log  = f"✔  {auditor}  |  {ts_now}\n{old_log}".strip()

                        current_ws.update_cell(sheet_row, col_map[COL_STATUS],  VAL_DONE)
                        current_ws.update_cell(sheet_row, col_map[COL_LOG],     new_log)
                        current_ws.update_cell(sheet_row, col_map[COL_AUDITOR], auditor)
                        current_ws.update_cell(sheet_row, col_map[COL_DATE],    ts_now)

                    st.success(t("saved_ok"))
                    time.sleep(0.8)
                    st.rerun()

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 2 — PROCESSED ARCHIVE
        # ══════════════════════════════════════════════════════════════════════
        with tabs[1]:
            done_view = view_df[view_df[COL_STATUS] == VAL_DONE].copy()
            done_view.index = done_view.index + 2

            st.dataframe(
                done_view.astype(str),
                use_container_width=True,
            )

            # Admin: re-open a processed record
            if is_admin and not done_view.empty:
                st.markdown("---")
                st.markdown(
                    f"<div class='section-title'>↩️ {t('reopen')}</div>",
                    unsafe_allow_html=True,
                )
                reopen_opts = ["—"] + [f"Row {idx}" for idx in done_view.index]
                reopen_sel  = st.selectbox(
                    "Select record to re-open:", reopen_opts, key="reopen_sel"
                )
                if reopen_sel != "—":
                    ridx = int(reopen_sel.split(" ")[1])
                    if st.button(t("reopen"), key="reopen_btn"):
                        if COL_STATUS in col_map:
                            current_ws.update_cell(ridx, col_map[COL_STATUS], VAL_PENDING)
                            st.rerun()

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 3 — ANALYTICS
        # ══════════════════════════════════════════════════════════════════════
        with tabs[2]:
            render_analytics(df)

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 4 — USER ADMINISTRATION  (admin only)
        # ══════════════════════════════════════════════════════════════════════
        if is_admin:
            with tabs[3]:
                render_user_admin(users_ws)

    except Exception as exc:
        st.error(f"🚨  System Error: {exc}")
        with st.expander("Technical Details", expanded=False):
            st.exception(exc)

# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
