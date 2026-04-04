# =============================================================================
#  OFFICIAL TAX AUDIT PORTAL  ·  v5.0
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
    initial_sidebar_state="expanded",
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
#  2 · THEME PALETTES
# ─────────────────────────────────────────────────────────────────────────────
_PALETTES: dict = {
    "dark": {
        "page_bg"       : "#060B14",
        "surface"       : "#0C1526",
        "surface2"      : "#10192E",
        "card"          : "#0E1729",
        "card2"         : "#121F36",
        "border"        : "#1A2C48",
        "border2"       : "#203858",
        "text_primary"  : "#E8EDF8",
        "text_secondary": "#7A9CC4",
        "text_muted"    : "#3D5A80",
        "gold"          : "#C8A84B",
        "gold_light"    : "#E4C878",
        "gold_bg"       : "rgba(200,168,75,0.10)",
        "blue_accent"   : "#3470CC",
        "blue_bg"       : "rgba(52,112,204,0.12)",
        "green"         : "#25A374",
        "green_bg"      : "rgba(37,163,116,0.12)",
        "amber"         : "#E09820",
        "amber_bg"      : "rgba(224,152,32,0.12)",
        "red"           : "#D94F4F",
        "red_bg"        : "rgba(217,79,79,0.12)",
        "input_bg"      : "#060B14",
        "input_border"  : "#1A2C48",
        "btn_primary"   : "#C8A84B",
        "btn_text"      : "#060B14",
        "prog_track"    : "#1A2C48",
        "prog_fill_a"   : "#C8A84B",
        "prog_fill_b"   : "#3470CC",
        "plotly_theme"  : "plotly_dark",
        "plot_bg"       : "#0C1526",
        "plot_grid"     : "#1A2C48",
        # ── CRITICAL: explicit table colours ──
        "tbl_bg"        : "#0E1729",
        "tbl_header_bg" : "#10192E",
        "tbl_text"      : "#E8EDF8",   # ← always visible on dark bg
        "tbl_header_txt": "#7A9CC4",
        "tbl_border"    : "#1A2C48",
        "tbl_row_hover" : "#121F36",
    },
    "light": {
        "page_bg"       : "#F2F5FB",
        "surface"       : "#FFFFFF",
        "surface2"      : "#EAF0FA",
        "card"          : "#FFFFFF",
        "card2"         : "#F5F8FE",
        "border"        : "#C8D8F0",
        "border2"       : "#AABDE0",
        "text_primary"  : "#06172E",
        "text_secondary": "#2E4F78",
        "text_muted"    : "#6A86A8",
        "gold"          : "#9A7020",
        "gold_light"    : "#B88A30",
        "gold_bg"       : "rgba(154,112,32,0.08)",
        "blue_accent"   : "#1658B8",
        "blue_bg"       : "rgba(22,88,184,0.08)",
        "green"         : "#157A50",
        "green_bg"      : "rgba(21,122,80,0.08)",
        "amber"         : "#A85808",
        "amber_bg"      : "rgba(168,88,8,0.08)",
        "red"           : "#A82828",
        "red_bg"        : "rgba(168,40,40,0.08)",
        "input_bg"      : "#FFFFFF",
        "input_border"  : "#B8CCE8",
        "btn_primary"   : "#0D2A58",
        "btn_text"      : "#FFFFFF",
        "prog_track"    : "#D5E3F5",
        "prog_fill_a"   : "#9A7020",
        "prog_fill_b"   : "#1658B8",
        "plotly_theme"  : "plotly_white",
        "plot_bg"       : "#FFFFFF",
        "plot_grid"     : "#D5E3F5",
        "tbl_bg"        : "#FFFFFF",
        "tbl_header_bg" : "#EAF0FA",
        "tbl_text"      : "#06172E",   # ← always visible on light bg
        "tbl_header_txt": "#2E4F78",
        "tbl_border"    : "#C8D8F0",
        "tbl_row_hover" : "#F0F6FF",
    },
}

P = _PALETTES[st.session_state.theme]

# ─────────────────────────────────────────────────────────────────────────────
#  3 · FULL CSS INJECTION
# ─────────────────────────────────────────────────────────────────────────────
def inject_css(P: dict) -> None:
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

/* ── CSS Variables ── */
:root {{
  --page-bg        : {P['page_bg']};
  --surface        : {P['surface']};
  --surface2       : {P['surface2']};
  --card           : {P['card']};
  --card2          : {P['card2']};
  --border         : {P['border']};
  --border2        : {P['border2']};
  --text-primary   : {P['text_primary']};
  --text-secondary : {P['text_secondary']};
  --text-muted     : {P['text_muted']};
  --gold           : {P['gold']};
  --gold-light     : {P['gold_light']};
  --gold-bg        : {P['gold_bg']};
  --blue           : {P['blue_accent']};
  --blue-bg        : {P['blue_bg']};
  --green          : {P['green']};
  --green-bg       : {P['green_bg']};
  --amber          : {P['amber']};
  --amber-bg       : {P['amber_bg']};
  --red            : {P['red']};
  --red-bg         : {P['red_bg']};
  --input-bg       : {P['input_bg']};
  --input-border   : {P['input_border']};
  --btn-primary    : {P['btn_primary']};
  --btn-text       : {P['btn_text']};
  --prog-track     : {P['prog_track']};
  --tbl-bg         : {P['tbl_bg']};
  --tbl-header-bg  : {P['tbl_header_bg']};
  --tbl-text       : {P['tbl_text']};
  --tbl-header-txt : {P['tbl_header_txt']};
  --tbl-border     : {P['tbl_border']};
  --tbl-row-hover  : {P['tbl_row_hover']};
}}

/* ── Global ── */
*, *::before, *::after {{ box-sizing: border-box !important; }}
html, body, .stApp, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main, .block-container {{
  font-family      : 'IBM Plex Sans', sans-serif !important;
  background-color : var(--page-bg) !important;
  color            : var(--text-primary) !important;
}}
p, span, div, li, td, th, label {{
  color: var(--text-primary) !important;
}}
#MainMenu, footer, header, .stDeployButton,
[data-testid="stToolbar"] {{ display: none !important; }}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
  background   : var(--surface) !important;
  border-right : 1px solid var(--border) !important;
  padding-top  : 0 !important;
}}
[data-testid="stSidebar"] > div {{ padding: 0 !important; }}
[data-testid="stSidebar"] * {{
  color       : var(--text-primary) !important;
  font-family : 'IBM Plex Sans', sans-serif !important;
}}
[data-testid="stSidebarCollapseButton"] {{ display: none !important; }}

/* ── Metrics ── */
[data-testid="stMetricContainer"] {{
  background    : var(--card) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 12px !important;
  padding       : 18px 22px !important;
  box-shadow    : 0 2px 8px rgba(0,0,0,0.12) !important;
  transition    : transform .2s ease, box-shadow .2s ease !important;
}}
[data-testid="stMetricContainer"]:hover {{
  transform  : translateY(-4px) !important;
  box-shadow : 0 10px 24px rgba(0,0,0,0.20),
               0 0 0 1px var(--gold) !important;
}}
[data-testid="stMetricValue"] {{
  font-family : 'IBM Plex Mono', monospace !important;
  font-size   : 2.1rem !important;
  font-weight : 600 !important;
  color       : var(--gold) !important;
}}
[data-testid="stMetricLabel"] {{
  font-size      : 0.68rem !important;
  font-weight    : 700 !important;
  letter-spacing : 0.12em !important;
  text-transform : uppercase !important;
  color          : var(--text-muted) !important;
}}

/* ── Forms ── */
div[data-testid="stForm"] {{
  background    : var(--card) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 14px !important;
  padding       : 24px 28px !important;
}}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stTextArea  > div > div > textarea,
.stSelectbox > div > div > div,
.stMultiSelect > div > div > div {{
  background    : var(--input-bg)     !important;
  color         : var(--text-primary) !important;
  border        : 1px solid var(--input-border) !important;
  border-radius : 7px !important;
  font-family   : 'IBM Plex Sans', sans-serif !important;
  font-size     : 0.875rem !important;
  caret-color   : var(--gold) !important;
}}
.stTextInput > div > div > input:focus,
.stTextArea  > div > div > textarea:focus {{
  border-color : var(--gold) !important;
  box-shadow   : 0 0 0 3px var(--gold-bg) !important;
  outline      : none !important;
}}
.stTextInput label, .stTextArea label,
.stSelectbox label, .stMultiSelect label {{
  color          : var(--text-muted)  !important;
  font-size      : 0.68rem !important;
  font-weight    : 700 !important;
  letter-spacing : 0.10em !important;
  text-transform : uppercase !important;
}}
.stTextInput > div > div > input::placeholder,
.stTextArea  > div > div > textarea::placeholder {{
  color   : var(--text-muted) !important;
  opacity : 0.6 !important;
}}

/* ── Buttons ── */
.stButton > button {{
  background     : var(--btn-primary) !important;
  color          : var(--btn-text)    !important;
  border         : none !important;
  border-radius  : 7px !important;
  font-family    : 'IBM Plex Sans', sans-serif !important;
  font-weight    : 600 !important;
  font-size      : 0.84rem !important;
  letter-spacing : 0.03em !important;
  padding        : 9px 18px !important;
  transition     : opacity .15s ease, transform .15s ease !important;
}}
.stButton > button:hover  {{ opacity: .85 !important; transform: translateY(-1px) !important; }}
.stButton > button:active {{ transform: translateY(0) !important; }}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {{
  gap           : 2px !important;
  background    : transparent !important;
  border-bottom : 2px solid var(--border) !important;
}}
.stTabs [data-baseweb="tab"] {{
  background    : transparent !important;
  color         : var(--text-muted) !important;
  border-radius : 8px 8px 0 0 !important;
  border        : 1px solid transparent !important;
  border-bottom : none !important;
  padding       : 10px 20px !important;
  font-weight   : 600 !important;
  font-size     : 0.82rem !important;
  letter-spacing: 0.03em !important;
}}
.stTabs [data-baseweb="tab"]:hover {{ color: var(--text-primary) !important; }}
.stTabs [aria-selected="true"] {{
  background          : var(--card) !important;
  color               : var(--gold) !important;
  border-color        : var(--border) !important;
  border-bottom-color : var(--card) !important;
  margin-bottom       : -2px !important;
}}

/* ════════════════════════════════════════════════════════════════
   CRITICAL FIX — HTML TABLE TEXT VISIBILITY
   We use st.markdown HTML tables instead of st.dataframe to
   guarantee pixel-perfect text colour in both themes.
════════════════════════════════════════════════════════════════ */
.gov-table-wrap {{
  overflow-x    : auto;
  border        : 1px solid var(--tbl-border);
  border-radius : 12px;
  margin-bottom : 16px;
}}
.gov-table {{
  width           : 100%;
  border-collapse : collapse;
  background-color: var(--tbl-bg);
  font-family     : 'IBM Plex Sans', sans-serif;
  font-size       : 0.82rem;
}}
.gov-table thead tr {{
  background-color : var(--tbl-header-bg);
  border-bottom    : 2px solid var(--border2);
}}
.gov-table th {{
  color          : var(--tbl-header-txt) !important;
  font-weight    : 700 !important;
  font-size      : 0.68rem !important;
  letter-spacing : 0.09em !important;
  text-transform : uppercase !important;
  padding        : 11px 14px !important;
  white-space    : nowrap;
  text-align     : left !important;
  border-right   : 1px solid var(--tbl-border);
}}
.gov-table th:last-child {{ border-right: none; }}
.gov-table td {{
  color          : var(--tbl-text) !important;
  padding        : 9px 14px !important;
  font-size      : 0.82rem !important;
  border-bottom  : 1px solid var(--tbl-border) !important;
  border-right   : 1px solid var(--tbl-border) !important;
  vertical-align : middle !important;
  max-width      : 220px;
  overflow       : hidden;
  text-overflow  : ellipsis;
  white-space    : nowrap;
}}
.gov-table td:last-child {{ border-right: none; }}
.gov-table tbody tr:hover td {{
  background-color : var(--tbl-row-hover) !important;
}}
.gov-table tbody tr:last-child td {{ border-bottom: none !important; }}
/* Row index column */
.gov-table td.row-idx, .gov-table th.row-idx {{
  color      : var(--text-muted) !important;
  font-family: 'IBM Plex Mono', monospace !important;
  font-size  : 0.72rem !important;
  min-width  : 46px;
  text-align : center !important;
}}

/* Status chips inside tables */
.s-chip {{
  display        : inline-flex;
  align-items    : center;
  padding        : 2px 9px;
  border-radius  : 99px;
  font-size      : 0.65rem;
  font-weight    : 700;
  letter-spacing : 0.07em;
  text-transform : uppercase;
}}
.s-done    {{ background: {P['green_bg']}; color: {P['green']}; }}
.s-pending {{ background: {P['amber_bg']}; color: {P['amber']}; }}

/* ── Filter panel ── */
.filter-panel {{
  background    : var(--card);
  border        : 1px solid var(--border);
  border-left   : 3px solid var(--gold);
  border-radius : 10px;
  padding       : 14px 16px;
  margin-bottom : 16px;
}}
.filter-title {{
  font-size      : 0.68rem;
  font-weight    : 700;
  letter-spacing : 0.12em;
  text-transform : uppercase;
  color          : var(--gold);
  margin-bottom  : 10px;
}}
.filter-badge {{
  display        : inline-flex;
  align-items    : center;
  gap            : 5px;
  background     : var(--blue-bg);
  color          : var(--blue) !important;
  border         : 1px solid var(--blue);
  border-radius  : 99px;
  font-size      : 0.68rem;
  font-weight    : 700;
  padding        : 2px 9px;
  margin-right   : 5px;
  margin-top     : 4px;
}}
.result-count {{
  font-family : 'IBM Plex Mono', monospace;
  font-size   : 0.78rem;
  color       : var(--text-muted);
  padding     : 4px 0 10px;
}}

/* ── Expander ── */
.streamlit-expanderHeader {{
  background    : var(--surface2) !important;
  color         : var(--text-primary) !important;
  border        : 1px solid var(--border) !important;
  border-radius : 8px !important;
  font-weight   : 600 !important;
  font-size     : 0.84rem !important;
}}
.streamlit-expanderContent {{
  background    : var(--card) !important;
  border        : 1px solid var(--border) !important;
  border-top    : none !important;
  border-radius : 0 0 8px 8px !important;
  padding       : 14px !important;
}}

/* ── Alerts ── */
[data-testid="stAlert"] {{
  border-radius : 9px !important;
  border        : 1px solid var(--border) !important;
}}

/* ── Login card ── */
.gov-login-card {{
  width         : 100%;
  max-width     : 440px;
  background    : var(--card);
  border        : 1px solid var(--border2);
  border-top    : 4px solid var(--gold);
  border-radius : 16px;
  padding       : 42px 40px 36px;
  box-shadow    : 0 24px 60px rgba(0,0,0,0.26);
}}
.gov-seal {{
  font-size    : 3.2rem;
  text-align   : center;
  margin-bottom: 8px;
  filter       : drop-shadow(0 2px 8px rgba(200,168,75,0.35));
}}
.gov-ministry {{
  font-size      : 0.65rem;
  font-weight    : 700;
  letter-spacing : 0.18em;
  text-transform : uppercase;
  text-align     : center;
  color          : var(--text-muted);
  margin-bottom  : 4px;
}}
.gov-portal-title {{
  font-size     : 1.35rem;
  font-weight   : 700;
  text-align    : center;
  color         : var(--text-primary);
  letter-spacing: -0.02em;
  margin-bottom : 4px;
}}
.gov-portal-sub {{
  font-size    : 0.78rem;
  text-align   : center;
  color        : var(--text-muted);
  margin-bottom: 24px;
}}
.gold-divider {{
  width        : 44px;
  height       : 3px;
  background   : var(--gold);
  border-radius: 99px;
  margin       : 8px auto 22px;
}}

/* ── Page / section headers ── */
.page-title {{
  font-size     : 1.5rem;
  font-weight   : 700;
  color         : var(--text-primary);
  letter-spacing: -0.02em;
  margin-bottom : 2px;
}}
.page-sub {{ font-size: 0.80rem; color: var(--text-muted); margin-bottom: 20px; }}
.section-title {{
  display       : flex;
  align-items   : center;
  gap           : 9px;
  font-size     : 0.86rem;
  font-weight   : 700;
  color         : var(--text-primary);
  letter-spacing: 0.01em;
  margin        : 20px 0 11px;
  padding-left  : 11px;
  border-left   : 3px solid var(--gold);
}}
.worklist-header {{
  display        : flex;
  align-items    : center;
  justify-content: space-between;
  background     : var(--surface2);
  border         : 1px solid var(--border);
  border-top     : 3px solid var(--blue);
  border-radius  : 10px;
  padding        : 12px 18px;
  margin-bottom  : 14px;
}}
.worklist-title {{
  font-size     : 0.94rem;
  font-weight   : 700;
  color         : var(--text-primary);
  letter-spacing: -0.01em;
}}
.worklist-sub {{
  font-size : 0.72rem;
  color     : var(--text-muted);
  margin-top: 2px;
}}

/* ── Progress ── */
.gov-progress-wrap {{
  background    : var(--prog-track);
  border-radius : 99px;
  height        : 6px;
  overflow      : hidden;
  margin        : 5px 0 9px;
}}
.gov-progress-fill {{
  height       : 100%;
  border-radius: 99px;
  background   : linear-gradient(90deg, {P['prog_fill_a']}, {P['prog_fill_b']});
  transition   : width .7s cubic-bezier(.4,0,.2,1);
}}
.prog-labels {{
  display        : flex;
  justify-content: space-between;
  font-size      : 0.70rem;
  color          : var(--text-muted);
  font-weight    : 600;
  margin-bottom  : 3px;
}}

/* ── Status chips (general) ── */
.chip {{
  display        : inline-flex;
  align-items    : center;
  gap            : 4px;
  padding        : 3px 10px;
  border-radius  : 99px;
  font-size      : 0.68rem;
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
  border-radius : 9px;
  padding       : 11px 13px;
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
.sb-email {{ font-size: 0.85rem; font-weight: 700; color: var(--text-primary); }}

/* ── Leaderboard ── */
.lb-row {{
  display       : flex;
  align-items   : center;
  gap           : 11px;
  padding       : 10px 14px;
  background    : var(--card2);
  border        : 1px solid var(--border);
  border-radius : 9px;
  margin-bottom : 6px;
  transition    : transform .16s ease, border-color .16s ease;
}}
.lb-row:hover {{ transform: translateX(4px); border-color: var(--gold); }}
.lb-medal {{ font-size: 1.1rem; width: 24px; text-align: center; }}
.lb-name  {{ flex: 1; font-size: 0.84rem; font-weight: 600; color: var(--text-primary); }}
.lb-count {{
  font-family : 'IBM Plex Mono', monospace;
  font-size   : 0.92rem;
  font-weight : 700;
  color       : var(--gold);
}}

/* ── Audit trail ── */
.log-line {{
  font-family  : 'IBM Plex Mono', monospace;
  font-size    : 0.74rem;
  color        : var(--text-secondary);
  padding      : 3px 0;
  border-bottom: 1px solid var(--border);
}}
.log-line:last-child {{ border-bottom: none; }}

/* ── Inspection form label ── */
.inspect-row {{
  background    : var(--card2);
  border        : 1px solid var(--border);
  border-radius : 8px;
  padding       : 10px 14px;
  margin-bottom : 8px;
}}
.inspect-col-name {{
  font-size      : 0.66rem;
  font-weight    : 700;
  letter-spacing : 0.09em;
  text-transform : uppercase;
  color          : var(--text-muted);
  margin-bottom  : 4px;
}}

/* ── Sidebar filter section ── */
.sb-filter-section {{
  background    : var(--surface2);
  border        : 1px solid var(--border);
  border-radius : 9px;
  padding       : 12px 14px;
  margin-bottom : 12px;
}}
.sb-filter-title {{
  font-size      : 0.60rem;
  font-weight    : 700;
  letter-spacing : 0.14em;
  text-transform : uppercase;
  color          : var(--gold);
  margin-bottom  : 10px;
}}
</style>
""", unsafe_allow_html=True)

inject_css(P)

# ─────────────────────────────────────────────────────────────────────────────
#  4 · TRANSLATIONS
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry"        : "Ministry of Finance & Customs",
        "portal_title"    : "Official Tax Audit Portal",
        "portal_sub"      : "Secure · Classified · Government Use Only",
        "login_prompt"    : "Enter your authorised credentials to proceed",
        "email"           : "Official Email / User ID",
        "password"        : "Password",
        "sign_in"         : "Authenticate",
        "sign_out"        : "Sign Out",
        "bad_creds"       : "Authentication failed. Verify your credentials.",
        "theme"           : "Display Theme",
        "language"        : "Interface Language",
        "workspace"       : "Select Case Register",
        "overview"        : "Case Overview",
        "total"           : "Total Cases",
        "processed"       : "Processed",
        "outstanding"     : "Outstanding",
        "worklist_title"  : "Audit Worklist",
        "worklist_sub"    : "Active cases pending review and approval",
        "tab_queue"       : "📋  Audit Worklist",
        "tab_archive"     : "✅  Processed Archive",
        "tab_analytics"   : "📊  Analytics",
        "tab_users"       : "⚙️  User Admin",
        "select_case"     : "Select a case to inspect",
        "audit_trail"     : "Audit Trail",
        "approve_save"    : "Approve & Commit Record",
        "reopen"          : "Re-open Record (Admin)",
        "leaderboard"     : "Auditor Productivity Leaderboard",
        "daily_trend"     : "Daily Processing Trend",
        "period"          : "Time Period",
        "today"           : "Today",
        "this_week"       : "This Week",
        "this_month"      : "This Month",
        "all_time"        : "All Time",
        "add_auditor"     : "Register New Auditor",
        "update_pw"       : "Update Password",
        "remove_user"     : "Revoke Access",
        "staff_dir"       : "Authorised Staff",
        "no_records"      : "No records found for this period.",
        "empty_sheet"     : "This register contains no data.",
        "saved_ok"        : "Record approved and committed to the register.",
        "dup_email"       : "This email is already registered.",
        "fill_fields"     : "All fields are required.",
        "signed_as"       : "Authenticated as",
        "role_admin"      : "System Administrator",
        "role_auditor"    : "Tax Auditor",
        "processing"      : "Inspecting Case",
        "no_history"      : "No audit trail for this record.",
        "records_period"  : "Records (period)",
        "active_days"     : "Active Days",
        "avg_per_day"     : "Avg / Day",
        "filters"         : "Advanced Filters",
        "filter_email"    : "Auditor Email",
        "filter_binder"   : "Company Binder No.",
        "filter_company"  : "Company Name",
        "filter_license"  : "License Number",
        "filter_status"   : "Status",
        "clear_filters"   : "Clear All Filters",
        "active_filters"  : "Active filters",
        "results_shown"   : "results shown",
        "no_filter_match" : "No records match the applied filters.",
        "status_all"      : "All Statuses",
        "status_pending"  : "Pending Only",
        "status_done"     : "Processed Only",
    },
    "ku": {
        "ministry"        : "وەزارەتی دارایی و گومرگ",
        "portal_title"    : "پۆرتەلی وردبینی باجی فەرمی",
        "portal_sub"      : "پارێزراو · نهێنی · تەنها بەکارهێنانی حکومی",
        "login_prompt"    : "زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email"           : "ئیمەیڵی فەرمی / ناساندن",
        "password"        : "پاسۆرد",
        "sign_in"         : "دەستپێبکە",
        "sign_out"        : "چوونەدەرەوە",
        "bad_creds"       : "ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "theme"           : "تیمی پیشاندان",
        "language"        : "زمانی ڕووکار",
        "workspace"       : "تۆماری کیسەکان هەڵبژێرە",
        "overview"        : "کورتەی کیسەکان",
        "total"           : "کۆی کیسەکان",
        "processed"       : "کارکراوە",
        "outstanding"     : "ماوە",
        "worklist_title"  : "لیستی کاری وردبینی",
        "worklist_sub"    : "کیسە چالاکەکان کە چاوەڕوانی پشکنین و پەسەندکردنن",
        "tab_queue"       : "📋  لیستی کاری وردبینی",
        "tab_archive"     : "✅  ئەرشیفی کارکراو",
        "tab_analytics"   : "📊  ئەنالیتیکس",
        "tab_users"       : "⚙️  بەڕێوەبردنی بەکارهێنەر",
        "select_case"     : "کیسێک هەڵبژێرە بۆ پشکنین",
        "audit_trail"     : "مێژووی گۆڕانکاری",
        "approve_save"    : "پەسەندکردن و پاشەکەوتکردن",
        "reopen"          : "کردنەوەی دووبارەی کیس (ئەدمین)",
        "leaderboard"     : "تەختەی بەرهەمهێنانی ئۆدیتۆر",
        "daily_trend"     : "ترەندی بەرپرسانەی ڕۆژانە",
        "period"          : "ماوەی کات",
        "today"           : "ئەمڕۆ",
        "this_week"       : "ئەم هەفتەیە",
        "this_month"      : "ئەم مانگەیە",
        "all_time"        : "هەموو کات",
        "add_auditor"     : "تۆمارکردنی ئۆدیتۆری نوێ",
        "update_pw"       : "نوێکردنەوەی پاسۆرد",
        "remove_user"     : "هەڵوەشاندنەوەی دەستپێگەیشتن",
        "staff_dir"       : "کارمەندە مەرجداركراوەکان",
        "no_records"      : "هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet"     : "ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok"        : "کیسەکە پەسەندکرا و پاشەکەوتکرا.",
        "dup_email"       : "ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields"     : "هەموو خانەکان پەیوەندییانە.",
        "signed_as"       : "چووییتە ژوورەوە بەناوی",
        "role_admin"      : "بەڕێوەبەری سیستەم",
        "role_auditor"    : "ئۆدیتۆری باج",
        "processing"      : "پشکنینی کیسی",
        "no_history"      : "هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period"  : "تۆمارەکان (ماوە)",
        "active_days"     : "ڕۆژی چالاک",
        "avg_per_day"     : "تێکڕای ڕۆژانە",
        "filters"         : "فلتەرە پێشکەوتووەکان",
        "filter_email"    : "ئیمەیڵی ئۆدیتۆر",
        "filter_binder"   : "ژمارەی بایندەری کۆمپانیا",
        "filter_company"  : "ناوی کۆمپانیا",
        "filter_license"  : "ژمارەی مۆڵەتی کۆمپانیا",
        "filter_status"   : "دەربار",
        "clear_filters"   : "سڕینەوەی هەموو فلتەرەکان",
        "active_filters"  : "فلتەرە چالاکەکان",
        "results_shown"   : "ئەنجامی پیشاندراو",
        "no_filter_match" : "هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "status_all"      : "هەموو دەرباریەکان",
        "status_pending"  : "چاوەڕوان تەنها",
        "status_done"     : "کارکراو تەنها",
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
#  6 · COLUMN DETECTION  (fuzzy-match known Arabic/Kurdish labels)
# ─────────────────────────────────────────────────────────────────────────────
# These keyword lists map to the four filterable columns.
# The detector searches column headers for any of these substrings (case-insensitive).
_COL_KEYWORDS = {
    "binder" : [
        "binder", "ملف", "بايندر", "بایندەر", "file no", "file_no",
        "رقم ملف", "رقم_ملف", "ژمارەی بایندەری",
    ],
    "company": [
        "company", "اسم الشركة", "اسم_الشركة", "شركة", "كومبانيا",
        "کۆمپانیا", "company name", "company_name",
    ],
    "license": [
        "license", "ترخيص", "ترخیص", "رقم الترخيص", "رقم_الترخيص",
        "مۆڵەت", "مۆڵەتی", "license no", "license_no",
    ],
}

def detect_column(headers: list[str], kind: str) -> str | None:
    """Return first header that contains any keyword for 'kind', or None."""
    keywords = _COL_KEYWORDS.get(kind, [])
    for h in headers:
        hl = h.lower()
        for kw in keywords:
            if kw.lower() in hl:
                return h
    return None

# ─────────────────────────────────────────────────────────────────────────────
#  7 · HELPERS
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
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\u200b","").replace("\u200c","").replace("\u200d","")
    s = s.replace("\ufeff","").replace("\xa0"," ")
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
#  8 · HTML TABLE RENDERER  (guaranteed text visibility — no canvas)
# ─────────────────────────────────────────────────────────────────────────────
def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    """
    Renders a DataFrame as a styled HTML table with guaranteed text
    visibility in both dark and light themes.

    Uses CSS variables defined in inject_css() so the table always
    respects the active palette — no canvas, no invisible text.
    """
    if df.empty:
        st.info("No records to display.")
        return

    display_df = df.head(max_rows).copy()

    # ── Build header row ─────────────────────────────────────────────────────
    th_cells = "<th class='row-idx'>#</th>"
    for col in display_df.columns:
        # Skip raw audit log in table — too long
        if col == COL_LOG:
            continue
        th_cells += f"<th>{col}</th>"

    # ── Build data rows ──────────────────────────────────────────────────────
    rows_html = ""
    for i, (idx, row) in enumerate(display_df.iterrows()):
        # Alternate row shading via zebra inline style
        row_html = f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col == COL_LOG:
                continue
            cell_val = str(row[col]) if row[col] != "" else "—"
            # Status chips
            if col == COL_STATUS:
                if cell_val == VAL_DONE:
                    cell_val = f"<span class='s-chip s-done'>✓ {VAL_DONE}</span>"
                elif cell_val in (VAL_PENDING, ""):
                    cell_val = f"<span class='s-chip s-pending'>⏳ Pending</span>"
            # Truncate long text
            display_val = cell_val if len(str(cell_val)) <= 60 else cell_val[:57] + "…"
            row_html += f"<td title='{str(row[col])}'>{display_val}</td>"
        rows_html += f"<tr>{row_html}</tr>"

    html = f"""
    <div class="gov-table-wrap">
      <table class="gov-table">
        <thead><tr>{th_cells}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  9 · FILTER LOGIC
# ─────────────────────────────────────────────────────────────────────────────
def apply_filters(
    df: pd.DataFrame,
    f_email: str,
    f_binder: str,
    f_company: str,
    f_license: str,
    f_status: str,
    col_binder: str | None,
    col_company: str | None,
    col_license: str | None,
) -> pd.DataFrame:
    """
    Apply all active filters to df.  All string filters are
    case-insensitive partial matches.  Returns filtered DataFrame
    (never raises — returns empty df on zero matches).
    """
    result = df.copy()

    # Status filter
    if f_status == "pending":
        result = result[result[COL_STATUS] != VAL_DONE]
    elif f_status == "done":
        result = result[result[COL_STATUS] == VAL_DONE]

    # Auditor email
    if f_email.strip() and COL_AUDITOR in result.columns:
        result = result[
            result[COL_AUDITOR].str.contains(f_email.strip(), case=False, na=False)
        ]

    # Binder number
    if f_binder.strip() and col_binder and col_binder in result.columns:
        result = result[
            result[col_binder].str.contains(f_binder.strip(), case=False, na=False)
        ]

    # Company name
    if f_company.strip() and col_company and col_company in result.columns:
        result = result[
            result[col_company].str.contains(f_company.strip(), case=False, na=False)
        ]

    # License number
    if f_license.strip() and col_license and col_license in result.columns:
        result = result[
            result[col_license].str.contains(f_license.strip(), case=False, na=False)
        ]

    return result

def active_filter_count(f_email, f_binder, f_company, f_license, f_status) -> int:
    count = 0
    if f_email.strip():   count += 1
    if f_binder.strip():  count += 1
    if f_company.strip(): count += 1
    if f_license.strip(): count += 1
    if f_status != "all": count += 1
    return count

# ─────────────────────────────────────────────────────────────────────────────
#  10 · GOOGLE SHEETS CONNECTION
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
#  11 · DATA LOADER
# ─────────────────────────────────────────────────────────────────────────────
def load_worksheet(ws) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    raw = ws.get_all_values()
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
    normalised: list[list[str]] = []
    for raw_row in raw[1:]:
        row = [clean_cell(c) for c in raw_row]
        if len(row) < n_cols:
            row += [""] * (n_cols - len(row))
        else:
            row = row[:n_cols]
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


def ensure_system_cols(ws, headers, col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers) + 1
            if new_pos > ws.col_count:
                ws.add_cols(max(4, new_pos - ws.col_count + 1))
            ws.update_cell(1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map

# ─────────────────────────────────────────────────────────────────────────────
#  12 · AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────
def authenticate(email: str, password: str, users_ws) -> str | None:
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
#  13 · LOGIN PAGE
# ─────────────────────────────────────────────────────────────────────────────
def render_login(users_ws) -> None:
    st.markdown("""
    <style>
      [data-testid="stSidebar"],
      [data-testid="collapsedControl"]
      { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    gap, c1, c2, c3, c4 = st.columns([5, .55, .55, .55, .55])
    with c1:
        if st.button("EN", key="lg_en"): st.session_state.lang="en";  st.rerun()
    with c2:
        if st.button("KU", key="lg_ku"): st.session_state.lang="ku";  st.rerun()
    with c3:
        if st.button("☀️", key="lg_lgt"):st.session_state.theme="light";st.rerun()
    with c4:
        if st.button("🌙", key="lg_drk"):st.session_state.theme="dark"; st.rerun()

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

        with st.form("login_form", clear_on_submit=False):
            st.markdown(
                f"<p style='font-size:.78rem;color:var(--text-muted);text-align:center;"
                f"margin-bottom:16px;'>{t('login_prompt')}</p>",
                unsafe_allow_html=True,
            )
            email_in = st.text_input(
                t("email"), placeholder="admin  ·  or  ·  auditor@mof.gov"
            )
            pass_in = st.text_input(
                t("password"), type="password", placeholder="••••••••"
            )
            submitted = st.form_submit_button(
                f"🔐  {t('sign_in')}", use_container_width=True
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
#  14 · SIDEBAR (post-login)
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(
    df: pd.DataFrame,
    headers: list[str],
    col_binder: str | None,
    col_company: str | None,
    col_license: str | None,
) -> tuple[str, str, str, str, str]:
    """
    Renders the sidebar and returns (f_email, f_binder, f_company, f_license, f_status).
    """
    with st.sidebar:
        # Brand strip
        st.markdown(f"""
        <div style="border-top:3px solid var(--gold);padding:18px 16px 14px;">
          <div style="font-size:1.05rem;font-weight:700;letter-spacing:-.01em;
                      color:var(--text-primary);margin-bottom:2px;">
            🏛️&nbsp; {t('portal_title')}
          </div>
          <div style="font-size:0.64rem;color:var(--text-muted);
                      letter-spacing:.12em;text-transform:uppercase;">
            {t('ministry')}
          </div>
        </div>
        <hr style="margin:0;border-color:var(--border);"/>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Language
        st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"):
            st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang = "ku"; st.rerun()

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Theme
        st.markdown(f"<div class='sb-label'>{t('theme')}</div>", unsafe_allow_html=True)
        tc1, tc2 = st.columns(2)
        if tc1.button("☀️  Light", use_container_width=True, key="sb_lgt"):
            st.session_state.theme = "light"; st.rerun()
        if tc2.button("🌙  Dark",  use_container_width=True, key="sb_drk"):
            st.session_state.theme = "dark";  st.rerun()

        st.markdown("<hr style='border-color:var(--border);margin:14px 0;'/>",
                    unsafe_allow_html=True)

        # ── ADVANCED FILTER PANEL ────────────────────────────────────────────
        st.markdown(f"""
        <div class="sb-filter-title">🔍 {t('filters')}</div>
        """, unsafe_allow_html=True)

        # Status filter
        status_opts = {
            "all"    : t("status_all"),
            "pending": t("status_pending"),
            "done"   : t("status_done"),
        }
        f_status = st.selectbox(
            t("filter_status"),
            options=list(status_opts.keys()),
            format_func=lambda k: status_opts[k],
            key="f_status",
        )

        # Auditor email
        f_email = st.text_input(
            t("filter_email"),
            placeholder="e.g. auditor@",
            key="f_email",
        )

        # Binder number
        binder_label = f"{t('filter_binder')}" + (f"  [{col_binder}]" if col_binder else " (not detected)")
        f_binder = st.text_input(
            binder_label,
            placeholder="e.g. 12345",
            key="f_binder",
            disabled=(col_binder is None),
        )

        # Company name
        company_label = f"{t('filter_company')}" + (f"  [{col_company}]" if col_company else " (not detected)")
        f_company = st.text_input(
            company_label,
            placeholder="e.g. Al-Rasheed",
            key="f_company",
            disabled=(col_company is None),
        )

        # License number
        license_label = f"{t('filter_license')}" + (f"  [{col_license}]" if col_license else " (not detected)")
        f_license = st.text_input(
            license_label,
            placeholder="e.g. LIC-2024",
            key="f_license",
            disabled=(col_license is None),
        )

        # Clear all filters button
        if st.button(f"✕  {t('clear_filters')}", use_container_width=True, key="clear_f"):
            for key in ["f_email", "f_binder", "f_company", "f_license"]:
                if key in st.session_state:
                    st.session_state[key] = ""
            st.session_state["f_status"] = "all"
            st.rerun()

        st.markdown("<hr style='border-color:var(--border);margin:14px 0;'/>",
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

    return (
        st.session_state.get("f_email", ""),
        st.session_state.get("f_binder", ""),
        st.session_state.get("f_company", ""),
        st.session_state.get("f_license", ""),
        st.session_state.get("f_status", "all"),
    )

# ─────────────────────────────────────────────────────────────────────────────
#  15 · FILTER STATUS BAR
# ─────────────────────────────────────────────────────────────────────────────
def render_filter_bar(
    total: int,
    filtered: int,
    f_email: str,
    f_binder: str,
    f_company: str,
    f_license: str,
    f_status: str,
) -> None:
    n_active = active_filter_count(f_email, f_binder, f_company, f_license, f_status)
    if n_active == 0:
        return

    badges = ""
    if f_status != "all":
        badges += f"<span class='filter-badge'>⚡ Status: {f_status}</span>"
    if f_email.strip():
        badges += f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span>"
    if f_binder.strip():
        badges += f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span>"
    if f_company.strip():
        badges += f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span>"
    if f_license.strip():
        badges += f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span>"

    st.markdown(f"""
    <div class="filter-panel">
      <div class="filter-title">
        🔍 {t('active_filters')} ({n_active})
      </div>
      {badges}
      <div class="result-count" style="margin-top:8px;">
        Showing <strong style="color:var(--gold);">{filtered}</strong> of
        <strong>{total}</strong> {t('results_shown')}
      </div>
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  16 · ANALYTICS TAB
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df: pd.DataFrame) -> None:
    pt = P["plotly_theme"]
    pb = P["plot_bg"]
    pg = P["plot_grid"]
    fc = P["text_primary"]

    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>", unsafe_allow_html=True)
    periods = [
        ("all", t("all_time")), ("today", t("today")),
        ("this_week", t("this_week")), ("this_month", t("this_month")),
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

    ma, mb, mc = st.columns(3)
    ma.metric(t("records_period"), len(done_f))

    active = 0
    if COL_DATE in done_f.columns:
        active = done_f[COL_DATE].apply(
            lambda s: parse_dt(s).date() if parse_dt(s) else None
        ).nunique()
    mb.metric(t("active_days"), active)
    mc.metric(t("avg_per_day"), f"{len(done_f)/max(active,1):.1f}")

    left, right = st.columns([1, 1.6], gap="large")

    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>",
                    unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = (
                done_f[COL_AUDITOR].replace("", "—")
                .value_counts().reset_index()
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

            fig_lb = px.bar(
                lb.head(10), x="Count", y="Auditor", orientation="h",
                color="Count",
                color_continuous_scale=[P["blue_accent"], P["gold"]],
                template=pt,
            )
            fig_lb.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="IBM Plex Sans", color=fc, size=11),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=8, r=8, t=10, b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color=fc)),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color=fc)),
                height=min(320, max(180, 36 * len(lb.head(10)))),
            )
            fig_lb.update_traces(marker_line_width=0)
            st.plotly_chart(fig_lb, use_container_width=True)

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
                .groupby("_date").size()
                .reset_index(name="Records")
            )
            trend.columns = ["Date", "Records"]
            if not trend.empty:
                if len(trend) > 1:
                    full_rng = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = (
                        trend.set_index("Date")
                        .reindex(full_rng.date, fill_value=0)
                        .reset_index()
                    )
                    trend.columns = ["Date", "Records"]

                fig_line = go.Figure()
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="none", fill="tozeroy",
                    fillcolor=P["gold_bg"], showlegend=False,
                ))
                fig_line.add_trace(go.Scatter(
                    x=trend["Date"], y=trend["Records"],
                    mode="lines+markers",
                    line=dict(color=P["gold"], width=2.5),
                    marker=dict(color=P["blue_accent"], size=7,
                                line=dict(color=P["card"], width=2)),
                    name=t("records_period"),
                ))
                fig_line.update_layout(
                    template=pt, paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="IBM Plex Sans", color=fc, size=11),
                    showlegend=False, margin=dict(l=8, r=8, t=10, b=8),
                    xaxis=dict(gridcolor=pg, zeroline=False,
                               tickfont=dict(color=P["text_secondary"])),
                    yaxis=dict(gridcolor=pg, zeroline=False,
                               tickfont=dict(color=P["text_secondary"])),
                    height=380, hovermode="x unified",
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info(t("no_records"))

# ─────────────────────────────────────────────────────────────────────────────
#  17 · USER ADMINISTRATION TAB
# ─────────────────────────────────────────────────────────────────────────────
def render_user_admin(users_ws) -> None:
    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>",
                    unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_email = st.text_input("Email", placeholder="auditor@mof.gov")
            nu_pass  = st.text_input("Password", type="password")
            if st.form_submit_button("Register Auditor", use_container_width=True):
                if nu_email.strip() and nu_pass.strip():
                    recs = pd.DataFrame(users_ws.get_all_records())
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

    with col_right:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>",
                    unsafe_allow_html=True)
        staff_df = pd.DataFrame(users_ws.get_all_records())
        if not staff_df.empty and "email" in staff_df.columns:
            safe_cols = [c for c in ["email", "created_at"] if c in staff_df.columns]
            render_html_table(staff_df[safe_cols].reset_index())

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
#  18 · MAIN APPLICATION CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        spreadsheet   = get_spreadsheet()
        all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]

        if "UsersDB" not in all_ws_titles:
            users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="500", cols="3")
            users_ws.append_row(["email", "password", "created_at"])
        else:
            users_ws = spreadsheet.worksheet("UsersDB")

        # ── AUTH GATE ────────────────────────────────────────────────────────
        if not st.session_state.logged_in:
            render_login(users_ws)
            return

        # ── AUTHENTICATED SHELL ──────────────────────────────────────────────
        st.markdown(
            "<style>[data-testid='stSidebar']{display:flex!important;}</style>",
            unsafe_allow_html=True,
        )

        # Page header
        ts = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-title">🏛️  {t('portal_title')}</div>
        <div class="page-sub">{ts}</div>
        """, unsafe_allow_html=True)

        # Workspace selector
        data_sheets = [n for n in all_ws_titles if n != "UsersDB"]
        ws_name = st.selectbox(
            t("workspace"), data_sheets, key="ws_sel", label_visibility="visible"
        )
        current_ws           = spreadsheet.worksheet(ws_name)
        df, headers, col_map = load_worksheet(current_ws)

        if df.empty:
            st.warning(t("empty_sheet"))
            return

        # ── Detect filterable columns ────────────────────────────────────────
        col_binder  = detect_column(headers, "binder")
        col_company = detect_column(headers, "company")
        col_license = detect_column(headers, "license")

        is_admin = (st.session_state.user_role == "admin")

        # ── SIDEBAR + FILTERS ────────────────────────────────────────────────
        f_email, f_binder, f_company, f_license, f_status = render_sidebar(
            df, headers, col_binder, col_company, col_license
        )

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
          <span>{t('processed')}</span><span>{int(pct*100)}%</span>
        </div>
        <div class="gov-progress-wrap">
          <div class="gov-progress-fill" style="width:{int(pct*100)}%;"></div>
        </div>
        """, unsafe_allow_html=True)

        # ── Apply all filters to full df ─────────────────────────────────────
        filtered_df = apply_filters(
            df, f_email, f_binder, f_company, f_license, f_status,
            col_binder, col_company, col_license,
        )

        # Show filter bar (only if filters active)
        render_filter_bar(
            total_n, len(filtered_df),
            f_email, f_binder, f_company, f_license, f_status,
        )

        # ── TABS ─────────────────────────────────────────────────────────────
        tab_names = [t("tab_queue"), t("tab_archive"), t("tab_analytics")]
        if is_admin:
            tab_names.append(t("tab_users"))
        tabs = st.tabs(tab_names)

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 1 — AUDIT WORKLIST
        # ══════════════════════════════════════════════════════════════════════
        with tabs[0]:
            # Pending records from filtered set
            pending_view = filtered_df[filtered_df[COL_STATUS] != VAL_DONE].copy()

            # ── CRITICAL: re-align index to Google Sheets row numbers ─────────
            # The original df has 0-based index matching sheet rows (header=row1).
            # Sheet row = df_index + 2 (1 for header, 1 for 0-vs-1 indexing).
            # We must preserve the original integer index from df — not reset it —
            # so row selection maps back correctly.
            pending_display = pending_view.copy()
            pending_display.index = pending_display.index + 2  # sheet row numbers

            # ── Worklist header banner ────────────────────────────────────────
            p_count = len(pending_display)
            st.markdown(f"""
            <div class="worklist-header">
              <div>
                <div class="worklist-title">📋 {t('worklist_title')}</div>
                <div class="worklist-sub">{t('worklist_sub')}</div>
              </div>
              <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
            </div>
            """, unsafe_allow_html=True)

            if pending_display.empty:
                if active_filter_count(f_email, f_binder, f_company, f_license, f_status) > 0:
                    st.warning(t("no_filter_match"))
                else:
                    st.success("✅  All cases have been processed.")
            else:
                # ── HTML table — guaranteed text visibility ────────────────────
                render_html_table(pending_display)

                st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>",
                            unsafe_allow_html=True)

                label_col = next(
                    (h for h in headers if h not in SYSTEM_COLS),
                    headers[0] if headers else "Row"
                )
                opts = ["—"] + [
                    f"Row {idx}  ·  {str(row.get(label_col, ''))[:55]}"
                    for idx, row in pending_display.iterrows()
                ]
                row_sel = st.selectbox(
                    "", opts, key="row_sel", label_visibility="collapsed"
                )

                if row_sel != "—":
                    sheet_row = int(row_sel.split("  ·  ")[0].replace("Row ", "").strip())
                    # Retrieve record from original df using (sheet_row - 2)
                    record = df.iloc[sheet_row - 2].to_dict()

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

                    st.markdown(
                        f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>",
                        unsafe_allow_html=True,
                    )
                    SKIP   = set(SYSTEM_COLS)
                    fields = {k: v for k, v in record.items() if k not in SKIP}

                    with st.form("audit_form"):
                        new_vals: dict[str, str] = {}
                        for fname, fval in fields.items():
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
                            headers, col_map = ensure_system_cols(
                                current_ws, headers, col_map
                            )
                            for fname, fval in new_vals.items():
                                if fname in col_map and clean_cell(record.get(fname, "")) != fval:
                                    current_ws.update_cell(sheet_row, col_map[fname], fval)

                            ts_now  = now_str()
                            auditor = st.session_state.user_email
                            old_log = str(record.get(COL_LOG, "")).strip()
                            new_log = f"✔  {auditor}  |  {ts_now}\n{old_log}".strip()

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
            done_view = filtered_df[filtered_df[COL_STATUS] == VAL_DONE].copy()
            done_view.index = done_view.index + 2

            # ── Worklist header banner ────────────────────────────────────────
            d_count = len(done_view)
            st.markdown(f"""
            <div class="worklist-header">
              <div>
                <div class="worklist-title">✅ {t('tab_archive')}</div>
                <div class="worklist-sub">Completed and committed audit records</div>
              </div>
              <span class="chip chip-done">{d_count} {t('processed')}</span>
            </div>
            """, unsafe_allow_html=True)

            if done_view.empty:
                if active_filter_count(f_email, f_binder, f_company, f_license, f_status) > 0:
                    st.warning(t("no_filter_match"))
                else:
                    st.info("No processed records yet.")
            else:
                render_html_table(done_view)

            # Admin: re-open
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
        #  TAB 4 — USER ADMIN
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
