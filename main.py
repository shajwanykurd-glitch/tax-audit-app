# =============================================================================
#  OFFICIAL TAX AUDIT PORTAL  ·  v6.0
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
    user_role   = "",
    theme       = "dark",
    lang        = "en",
    date_filter = "all",
)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─────────────────────────────────────────────────────────────────────────────
#  2 · THEME PALETTES
#     Dark:  deep navy #0E1117 backbone, crisp off-white #E6EDF3 text
#     Light: pure white #FFFFFF backbone, deep charcoal #1F2328 text
# ─────────────────────────────────────────────────────────────────────────────
_PALETTES: dict = {
    "dark": {
        "page_bg"      : "#0E1117",
        "surface"      : "#131720",
        "surface2"     : "#181E2C",
        "card"         : "#141B27",
        "card2"        : "#192030",
        "border"       : "#1E2D45",
        "border2"      : "#263A58",
        "text_primary" : "#E6EDF3",
        "text_secondary": "#8BA8CC",
        "text_muted"   : "#4A6585",
        "gold"         : "#CBA84C",
        "gold_light"   : "#E6C97E",
        "gold_bg"      : "rgba(203,168,76,0.10)",
        "blue_accent"  : "#3878D8",
        "blue_bg"      : "rgba(56,120,216,0.13)",
        "green"        : "#28A878",
        "green_bg"     : "rgba(40,168,120,0.13)",
        "amber"        : "#E09820",
        "amber_bg"     : "rgba(224,152,32,0.13)",
        "red"          : "#D94F4F",
        "red_bg"       : "rgba(217,79,79,0.13)",
        "input_bg"     : "#0A0E16",
        "input_border" : "#1E2D45",
        "input_focus"  : "#CBA84C",
        "btn_primary"  : "#CBA84C",
        "btn_text"     : "#0E1117",
        "prog_track"   : "#1E2D45",
        "prog_fill_a"  : "#CBA84C",
        "prog_fill_b"  : "#3878D8",
        "plotly_theme" : "plotly_dark",
        "plot_bg"      : "#131720",
        "plot_grid"    : "#1E2D45",
        "tbl_bg"       : "#141B27",
        "tbl_header_bg": "#181E2C",
        "tbl_text"     : "#E6EDF3",
        "tbl_hdr_txt"  : "#8BA8CC",
        "tbl_border"   : "#1E2D45",
        "tbl_hover"    : "#1A2438",
        "metric_rgba"  : "rgba(20,27,39,0.90)",
    },
    "light": {
        "page_bg"      : "#FFFFFF",
        "surface"      : "#F5F7FB",
        "surface2"     : "#EBF0F9",
        "card"         : "#FFFFFF",
        "card2"        : "#F7F9FD",
        "border"       : "#C4D3EC",
        "border2"      : "#A8C0E0",
        "text_primary" : "#1F2328",
        "text_secondary": "#2D4A70",
        "text_muted"   : "#6580A0",
        "gold"         : "#9A7020",
        "gold_light"   : "#B88A30",
        "gold_bg"      : "rgba(154,112,32,0.09)",
        "blue_accent"  : "#1658B8",
        "blue_bg"      : "rgba(22,88,184,0.09)",
        "green"        : "#157A50",
        "green_bg"     : "rgba(21,122,80,0.09)",
        "amber"        : "#A85808",
        "amber_bg"     : "rgba(168,88,8,0.09)",
        "red"          : "#A82828",
        "red_bg"       : "rgba(168,40,40,0.09)",
        "input_bg"     : "#FFFFFF",
        "input_border" : "#B8CCE8",
        "input_focus"  : "#1658B8",
        "btn_primary"  : "#0D2A58",
        "btn_text"     : "#FFFFFF",
        "prog_track"   : "#D0E0F5",
        "prog_fill_a"  : "#9A7020",
        "prog_fill_b"  : "#1658B8",
        "plotly_theme" : "plotly_white",
        "plot_bg"      : "#FFFFFF",
        "plot_grid"    : "#D0E0F5",
        "tbl_bg"       : "#FFFFFF",
        "tbl_header_bg": "#EBF0F9",
        "tbl_text"     : "#1F2328",
        "tbl_hdr_txt"  : "#2D4A70",
        "tbl_border"   : "#C4D3EC",
        "tbl_hover"    : "#EDF3FF",
        "metric_rgba"  : "rgba(255,255,255,0.95)",
    },
}

P = _PALETTES[st.session_state.theme]

# ─────────────────────────────────────────────────────────────────────────────
#  3 · FORCED-CONTRAST CSS INJECTION
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
  --input-focus    : {P['input_focus']};
  --btn-primary    : {P['btn_primary']};
  --btn-text       : {P['btn_text']};
  --prog-track     : {P['prog_track']};
  --tbl-bg         : {P['tbl_bg']};
  --tbl-header-bg  : {P['tbl_header_bg']};
  --tbl-text       : {P['tbl_text']};
  --tbl-hdr-txt    : {P['tbl_hdr_txt']};
  --tbl-border     : {P['tbl_border']};
  --tbl-hover      : {P['tbl_hover']};
}}

/* ══ FORCED GLOBAL RESET ══════════════════════════════════════════════════ */
*, *::before, *::after {{
  box-sizing  : border-box !important;
  font-family : 'IBM Plex Sans', sans-serif !important;
}}
html, body,
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main, .block-container {{
  background-color : {P['page_bg']} !important;
  color            : {P['text_primary']} !important;
}}
/* Force ALL text-bearing elements — covers markdown, widgets, tables */
p, span, div, li, label,
h1, h2, h3, h4, h5, h6,
.stMarkdown, .stText,
[data-testid="stMarkdownContainer"] {{
  color: {P['text_primary']} !important;
}}
#MainMenu, footer, header,
.stDeployButton, [data-testid="stToolbar"] {{
  display: none !important;
}}

/* ══ SIDEBAR ══════════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {{
  background-color : {P['surface']} !important;
  border-right     : 1px solid {P['border']} !important;
}}
[data-testid="stSidebar"] * {{
  color : {P['text_primary']} !important;
}}
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"] {{ display: none !important; }}

/* ══ INPUTS — Forced Contrast Engine ══════════════════════════════════════ */
.stTextInput > div > div > input {{
  background-color : {P['input_bg']} !important;
  color            : {P['text_primary']} !important;
  border           : 1.5px solid {P['input_border']} !important;
  border-radius    : 7px !important;
  font-size        : 0.875rem !important;
  caret-color      : {P['gold']} !important;
  padding          : 9px 12px !important;
  transition       : border-color 0.2s ease, box-shadow 0.2s ease !important;
}}
.stTextInput > div > div > input:focus {{
  border-color : {P['input_focus']} !important;
  box-shadow   : 0 0 0 3px {P['gold_bg']},
                 inset 0 0 0 1px {P['input_focus']} !important;
  outline      : none !important;
  color        : {P['text_primary']} !important;
}}
.stTextInput > div > div > input::placeholder {{
  color: {P['text_muted']} !important; opacity: 0.75 !important;
}}
/* Sidebar inputs — extra specificity to override Streamlit theme */
[data-testid="stSidebar"] .stTextInput > div > div > input {{
  background-color : {P['input_bg']} !important;
  color            : {P['text_primary']} !important;
  border           : 1.5px solid {P['input_border']} !important;
}}
[data-testid="stSidebar"] .stTextInput > div > div > input:focus {{
  border-color : {P['input_focus']} !important;
  box-shadow   : 0 0 0 3px {P['gold_bg']} !important;
  color        : {P['text_primary']} !important;
}}
[data-testid="stSidebar"] .stTextInput > div > div > input::placeholder {{
  color: {P['text_muted']} !important; opacity: 0.8 !important;
}}
[data-testid="stSidebar"] .stTextInput > div > div > input:disabled {{
  opacity: 0.38 !important;
  cursor : not-allowed !important;
  color  : {P['text_muted']} !important;
}}

/* Selectboxes */
.stSelectbox > div > div,
.stSelectbox > div > div > div,
[data-baseweb="select"] > div {{
  background-color : {P['input_bg']} !important;
  color            : {P['text_primary']} !important;
  border-color     : {P['input_border']} !important;
}}
[data-baseweb="select"]:focus-within > div {{
  border-color : {P['input_focus']} !important;
  box-shadow   : 0 0 0 3px {P['gold_bg']} !important;
}}
[data-baseweb="menu"] li,
[data-baseweb="menu"] [role="option"] {{
  background-color : {P['surface']} !important;
  color            : {P['text_primary']} !important;
}}
[data-baseweb="menu"] li:hover,
[data-baseweb="menu"] [aria-selected="true"] {{
  background-color : {P['surface2']} !important;
  color            : {P['gold']} !important;
}}

/* Input / select labels */
.stTextInput > label,
.stTextArea > label,
.stSelectbox > label,
.stMultiSelect > label {{
  color: {P['text_muted']} !important;
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.10em !important;
  text-transform: uppercase !important;
}}

/* ══ METRIC CARDS — semi-transparent rgba ═══════════════════════════════ */
[data-testid="stMetricContainer"] {{
  background      : {P['metric_rgba']} !important;
  border          : 1px solid {P['border']} !important;
  border-radius   : 12px !important;
  padding         : 18px 22px !important;
  backdrop-filter : blur(8px) !important;
  box-shadow      : 0 2px 12px rgba(0,0,0,0.14) !important;
  transition      : transform 0.22s ease, box-shadow 0.22s ease !important;
}}
[data-testid="stMetricContainer"]:hover {{
  transform  : translateY(-4px) !important;
  box-shadow : 0 10px 28px rgba(0,0,0,0.22), 0 0 0 1px {P['gold']} !important;
}}
[data-testid="stMetricValue"] {{
  font-family : 'IBM Plex Mono', monospace !important;
  font-size   : 2.1rem !important;
  font-weight : 600 !important;
  color       : {P['gold']} !important;
}}
[data-testid="stMetricLabel"] {{
  font-size: 0.68rem !important; font-weight: 700 !important;
  letter-spacing: 0.12em !important; text-transform: uppercase !important;
  color: {P['text_muted']} !important;
}}

/* ══ BUTTONS ══════════════════════════════════════════════════════════════ */
.stButton > button {{
  background-color : {P['btn_primary']} !important;
  color            : {P['btn_text']}    !important;
  border           : none !important;
  border-radius    : 7px !important;
  font-weight      : 600 !important;
  font-size        : 0.84rem !important;
  padding          : 9px 18px !important;
  transition       : opacity 0.15s ease, transform 0.15s ease,
                     box-shadow 0.15s ease !important;
}}
.stButton > button:hover {{
  opacity    : 0.88 !important;
  transform  : translateY(-2px) !important;
  box-shadow : 0 6px 16px rgba(0,0,0,0.25) !important;
}}
.stButton > button:active {{ transform: translateY(0) !important; }}

/* ══ FORMS ════════════════════════════════════════════════════════════════ */
div[data-testid="stForm"] {{
  background-color : {P['card']} !important;
  border           : 1px solid {P['border']} !important;
  border-radius    : 14px !important;
  padding          : 24px 28px !important;
}}

/* ══ TABS ═════════════════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {{
  gap: 2px !important; background: transparent !important;
  border-bottom: 2px solid {P['border']} !important;
}}
.stTabs [data-baseweb="tab"] {{
  background: transparent !important;
  color: {P['text_muted']} !important;
  border-radius: 8px 8px 0 0 !important;
  border: 1px solid transparent !important;
  border-bottom: none !important;
  padding: 10px 20px !important;
  font-weight: 600 !important; font-size: 0.82rem !important;
}}
.stTabs [data-baseweb="tab"]:hover {{ color: {P['text_primary']} !important; }}
.stTabs [aria-selected="true"] {{
  background-color    : {P['card']} !important;
  color               : {P['gold']} !important;
  border-color        : {P['border']} !important;
  border-bottom-color : {P['card']} !important;
  margin-bottom       : -2px !important;
}}

/* ══ HTML TABLE — GUARANTEED TEXT VISIBILITY ══════════════════════════════
   Rendered with st.markdown() so every rule here applies perfectly.
   Each colour is hardcoded (not via CSS var) to defeat Streamlit's engine. */
.gov-table-wrap {{
  overflow-x    : auto;
  border        : 1px solid {P['tbl_border']};
  border-radius : 12px;
  margin-bottom : 16px;
}}
.gov-table {{
  width: 100%; border-collapse: collapse;
  background-color: {P['tbl_bg']};
  font-size: 0.82rem;
}}
.gov-table thead tr {{
  background-color: {P['tbl_header_bg']};
  border-bottom: 2px solid {P['border2']};
}}
.gov-table th {{
  color            : {P['tbl_hdr_txt']} !important;
  background-color : {P['tbl_header_bg']} !important;
  font-weight      : 700 !important;
  font-size        : 0.65rem !important;
  letter-spacing   : 0.10em !important;
  text-transform   : uppercase !important;
  padding          : 11px 14px !important;
  white-space      : nowrap;
  text-align       : left !important;
  border-right     : 1px solid {P['tbl_border']};
}}
.gov-table th:last-child {{ border-right: none; }}
.gov-table td {{
  color            : {P['tbl_text']} !important;
  background-color : {P['tbl_bg']} !important;
  padding          : 9px 14px !important;
  font-size        : 0.82rem !important;
  border-bottom    : 1px solid {P['tbl_border']} !important;
  border-right     : 1px solid {P['tbl_border']} !important;
  vertical-align   : middle !important;
  max-width        : 220px;
  overflow         : hidden;
  text-overflow    : ellipsis;
  white-space      : nowrap;
}}
.gov-table td:last-child {{ border-right: none; }}
.gov-table tbody tr:hover td {{
  background-color : {P['tbl_hover']} !important;
  color            : {P['tbl_text']} !important;
}}
.gov-table tbody tr:last-child td {{ border-bottom: none !important; }}
.gov-table td.row-idx, .gov-table th.row-idx {{
  color       : {P['text_muted']} !important;
  font-family : 'IBM Plex Mono', monospace !important;
  font-size   : 0.70rem !important;
  min-width   : 50px; text-align: center !important;
}}
.s-chip {{
  display: inline-flex; align-items: center;
  padding: 2px 9px; border-radius: 99px;
  font-size: 0.62rem; font-weight: 700;
  letter-spacing: 0.07em; text-transform: uppercase; white-space: nowrap;
}}
.s-done    {{ background:{P['green_bg']}; color:{P['green']} !important; }}
.s-pending {{ background:{P['amber_bg']}; color:{P['amber']} !important; }}

/* ══ EXPANDER ════════════════════════════════════════════════════════════ */
.streamlit-expanderHeader {{
  background-color: {P['surface2']} !important;
  color: {P['text_primary']} !important;
  border: 1px solid {P['border']} !important;
  border-radius: 8px !important;
  font-weight: 600 !important; font-size: 0.84rem !important;
}}
.streamlit-expanderContent {{
  background-color: {P['card']} !important;
  border: 1px solid {P['border']} !important;
  border-top: none !important;
  border-radius: 0 0 8px 8px !important; padding: 14px !important;
}}

/* ══ ALERTS ══════════════════════════════════════════════════════════════ */
[data-testid="stAlert"] {{
  border-radius: 9px !important; border: 1px solid {P['border']} !important;
  background-color: {P['surface2']} !important;
}}
[data-testid="stAlert"] * {{ color: {P['text_primary']} !important; }}

/* ══ LOGIN CARD — Official Governmental ══════════════════════════════════ */
.gov-login-card {{
  width: 100%; max-width: 440px;
  background-color: {P['card']} !important;
  border: 1.5px solid {P['border2']};
  border-top: 5px solid {P['gold']};
  border-radius: 16px; padding: 44px 42px 36px;
  box-shadow: 0 28px 64px rgba(0,0,0,0.28), 0 0 0 1px {P['border']};
}}
.gov-seal {{
  font-size: 3.4rem; text-align: center; margin-bottom: 8px;
  filter: drop-shadow(0 3px 10px rgba(203,168,76,0.38));
}}
.gov-ministry {{
  font-size: 0.64rem; font-weight: 700; letter-spacing: 0.20em;
  text-transform: uppercase; text-align: center;
  color: {P['text_muted']} !important; margin-bottom: 5px;
}}
.gov-portal-title {{
  font-size: 1.38rem; font-weight: 700; text-align: center;
  color: {P['text_primary']} !important; letter-spacing: -0.02em; margin-bottom: 4px;
}}
.gov-portal-sub {{
  font-size: 0.76rem; text-align: center;
  color: {P['text_muted']} !important; margin-bottom: 22px;
}}
.gold-divider {{
  width: 44px; height: 3px; background: {P['gold']};
  border-radius: 99px; margin: 8px auto 20px;
}}
/* Login button — high-visibility gold with strong hover */
.gov-login-card .stButton > button {{
  background-color : {P['gold']} !important;
  color            : {P['page_bg']} !important;
  font-weight      : 700 !important; font-size: 0.94rem !important;
  border-radius    : 9px !important; padding: 12px !important;
  width            : 100% !important;
  border           : 2px solid transparent !important;
  transition       : background-color 0.18s ease, border-color 0.18s ease,
                     transform 0.15s ease, box-shadow 0.18s ease !important;
}}
.gov-login-card .stButton > button:hover {{
  background-color : {P['gold_light']} !important;
  border-color     : {P['gold']} !important;
  box-shadow       : 0 6px 20px rgba(203,168,76,0.38) !important;
  transform        : translateY(-2px) !important;
}}

/* ══ PAGE / SECTION HEADERS ══════════════════════════════════════════════ */
.page-title {{
  font-size: 1.5rem; font-weight: 700; color: {P['text_primary']} !important;
  letter-spacing: -0.02em; margin-bottom: 2px;
}}
.page-sub {{ font-size: 0.80rem; color: {P['text_muted']} !important; margin-bottom: 18px; }}
.section-title {{
  display: flex; align-items: center; gap: 9px;
  font-size: 0.86rem; font-weight: 700; color: {P['text_primary']} !important;
  margin: 20px 0 11px; padding-left: 11px; border-left: 3px solid {P['gold']};
}}
.worklist-header {{
  display: flex; align-items: center; justify-content: space-between;
  background-color: {P['surface2']} !important;
  border: 1px solid {P['border']}; border-top: 3px solid {P['blue_accent']};
  border-radius: 10px; padding: 13px 18px; margin-bottom: 14px;
}}
.worklist-title {{
  font-size: 0.94rem; font-weight: 700; color: {P['text_primary']} !important;
}}
.worklist-sub {{ font-size: 0.72rem; color: {P['text_muted']} !important; margin-top: 2px; }}

/* ══ PROGRESS ═════════════════════════════════════════════════════════════ */
.gov-progress-wrap {{
  background-color: {P['prog_track']}; border-radius: 99px;
  height: 6px; overflow: hidden; margin: 5px 0 9px;
}}
.gov-progress-fill {{
  height: 100%; border-radius: 99px;
  background: linear-gradient(90deg, {P['prog_fill_a']}, {P['prog_fill_b']});
  transition: width 0.7s cubic-bezier(0.4,0,0.2,1);
}}
.prog-labels {{
  display: flex; justify-content: space-between;
  font-size: 0.70rem; color: {P['text_muted']} !important;
  font-weight: 600; margin-bottom: 3px;
}}

/* ══ CHIPS ════════════════════════════════════════════════════════════════ */
.chip {{
  display: inline-flex; align-items: center; gap: 4px;
  padding: 3px 10px; border-radius: 99px;
  font-size: 0.68rem; font-weight: 700;
  letter-spacing: 0.07em; text-transform: uppercase;
}}
.chip-done    {{ background:{P['green_bg']}; color:{P['green']} !important; }}
.chip-pending {{ background:{P['amber_bg']}; color:{P['amber']} !important; }}
.chip-admin   {{ background:{P['gold_bg']};  color:{P['gold']} !important; }}
.chip-audit   {{ background:{P['blue_bg']};  color:{P['blue_accent']} !important; }}

/* ══ SIDEBAR LABELS & BADGE ═══════════════════════════════════════════════ */
.sb-label {{
  font-size: 0.60rem; font-weight: 700; letter-spacing: 0.13em;
  text-transform: uppercase; color: {P['text_muted']} !important; margin-bottom: 4px;
}}
.sb-email {{
  font-size: 0.85rem; font-weight: 700;
  color: {P['text_primary']} !important; word-break: break-all;
}}
.sb-user-badge {{
  background-color: {P['surface2']} !important;
  border: 1px solid {P['border']}; border-radius: 9px;
  padding: 11px 13px; margin-bottom: 10px;
}}
/* ── Advanced Filter Panel header in sidebar ── */
.adv-filter-header {{
  font-size: 0.64rem; font-weight: 700; letter-spacing: 0.15em;
  text-transform: uppercase; color: {P['gold']} !important;
  margin-bottom: 12px; padding-bottom: 8px;
  border-bottom: 1px solid {P['border']};
}}
.col-hint {{
  font-size: 0.60rem; font-weight: 400; opacity: 0.55;
  color: {P['text_muted']} !important;
}}

/* ══ FILTER RESULT BAR ════════════════════════════════════════════════════ */
.filter-result-bar {{
  background-color: {P['card']} !important;
  border: 1px solid {P['border']}; border-left: 3px solid {P['gold']};
  border-radius: 9px; padding: 12px 16px; margin-bottom: 14px;
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
}}
.filter-badge {{
  display: inline-flex; align-items: center; gap: 4px;
  background: {P['blue_bg']}; color: {P['blue_accent']} !important;
  border: 1px solid {P['blue_accent']}; border-radius: 99px;
  font-size: 0.66rem; font-weight: 700; padding: 2px 9px;
}}
.result-count {{
  font-family: 'IBM Plex Mono', monospace; font-size: 0.78rem;
  color: {P['text_muted']} !important; margin-left: auto;
}}

/* ══ LEADERBOARD ══════════════════════════════════════════════════════════ */
.lb-row {{
  display: flex; align-items: center; gap: 11px;
  padding: 10px 14px; background-color: {P['card2']} !important;
  border: 1px solid {P['border']}; border-radius: 9px; margin-bottom: 6px;
  transition: transform 0.16s ease, border-color 0.16s ease;
}}
.lb-row:hover {{ transform: translateX(4px); border-color: {P['gold']}; }}
.lb-medal {{ font-size: 1.1rem; width: 24px; text-align: center; }}
.lb-name  {{ flex: 1; font-size: 0.84rem; font-weight: 600; color: {P['text_primary']} !important; }}
.lb-count {{
  font-family: 'IBM Plex Mono', monospace; font-size: 0.92rem;
  font-weight: 700; color: {P['gold']} !important;
}}

/* ══ AUDIT LOG ════════════════════════════════════════════════════════════ */
.log-line {{
  font-family: 'IBM Plex Mono', monospace; font-size: 0.74rem;
  color: {P['text_secondary']} !important;
  padding: 3px 0; border-bottom: 1px solid {P['border']};
}}
.log-line:last-child {{ border-bottom: none; }}
</style>
""", unsafe_allow_html=True)


inject_css(P)

# ─────────────────────────────────────────────────────────────────────────────
#  4 · TRANSLATIONS
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
        "worklist_title": "Audit Worklist",
        "worklist_sub"  : "Active cases pending review and approval",
        "tab_queue"     : "📋  Audit Worklist",
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
        "adv_filters"   : "🔍 Advanced Filters",
        "f_email"       : "Auditor Email",
        "f_binder"      : "Company Binder No.",
        "f_company"     : "Company Name",
        "f_license"     : "License Number",
        "f_status"      : "Status",
        "clear_filters" : "Clear All Filters",
        "active_filters": "Active filters",
        "results_shown" : "results shown",
        "no_match"      : "No records match the applied filters.",
        "status_all"    : "All Statuses",
        "status_pending": "Pending Only",
        "status_done"   : "Processed Only",
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
        "worklist_title": "لیستی کاری وردبینی",
        "worklist_sub"  : "کیسە چالاکەکان کە چاوەڕوانی پشکنین و پەسەندکردنن",
        "tab_queue"     : "📋  لیستی کاری وردبینی",
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
        "adv_filters"   : "🔍 فلتەرە پێشکەوتووەکان",
        "f_email"       : "ئیمەیڵی ئۆدیتۆر",
        "f_binder"      : "ژمارەی بایندەری کۆمپانیا",
        "f_company"     : "ناوی کۆمپانیا",
        "f_license"     : "ژمارەی مۆڵەتی کۆمپانیا",
        "f_status"      : "دەربار",
        "clear_filters" : "سڕینەوەی هەموو فلتەرەکان",
        "active_filters": "فلتەرە چالاکەکان",
        "results_shown" : "ئەنجامی پیشاندراو",
        "no_match"      : "هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "status_all"    : "هەموو دەرباریەکان",
        "status_pending": "چاوەڕوان تەنها",
        "status_done"   : "کارکراو تەنها",
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
#  6 · ROBUST MULTI-LANGUAGE COLUMN DETECTION
#
#  Each keyword list encodes the EXACT Arabic and Kurdish phrases specified
#  by the client, plus common transliterations and English fallbacks.
#  detect_column() uses case-insensitive substring matching so it handles
#  any surrounding whitespace or minor spelling variants.
# ─────────────────────────────────────────────────────────────────────────────
_COL_KEYWORDS: dict[str, list[str]] = {
    # Client spec:  "رقم ملف الشركة"  |  "ژمارەی بایندەری کۆمپانیا"
    "binder": [
        "رقم ملف الشركة",       # exact Arabic phrase
        "رقم_ملف_الشركة",
        "رقم ملف",
        "ملف الشركة",
        "ژمارەی بایندەری کۆمپانیا",  # exact Kurdish phrase
        "ژمارەی بایندەری",
        "بایندەری",
        "binder",
        "file no",
        "file_no",
        "fileno",
    ],
    # Client spec:  "اسم الشركة"  |  "کۆمپانیای"
    "company": [
        "اسم الشركة",           # exact Arabic phrase
        "اسم_الشركة",
        "اسم الشركه",
        "کۆمپانیای",             # exact Kurdish phrase
        "كومبانيا",
        "شركة",
        "company name",
        "company_name",
        "companyname",
        "company",
    ],
    # Client spec:  "رقم الترخيص"  |  "ژمارەی مۆڵەتی کۆمپانیا"
    "license": [
        "رقم الترخيص",           # exact Arabic phrase
        "رقم_الترخيص",
        "الترخيص",
        "ژمارەی مۆڵەتی کۆمپانیا",  # exact Kurdish phrase
        "ژمارەی مۆڵەتی",
        "مۆڵەتی",
        "مۆڵەت",
        "license no",
        "license_no",
        "licenseno",
        "license",
        "licence",
    ],
}


def detect_column(headers: list[str], kind: str) -> str | None:
    """
    Scan every header for any keyword substring (case-insensitive).
    Returns the first matching header name, or None if not found.

    Priority: earlier headers win, matching the typical left-to-right
    sheet layout.  Longer keywords are tried first within each header
    so "رقم ملف الشركة" matches before the shorter "ملف الشركة".
    """
    keywords = sorted(_COL_KEYWORDS.get(kind, []),
                       key=len, reverse=True)   # longest match first
    for h in headers:
        h_lower = h.lower().strip()
        for kw in keywords:
            if kw.lower() in h_lower:
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
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        s = s.replace(ch, "")
    s = s.replace("\xa0", " ")
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
#  8 · HTML TABLE RENDERER  (no canvas → guaranteed colour fidelity)
# ─────────────────────────────────────────────────────────────────────────────
def render_html_table(df: pd.DataFrame, max_rows: int = 500) -> None:
    if df.empty:
        st.info("No records to display.")
        return

    display_df = df.head(max_rows)

    th_cells = "<th class='row-idx'>#</th>"
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
            cell_disp = raw_val if raw_val else "—"

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
        "<div class='gov-table-wrap'>"
        "<table class='gov-table'>"
        f"<thead><tr>{th_cells}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
#  9 · FILTER ENGINE
#
#  Rules
#  ─────
#  • Empty string  →  filter is skipped entirely (no effect on results).
#  • str.contains(val, case=False, na=False)  →  partial, case-insensitive.
#  • The DataFrame index is NEVER reset so the caller's mapping
#    "sheet_row = df_index + 2" always stays correct.
#  • Auditor email checks both the system COL_AUDITOR column AND any
#    column whose header contains "auditor_email" (case-insensitive).
# ─────────────────────────────────────────────────────────────────────────────
def apply_filters(
    df          : pd.DataFrame,
    f_email     : str,
    f_binder    : str,
    f_company   : str,
    f_license   : str,
    f_status    : str,
    col_binder  : str | None,
    col_company : str | None,
    col_license : str | None,
) -> pd.DataFrame:

    result = df.copy()   # original integer index preserved throughout

    # Status
    if f_status == "pending":
        result = result[result[COL_STATUS] != VAL_DONE]
    elif f_status == "done":
        result = result[result[COL_STATUS] == VAL_DONE]

    # Auditor email  (searches COL_AUDITOR + any "auditor_email" column)
    if f_email.strip():
        email_cols = [
            c for c in result.columns
            if "auditor_email" in c.lower() or c == COL_AUDITOR
        ]
        if email_cols:
            mask = pd.Series(False, index=result.index)
            for ec in email_cols:
                mask |= result[ec].str.contains(
                    f_email.strip(), case=False, na=False
                )
            result = result[mask]

    # Binder number  (only if column was detected)
    if f_binder.strip() and col_binder and col_binder in result.columns:
        result = result[
            result[col_binder].str.contains(
                f_binder.strip(), case=False, na=False
            )
        ]

    # Company name
    if f_company.strip() and col_company and col_company in result.columns:
        result = result[
            result[col_company].str.contains(
                f_company.strip(), case=False, na=False
            )
        ]

    # License number
    if f_license.strip() and col_license and col_license in result.columns:
        result = result[
            result[col_license].str.contains(
                f_license.strip(), case=False, na=False
            )
        ]

    return result   # index unchanged → sheet_row = index + 2


def _n_active(f_email, f_binder, f_company, f_license, f_status) -> int:
    return sum([
        bool(f_email.strip()),
        bool(f_binder.strip()),
        bool(f_company.strip()),
        bool(f_license.strip()),
        f_status != "all",
    ])

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
        df_u = pd.DataFrame(users_ws.get_all_records())
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
      [data-testid="collapsedControl"] { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    _g, c1, c2, c3, c4 = st.columns([5, .55, .55, .55, .55])
    with c1:
        if st.button("EN", key="lg_en"):  st.session_state.lang  = "en";    st.rerun()
    with c2:
        if st.button("KU", key="lg_ku"):  st.session_state.lang  = "ku";    st.rerun()
    with c3:
        if st.button("☀️", key="lg_lgt"): st.session_state.theme = "light"; st.rerun()
    with c4:
        if st.button("🌙", key="lg_drk"): st.session_state.theme = "dark";  st.rerun()

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
                f"<p style='font-size:.78rem;color:{P['text_muted']};text-align:center;"
                f"margin-bottom:14px;'>{t('login_prompt')}</p>",
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
                st.session_state.user_email = (
                    "Admin" if role == "admin" else email_in.lower().strip()
                )
                st.session_state.user_role = role
                st.rerun()
            else:
                st.error(t("bad_creds"))

# ─────────────────────────────────────────────────────────────────────────────
#  14 · SIDEBAR  (Advanced Filters + controls)
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(
    headers    : list[str],
    col_binder : str | None,
    col_company: str | None,
    col_license: str | None,
) -> tuple[str, str, str, str, str]:

    with st.sidebar:
        # Brand strip
        st.markdown(f"""
        <div style="border-top:3px solid {P['gold']};padding:18px 16px 14px;">
          <div style="font-size:1.05rem;font-weight:700;
                      color:{P['text_primary']};margin-bottom:2px;">
            🏛️&nbsp; {t('portal_title')}
          </div>
          <div style="font-size:0.63rem;color:{P['text_muted']};
                      letter-spacing:.13em;text-transform:uppercase;">
            {t('ministry')}
          </div>
        </div>
        <hr style="margin:0;border-color:{P['border']};"/>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Language
        st.markdown(f"<div class='sb-label'>{t('language')}</div>",
                    unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"):
            st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang = "ku"; st.rerun()

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Theme
        st.markdown(f"<div class='sb-label'>{t('theme')}</div>",
                    unsafe_allow_html=True)
        tc1, tc2 = st.columns(2)
        if tc1.button("☀️  Light", use_container_width=True, key="sb_lgt"):
            st.session_state.theme = "light"; st.rerun()
        if tc2.button("🌙  Dark",  use_container_width=True, key="sb_drk"):
            st.session_state.theme = "dark";  st.rerun()

        st.markdown(
            f"<hr style='border-color:{P['border']};margin:14px 0;'/>",
            unsafe_allow_html=True,
        )

        # ════════════════════════════════════════════════════════════════════
        #  ADVANCED FILTERS — 4 specific criteria as requested
        # ════════════════════════════════════════════════════════════════════
        st.markdown(
            f"<div class='adv-filter-header'>{t('adv_filters')}</div>",
            unsafe_allow_html=True,
        )

        # 1 · Status dropdown
        status_opts = {
            "all"    : t("status_all"),
            "pending": t("status_pending"),
            "done"   : t("status_done"),
        }
        f_status = st.selectbox(
            t("f_status"),
            options=list(status_opts.keys()),
            format_func=lambda k: status_opts[k],
            key="f_status",
        )

        # 2 · Auditor Email
        # The label shows exactly what the user asked for
        st.markdown(
            f"<div class='sb-label' style='margin-top:10px;'>"
            f"{t('f_email')}"
            f"<span class='col-hint'> ({COL_AUDITOR})</span></div>",
            unsafe_allow_html=True,
        )
        f_email = st.text_input(
            label      = t("f_email"),
            placeholder= "partial match, e.g. @mof",
            key        = "f_email",
            label_visibility = "collapsed",
        )

        # 3 · Binder Number  →  "رقم ملف الشركة" / "ژمارەی بایندەری کۆمپانیا"
        binder_ok   = col_binder is not None
        binder_hint = col_binder if binder_ok else "column not detected"
        st.markdown(
            f"<div class='sb-label' style='margin-top:10px;'>"
            f"{t('f_binder')}"
            f"<span class='col-hint'> ({binder_hint})</span></div>",
            unsafe_allow_html=True,
        )
        f_binder = st.text_input(
            label      = t("f_binder"),
            placeholder= "e.g. 12345",
            key        = "f_binder",
            disabled   = not binder_ok,
            label_visibility = "collapsed",
        )

        # 4 · Company Name  →  "اسم الشركة" / "کۆمپانیای"
        company_ok   = col_company is not None
        company_hint = col_company if company_ok else "column not detected"
        st.markdown(
            f"<div class='sb-label' style='margin-top:10px;'>"
            f"{t('f_company')}"
            f"<span class='col-hint'> ({company_hint})</span></div>",
            unsafe_allow_html=True,
        )
        f_company = st.text_input(
            label      = t("f_company"),
            placeholder= "e.g. Al-Rasheed",
            key        = "f_company",
            disabled   = not company_ok,
            label_visibility = "collapsed",
        )

        # 5 · License Number  →  "رقم الترخيص" / "ژمارەی مۆڵەتی کۆمپانیا"
        license_ok   = col_license is not None
        license_hint = col_license if license_ok else "column not detected"
        st.markdown(
            f"<div class='sb-label' style='margin-top:10px;'>"
            f"{t('f_license')}"
            f"<span class='col-hint'> ({license_hint})</span></div>",
            unsafe_allow_html=True,
        )
        f_license = st.text_input(
            label      = t("f_license"),
            placeholder= "e.g. LIC-2024",
            key        = "f_license",
            disabled   = not license_ok,
            label_visibility = "collapsed",
        )

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button(f"✕  {t('clear_filters')}", use_container_width=True, key="clr_f"):
            for k in ("f_email", "f_binder", "f_company", "f_license"):
                st.session_state[k] = ""
            st.session_state["f_status"] = "all"
            st.rerun()

        st.markdown(
            f"<hr style='border-color:{P['border']};margin:14px 0;'/>",
            unsafe_allow_html=True,
        )

        # User badge
        role_label = (
            t("role_admin") if st.session_state.user_role == "admin"
            else t("role_auditor")
        )
        chip_cls = (
            "chip-admin" if st.session_state.user_role == "admin" else "chip-audit"
        )
        st.markdown(f"""
        <div class="sb-user-badge">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="chip {chip_cls}" style="margin-top:6px;">{role_label}</span>
        </div>
        """, unsafe_allow_html=True)

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

# ─────────────────────────────────────────────────────────────────────────────
#  15 · FILTER RESULT BAR
# ─────────────────────────────────────────────────────────────────────────────
def render_filter_bar(
    total, filtered, f_email, f_binder, f_company, f_license, f_status
) -> None:
    n = _n_active(f_email, f_binder, f_company, f_license, f_status)
    if n == 0:
        return

    badges = ""
    if f_status != "all":
        badges += f"<span class='filter-badge'>⚡ {f_status}</span> "
    if f_email.strip():
        badges += f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span> "
    if f_binder.strip():
        badges += f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span> "
    if f_company.strip():
        badges += f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span> "
    if f_license.strip():
        badges += f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span> "

    st.markdown(f"""
    <div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:700;color:{P['gold']};
                   text-transform:uppercase;letter-spacing:.10em;">
        {t('active_filters')} ({n})
      </span>
      {badges}
      <span class="result-count">
        <strong style="color:{P['gold']};">{filtered}</strong> / {total}
        &nbsp;{t('results_shown')}
      </span>
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  16 · ANALYTICS TAB
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df: pd.DataFrame) -> None:
    pt = P["plotly_theme"]; pb = P["plot_bg"]
    pg = P["plot_grid"];    fc = P["text_primary"]

    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>",
                unsafe_allow_html=True)
    periods = [
        ("all", t("all_time")), ("today", t("today")),
        ("this_week", t("this_week")), ("this_month", t("this_month")),
    ]
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
            lambda s: parse_dt(s).date() if parse_dt(s) else None
        ).nunique()
    mb.metric(t("active_days"), active)
    mc.metric(t("avg_per_day"), f"{len(done_f)/max(active,1):.1f}")

    left, right = st.columns([1, 1.6], gap="large")

    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>",
                    unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb = (done_f[COL_AUDITOR].replace("", "—")
                  .value_counts().reset_index())
            lb.columns = ["Auditor", "Count"]
            medals = ["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i, r in lb.head(10).iterrows():
                m = medals[i] if i < len(medals) else f"{i+1}."
                st.markdown(f"""
                <div class="lb-row">
                  <span class="lb-medal">{m}</span>
                  <span class="lb-name">{r['Auditor']}</span>
                  <span class="lb-count">{r['Count']}</span>
                </div>""", unsafe_allow_html=True)

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
                margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg, zeroline=False, tickfont=dict(color=fc)),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending",
                           tickfont=dict(color=fc)),
                height=min(320, max(180, 36*len(lb.head(10)))),
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
            trend = (done_f.dropna(subset=["_date"])
                     .groupby("_date").size().reset_index(name="Records"))
            trend.columns = ["Date", "Records"]
            if not trend.empty:
                if len(trend) > 1:
                    full_rng = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = (trend.set_index("Date")
                             .reindex(full_rng.date, fill_value=0).reset_index())
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
                    showlegend=False, margin=dict(l=8,r=8,t=10,b=8),
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
                    already = (
                        not recs.empty
                        and nu_email.lower().strip() in
                        recs.get("email", pd.Series()).values
                    )
                    if already:
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
                sel_email = st.selectbox("Select staff", staff_df["email"].tolist())
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
            safe_cols = [c for c in ["email","created_at"] if c in staff_df.columns]
            render_html_table(staff_df[safe_cols].reset_index())

            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>",
                        unsafe_allow_html=True)
            del_email = st.selectbox(
                "Select account to revoke",
                ["—"] + staff_df["email"].tolist(), key="del_sel",
            )
            if del_email != "—":
                if st.button(f"Revoke — {del_email}", key="del_btn"):
                    cell = users_ws.find(del_email)
                    if cell:
                        users_ws.delete_rows(cell.row)
                        st.success(f"✅  {del_email} revoked.")
                        time.sleep(0.7); st.rerun()
        else:
            st.info("No auditor accounts registered.")

# ─────────────────────────────────────────────────────────────────────────────
#  18 · MAIN CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        spreadsheet   = get_spreadsheet()
        all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]

        if "UsersDB" not in all_ws_titles:
            users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="500", cols="3")
            users_ws.append_row(["email","password","created_at"])
        else:
            users_ws = spreadsheet.worksheet("UsersDB")

        # ── AUTH GATE ────────────────────────────────────────────────────────
        if not st.session_state.logged_in:
            render_login(users_ws)
            return

        st.markdown(
            "<style>[data-testid='stSidebar']{display:flex!important;}</style>",
            unsafe_allow_html=True,
        )

        # ── Page header ──────────────────────────────────────────────────────
        ts_str = datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-title">🏛️  {t('portal_title')}</div>
        <div class="page-sub">{ts_str}</div>
        """, unsafe_allow_html=True)

        # ── Workspace selector ───────────────────────────────────────────────
        data_sheets = [n for n in all_ws_titles if n != "UsersDB"]
        ws_name     = st.selectbox(t("workspace"), data_sheets, key="ws_sel")
        current_ws           = spreadsheet.worksheet(ws_name)
        df, headers, col_map = load_worksheet(current_ws)

        if df.empty:
            st.warning(t("empty_sheet"))
            return

        # ── Column auto-detection ────────────────────────────────────────────
        col_binder  = detect_column(headers, "binder")
        col_company = detect_column(headers, "company")
        col_license = detect_column(headers, "license")

        is_admin = st.session_state.user_role == "admin"

        # ── Sidebar + filters ────────────────────────────────────────────────
        f_email, f_binder, f_company, f_license, f_status = render_sidebar(
            headers, col_binder, col_company, col_license
        )

        # ── Overview metrics ─────────────────────────────────────────────────
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
        <div class="gov-progress-wrap">
          <div class="gov-progress-fill" style="width:{int(pct*100)}%;"></div>
        </div>
        """, unsafe_allow_html=True)

        # ── Apply all filters (original index preserved) ──────────────────────
        filtered_df = apply_filters(
            df, f_email, f_binder, f_company, f_license, f_status,
            col_binder, col_company, col_license,
        )

        render_filter_bar(
            total_n, len(filtered_df),
            f_email, f_binder, f_company, f_license, f_status,
        )

        # ── Tabs ─────────────────────────────────────────────────────────────
        tab_names = [t("tab_queue"), t("tab_archive"), t("tab_analytics")]
        if is_admin:
            tab_names.append(t("tab_users"))
        tabs = st.tabs(tab_names)

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 1 — AUDIT WORKLIST
        # ══════════════════════════════════════════════════════════════════════
        with tabs[0]:
            # Pending records from filtered set.  DO NOT reset_index().
            pending_view = filtered_df[filtered_df[COL_STATUS] != VAL_DONE].copy()

            # Shift index for display — values now equal Google Sheets row numbers.
            pending_display = pending_view.copy()
            pending_display.index = pending_display.index + 2

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
                msg = (
                    t("no_match")
                    if _n_active(f_email, f_binder, f_company, f_license, f_status)
                    else "✅  All cases have been processed."
                )
                st.info(msg)
            else:
                render_html_table(pending_display)

                st.markdown(
                    f"<div class='section-title'>🔍 {t('select_case')}</div>",
                    unsafe_allow_html=True,
                )

                label_col = next(
                    (h for h in headers if h not in SYSTEM_COLS),
                    headers[0] if headers else "Row",
                )
                opts = ["—"] + [
                    f"Row {idx}  ·  {str(row.get(label_col,''))[:55]}"
                    for idx, row in pending_display.iterrows()
                ]
                row_sel = st.selectbox(
                    "", opts, key="row_sel", label_visibility="collapsed"
                )

                if row_sel != "—":
                    # ── CRITICAL ROW INDEX MAPPING ────────────────────────────
                    # pending_display.index  = sheet row numbers (original + 2).
                    # df.iloc[sheet_row - 2] = correct original record.
                    sheet_row = int(
                        row_sel.split("  ·  ")[0].replace("Row ", "").strip()
                    )
                    df_iloc = sheet_row - 2

                    # Guard against edge-case index misalignment
                    if df_iloc < 0 or df_iloc >= len(df):
                        st.error("Row index out of range — please refresh.")
                        return

                    record = df.iloc[df_iloc].to_dict()

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
                        f"<div class='section-title'>✏️ {t('processing')} "
                        f"#{sheet_row}</div>",
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
                            # Write only changed user-editable fields
                            for fname, fval in new_vals.items():
                                if (fname in col_map
                                        and clean_cell(record.get(fname, "")) != fval):
                                    current_ws.update_cell(
                                        sheet_row, col_map[fname], fval
                                    )
                            # Automated metadata — silent, never editable
                            ts_now  = now_str()
                            auditor = st.session_state.user_email
                            old_log = str(record.get(COL_LOG, "")).strip()
                            new_log = (
                                f"✔  {auditor}  |  {ts_now}\n{old_log}".strip()
                            )
                            current_ws.update_cell(
                                sheet_row, col_map[COL_STATUS], VAL_DONE
                            )
                            current_ws.update_cell(
                                sheet_row, col_map[COL_LOG], new_log
                            )
                            current_ws.update_cell(
                                sheet_row, col_map[COL_AUDITOR], auditor
                            )
                            current_ws.update_cell(
                                sheet_row, col_map[COL_DATE], ts_now
                            )

                        st.success(t("saved_ok"))
                        time.sleep(0.8); st.rerun()

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 2 — PROCESSED ARCHIVE
        # ══════════════════════════════════════════════════════════════════════
        with tabs[1]:
            done_view = filtered_df[filtered_df[COL_STATUS] == VAL_DONE].copy()
            done_view.index = done_view.index + 2

            d_count = len(done_view)
            st.markdown(f"""
            <div class="worklist-header">
              <div>
                <div class="worklist-title">✅ Processed Archive</div>
                <div class="worklist-sub">Completed and committed audit records</div>
              </div>
              <span class="chip chip-done">{d_count} {t('processed')}</span>
            </div>
            """, unsafe_allow_html=True)

            if done_view.empty:
                msg = (
                    t("no_match")
                    if _n_active(f_email, f_binder, f_company, f_license, f_status)
                    else "No processed records yet."
                )
                st.info(msg)
            else:
                render_html_table(done_view)

            if is_admin and not done_view.empty:
                st.markdown("---")
                st.markdown(f"<div class='section-title'>↩️ {t('reopen')}</div>",
                            unsafe_allow_html=True)
                reopen_opts = ["—"] + [f"Row {idx}" for idx in done_view.index]
                reopen_sel  = st.selectbox(
                    "Select record to re-open:", reopen_opts, key="reopen_sel"
                )
                if reopen_sel != "—":
                    ridx = int(reopen_sel.split(" ")[1])
                    if st.button(t("reopen"), key="reopen_btn"):
                        if COL_STATUS in col_map:
                            current_ws.update_cell(
                                ridx, col_map[COL_STATUS], VAL_PENDING
                            )
                            st.rerun()

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 3 — ANALYTICS
        # ══════════════════════════════════════════════════════════════════════
        with tabs[2]:
            render_analytics(df)

        # ══════════════════════════════════════════════════════════════════════
        #  TAB 4 — USER ADMIN  (admin only)
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
