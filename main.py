# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  ·  v7.0
#  Enterprise Governmental Tax & Customs Data Audit Platform
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

st.set_page_config(
    page_title="Tax Audit & Compliance Portal",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

SYSTEM_SHEETS  = {"UsersDB", "Settings"}
SETTINGS_SHEET = "Settings"
USERS_SHEET    = "UsersDB"
SETTINGS_COL   = "Visible_Worksheets"

_PALETTES: dict = {
    "dark": {
        "page_bg":"#060B14","surface":"#0C1424","surface2":"#101A2E",
        "card":"#0E1728","card2":"#121F36","border":"#1A2C48","border2":"#1E3558",
        "text_primary":"#EAEEF8","text_secondary":"#7A9CC4","text_muted":"#3D5A7A",
        "gold":"#C9A84C","gold_light":"#E4C878","gold_bg":"rgba(201,168,76,0.10)",
        "blue_accent":"#3470CC","blue_bg":"rgba(52,112,204,0.12)",
        "green":"#25A374","green_bg":"rgba(37,163,116,0.13)",
        "amber":"#E09820","amber_bg":"rgba(224,152,32,0.13)",
        "red":"#D94F4F","red_bg":"rgba(217,79,79,0.13)",
        "input_bg":"#060B14","input_border":"#1A2C48","input_focus":"#C9A84C",
        "btn_primary":"#C9A84C","btn_text":"#060B14",
        "prog_track":"#1A2C48","prog_fill_a":"#C9A84C","prog_fill_b":"#3470CC",
        "plotly_theme":"plotly_dark","plot_bg":"#0C1424","plot_grid":"#1A2C48",
        "tbl_bg":"#0E1728","tbl_hdr_bg":"#101A2E",
        "tbl_text":"#FFFFFF","tbl_hdr_txt":"#7A9CC4",
        "tbl_border":"#1A2C48","tbl_hover":"#141E30",
        "metric_rgba":"rgba(14,23,40,0.90)",
    },
    "light": {
        "page_bg":"#F0F4FB","surface":"#FFFFFF","surface2":"#E8EFF8",
        "card":"#FFFFFF","card2":"#F5F8FD","border":"#C0D0E8","border2":"#A8C0DC",
        "text_primary":"#0C1A30","text_secondary":"#2A4068","text_muted":"#607898",
        "gold":"#9A7020","gold_light":"#B88A30","gold_bg":"rgba(154,112,32,0.09)",
        "blue_accent":"#1658B8","blue_bg":"rgba(22,88,184,0.09)",
        "green":"#157A50","green_bg":"rgba(21,122,80,0.09)",
        "amber":"#A85808","amber_bg":"rgba(168,88,8,0.09)",
        "red":"#A82828","red_bg":"rgba(168,40,40,0.09)",
        "input_bg":"#FFFFFF","input_border":"#B8CCE8","input_focus":"#1658B8",
        "btn_primary":"#0D2A58","btn_text":"#FFFFFF",
        "prog_track":"#D0E0F5","prog_fill_a":"#9A7020","prog_fill_b":"#1658B8",
        "plotly_theme":"plotly_white","plot_bg":"#FFFFFF","plot_grid":"#D0E0F5",
        "tbl_bg":"#FFFFFF","tbl_hdr_bg":"#E8EFF8",
        "tbl_text":"#000000","tbl_hdr_txt":"#2A4068",
        "tbl_border":"#C0D0E8","tbl_hover":"#EDF4FF",
        "metric_rgba":"rgba(255,255,255,0.95)",
    },
}

P = _PALETTES[st.session_state.theme]

def inject_css(P: dict) -> None:
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
:root{{
  --page-bg:{P['page_bg']};--surface:{P['surface']};--surface2:{P['surface2']};
  --card:{P['card']};--card2:{P['card2']};--border:{P['border']};--border2:{P['border2']};
  --text-primary:{P['text_primary']};--text-secondary:{P['text_secondary']};--text-muted:{P['text_muted']};
  --gold:{P['gold']};--gold-light:{P['gold_light']};--gold-bg:{P['gold_bg']};
  --blue:{P['blue_accent']};--blue-bg:{P['blue_bg']};
  --green:{P['green']};--green-bg:{P['green_bg']};
  --amber:{P['amber']};--amber-bg:{P['amber_bg']};
  --red:{P['red']};--red-bg:{P['red_bg']};
  --input-bg:{P['input_bg']};--input-border:{P['input_border']};--input-focus:{P['input_focus']};
  --btn-primary:{P['btn_primary']};--btn-text:{P['btn_text']};
  --prog-track:{P['prog_track']};
}}
*,*::before,*::after{{box-sizing:border-box!important;font-family:'IBM Plex Sans',sans-serif!important;}}
html,body,.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],.main,.block-container{{
  background-color:{P['page_bg']}!important;color:{P['text_primary']}!important;}}
p,span,div,li,label,h1,h2,h3,h4,h5,h6,.stMarkdown,[data-testid="stMarkdownContainer"]{{color:{P['text_primary']}!important;}}
#MainMenu,footer,header,.stDeployButton,[data-testid="stToolbar"]{{display:none!important;}}
[data-testid="stSidebar"]{{background-color:{P['surface']}!important;border-right:1px solid {P['border']}!important;}}
[data-testid="stSidebar"] *{{color:{P['text_primary']}!important;}}
[data-testid="stSidebarCollapseButton"],[data-testid="collapsedControl"]{{display:none!important;}}
.stTextInput>div>div>input,
.stTextArea>div>div>textarea,
[data-testid="stSidebar"] .stTextInput>div>div>input,
[data-testid="stSidebar"] .stTextArea>div>div>textarea{{
  background-color:{P['input_bg']}!important;color:{P['text_primary']}!important;
  border:1.5px solid {P['input_border']}!important;border-radius:7px!important;
  font-size:0.875rem!important;caret-color:{P['gold']}!important;padding:9px 12px!important;
  transition:border-color .2s ease,box-shadow .2s ease!important;}}
.stTextInput>div>div>input:focus,
.stTextArea>div>div>textarea:focus,
[data-testid="stSidebar"] .stTextInput>div>div>input:focus{{
  border-color:{P['input_focus']}!important;
  box-shadow:0 0 0 3px {P['gold_bg']},inset 0 0 0 1px {P['input_focus']}!important;
  outline:none!important;color:{P['text_primary']}!important;}}
.stTextInput>div>div>input::placeholder,
.stTextArea>div>div>textarea::placeholder,
[data-testid="stSidebar"] .stTextInput>div>div>input::placeholder{{
  color:{P['text_muted']}!important;opacity:.75!important;}}
[data-testid="stSidebar"] .stTextInput>div>div>input:disabled{{
  opacity:.35!important;cursor:not-allowed!important;color:{P['text_muted']}!important;}}
.stSelectbox>div>div,[data-baseweb="select"]>div,
[data-testid="stSidebar"] .stSelectbox>div>div,
[data-testid="stSidebar"] [data-baseweb="select"]>div{{
  background-color:{P['input_bg']}!important;color:{P['text_primary']}!important;border-color:{P['input_border']}!important;}}
[data-baseweb="select"]:focus-within>div{{border-color:{P['input_focus']}!important;box-shadow:0 0 0 3px {P['gold_bg']}!important;}}
[data-baseweb="menu"] li,[data-baseweb="menu"] [role="option"]{{background-color:{P['surface']}!important;color:{P['text_primary']}!important;}}
[data-baseweb="menu"] li:hover,[data-baseweb="menu"] [aria-selected="true"]{{background-color:{P['surface2']}!important;color:{P['gold']}!important;}}
.stTextInput>label,.stTextArea>label,.stSelectbox>label,.stMultiSelect>label{{
  color:{P['text_muted']}!important;font-size:.68rem!important;font-weight:700!important;
  letter-spacing:.10em!important;text-transform:uppercase!important;}}
[data-testid="stMetricContainer"]{{
  background:{P['metric_rgba']}!important;border:1px solid {P['border']}!important;
  border-radius:12px!important;padding:18px 22px!important;backdrop-filter:blur(8px)!important;
  box-shadow:0 2px 12px rgba(0,0,0,.14)!important;transition:transform .22s ease,box-shadow .22s ease!important;}}
[data-testid="stMetricContainer"]:hover{{transform:translateY(-4px)!important;box-shadow:0 10px 28px rgba(0,0,0,.22),0 0 0 1px {P['gold']}!important;}}
[data-testid="stMetricValue"]{{font-family:'IBM Plex Mono',monospace!important;font-size:2.1rem!important;font-weight:600!important;color:{P['gold']}!important;}}
[data-testid="stMetricLabel"]{{font-size:.68rem!important;font-weight:700!important;letter-spacing:.12em!important;text-transform:uppercase!important;color:{P['text_muted']}!important;}}
.stButton>button{{background-color:{P['btn_primary']}!important;color:{P['btn_text']}!important;
  border:none!important;border-radius:7px!important;font-weight:600!important;font-size:.84rem!important;
  padding:9px 18px!important;transition:opacity .15s ease,transform .15s ease,box-shadow .15s ease!important;}}
.stButton>button:hover{{opacity:.88!important;transform:translateY(-2px)!important;box-shadow:0 6px 16px rgba(0,0,0,.25)!important;}}
.stButton>button:active{{transform:translateY(0)!important;}}
div[data-testid="stForm"]{{background-color:{P['card']}!important;border:1px solid {P['border']}!important;border-radius:14px!important;padding:24px 28px!important;}}
.stTabs [data-baseweb="tab-list"]{{gap:2px!important;background:transparent!important;border-bottom:2px solid {P['border']}!important;}}
.stTabs [data-baseweb="tab"]{{background:transparent!important;color:{P['text_muted']}!important;
  border-radius:8px 8px 0 0!important;border:1px solid transparent!important;border-bottom:none!important;
  padding:10px 20px!important;font-weight:600!important;font-size:.82rem!important;}}
.stTabs [data-baseweb="tab"]:hover{{color:{P['text_primary']}!important;}}
.stTabs [aria-selected="true"]{{background-color:{P['card']}!important;color:{P['gold']}!important;
  border-color:{P['border']}!important;border-bottom-color:{P['card']}!important;margin-bottom:-2px!important;}}
.gov-table-wrap{{overflow-x:auto;border:1px solid {P['tbl_border']};border-radius:12px;margin-bottom:16px;}}
.gov-table{{width:100%;border-collapse:collapse;background-color:{P['tbl_bg']};font-size:.82rem;}}
.gov-table thead tr{{background-color:{P['tbl_hdr_bg']};border-bottom:2px solid {P['border2']};}}
.gov-table th{{color:{P['tbl_hdr_txt']}!important;background-color:{P['tbl_hdr_bg']}!important;
  font-weight:700!important;font-size:.65rem!important;letter-spacing:.10em!important;text-transform:uppercase!important;
  padding:11px 14px!important;white-space:nowrap;text-align:left!important;border-right:1px solid {P['tbl_border']};}}
.gov-table th:last-child{{border-right:none;}}
.gov-table td{{color:{P['tbl_text']}!important;background-color:{P['tbl_bg']}!important;
  padding:9px 14px!important;font-size:.82rem!important;
  border-bottom:1px solid {P['tbl_border']}!important;border-right:1px solid {P['tbl_border']}!important;
  vertical-align:middle!important;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.gov-table td:last-child{{border-right:none;}}
.gov-table tbody tr:hover td{{background-color:{P['tbl_hover']}!important;color:{P['tbl_text']}!important;}}
.gov-table tbody tr:last-child td{{border-bottom:none!important;}}
.gov-table td.row-idx,.gov-table th.row-idx{{color:{P['text_muted']}!important;font-family:'IBM Plex Mono',monospace!important;font-size:.70rem!important;min-width:50px;text-align:center!important;}}
.s-chip{{display:inline-flex;align-items:center;padding:2px 9px;border-radius:99px;font-size:.62rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;white-space:nowrap;}}
.s-done{{background:{P['green_bg']};color:{P['green']}!important;}}
.s-pending{{background:{P['amber_bg']};color:{P['amber']}!important;}}
.streamlit-expanderHeader{{background-color:{P['surface2']}!important;color:{P['text_primary']}!important;border:1px solid {P['border']}!important;border-radius:8px!important;font-weight:600!important;}}
.streamlit-expanderContent{{background-color:{P['card']}!important;border:1px solid {P['border']}!important;border-top:none!important;border-radius:0 0 8px 8px!important;padding:14px!important;}}
[data-testid="stAlert"]{{border-radius:9px!important;border:1px solid {P['border']}!important;background-color:{P['surface2']}!important;}}
[data-testid="stAlert"] *{{color:{P['text_primary']}!important;}}
.gov-login-card{{width:100%;max-width:460px;background-color:{P['card']}!important;
  border:1.5px solid {P['border2']};border-top:5px solid {P['gold']};border-radius:18px;
  padding:48px 44px 38px;box-shadow:0 32px 72px rgba(0,0,0,.30),0 0 0 1px {P['border']};}}
.gov-seal-ring{{width:72px;height:72px;margin:0 auto 14px;border-radius:50%;
  border:3px solid {P['gold']};background:linear-gradient(135deg,{P['surface2']},{P['card']});
  display:flex;align-items:center;justify-content:center;font-size:2rem;
  box-shadow:0 0 0 6px {P['gold_bg']},0 4px 16px rgba(0,0,0,.24);}}
.gov-ministry-band{{text-align:center;margin-bottom:4px;font-size:.62rem;font-weight:700;
  letter-spacing:.22em;text-transform:uppercase;color:{P['text_muted']}!important;}}
.gov-portal-name{{text-align:center;font-size:1.38rem;font-weight:700;color:{P['text_primary']}!important;letter-spacing:-.02em;margin-bottom:3px;}}
.gov-portal-tagline{{text-align:center;font-size:.76rem;color:{P['text_muted']}!important;margin-bottom:20px;}}
.gold-rule{{width:48px;height:3px;background:{P['gold']};border-radius:99px;margin:0 auto 18px;}}
.classification-badge{{display:inline-flex;align-items:center;gap:6px;background:{P['red_bg']};
  color:{P['red']}!important;border:1px solid {P['red']};border-radius:5px;
  font-size:.60rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;padding:3px 10px;margin:0 auto 16px;}}
.gov-login-card .stButton>button{{background-color:{P['gold']}!important;color:{P['page_bg']}!important;
  font-weight:700!important;font-size:.94rem!important;border-radius:9px!important;padding:12px!important;
  width:100%!important;border:2px solid transparent!important;
  transition:background-color .18s ease,border-color .18s ease,transform .15s ease,box-shadow .18s ease!important;}}
.gov-login-card .stButton>button:hover{{background-color:{P['gold_light']}!important;
  border-color:{P['gold']}!important;box-shadow:0 6px 20px rgba(201,168,76,.40)!important;transform:translateY(-2px)!important;}}
.page-title{{font-size:1.5rem;font-weight:700;color:{P['text_primary']}!important;letter-spacing:-.02em;margin-bottom:2px;}}
.page-sub{{font-size:.80rem;color:{P['text_muted']}!important;margin-bottom:18px;}}
.section-title{{display:flex;align-items:center;gap:9px;font-size:.86rem;font-weight:700;
  color:{P['text_primary']}!important;margin:20px 0 11px;padding-left:11px;border-left:3px solid {P['gold']};}}
.worklist-header{{display:flex;align-items:center;justify-content:space-between;
  background-color:{P['surface2']}!important;border:1px solid {P['border']};
  border-top:3px solid {P['blue_accent']};border-radius:10px;padding:13px 18px;margin-bottom:14px;}}
.worklist-title{{font-size:.94rem;font-weight:700;color:{P['text_primary']}!important;}}
.worklist-sub{{font-size:.72rem;color:{P['text_muted']}!important;margin-top:2px;}}
.gov-progress-wrap{{background-color:{P['prog_track']};border-radius:99px;height:6px;overflow:hidden;margin:5px 0 9px;}}
.gov-progress-fill{{height:100%;border-radius:99px;background:linear-gradient(90deg,{P['prog_fill_a']},{P['prog_fill_b']});transition:width .7s cubic-bezier(.4,0,.2,1);}}
.prog-labels{{display:flex;justify-content:space-between;font-size:.70rem;color:{P['text_muted']}!important;font-weight:600;margin-bottom:3px;}}
.chip{{display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:99px;font-size:.68rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;}}
.chip-done{{background:{P['green_bg']};color:{P['green']}!important;}}
.chip-pending{{background:{P['amber_bg']};color:{P['amber']}!important;}}
.chip-admin{{background:{P['gold_bg']};color:{P['gold']}!important;}}
.chip-audit{{background:{P['blue_bg']};color:{P['blue_accent']}!important;}}
.sb-label{{font-size:.60rem;font-weight:700;letter-spacing:.13em;text-transform:uppercase;color:{P['text_muted']}!important;margin-bottom:4px;}}
.sb-email{{font-size:.85rem;font-weight:700;color:{P['text_primary']}!important;word-break:break-all;}}
.sb-user-badge{{background-color:{P['surface2']}!important;border:1px solid {P['border']};border-radius:9px;padding:11px 13px;margin-bottom:10px;}}
.adv-filter-header{{font-size:.63rem;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:{P['gold']}!important;margin-bottom:11px;padding-bottom:8px;border-bottom:1px solid {P['border']};}}
.col-hint{{font-size:.58rem;font-weight:400;opacity:.55;color:{P['text_muted']}!important;}}
.filter-result-bar{{background-color:{P['card']}!important;border:1px solid {P['border']};border-left:3px solid {P['gold']};
  border-radius:9px;padding:11px 16px;margin-bottom:14px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;}}
.filter-badge{{display:inline-flex;align-items:center;gap:4px;background:{P['blue_bg']};color:{P['blue_accent']}!important;border:1px solid {P['blue_accent']};border-radius:99px;font-size:.64rem;font-weight:700;padding:2px 9px;}}
.result-count{{font-family:'IBM Plex Mono',monospace;font-size:.76rem;color:{P['text_muted']}!important;margin-left:auto;}}
.ws-item{{display:flex;align-items:center;justify-content:space-between;background-color:{P['card2']}!important;
  border:1px solid {P['border']};border-radius:9px;padding:10px 14px;margin-bottom:6px;transition:border-color .16s ease;}}
.ws-item:hover{{border-color:{P['gold']};}}
.ws-name{{font-size:.86rem;font-weight:600;color:{P['text_primary']}!important;font-family:'IBM Plex Mono',monospace;}}
.lb-row{{display:flex;align-items:center;gap:11px;padding:10px 14px;background-color:{P['card2']}!important;
  border:1px solid {P['border']};border-radius:9px;margin-bottom:6px;transition:transform .16s ease,border-color .16s ease;}}
.lb-row:hover{{transform:translateX(4px);border-color:{P['gold']};}}
.lb-medal{{font-size:1.1rem;width:24px;text-align:center;}}
.lb-name{{flex:1;font-size:.84rem;font-weight:600;color:{P['text_primary']}!important;}}
.lb-count{{font-family:'IBM Plex Mono',monospace;font-size:.92rem;font-weight:700;color:{P['gold']}!important;}}
.log-line{{font-family:'IBM Plex Mono',monospace;font-size:.74rem;color:{P['text_secondary']}!important;padding:3px 0;border-bottom:1px solid {P['border']};}}
.log-line:last-child{{border-bottom:none;}}
.rbac-banner{{background:{P['blue_bg']};border:1px solid {P['blue_accent']};border-left:3px solid {P['blue_accent']};
  border-radius:9px;padding:10px 14px;margin-bottom:14px;font-size:.78rem;color:{P['blue_accent']}!important;font-weight:500;}}
</style>
""", unsafe_allow_html=True)

inject_css(P)

_LANG: dict[str, dict[str, str]] = {
    "en": {
        "ministry":"Ministry of Finance & Customs",
        "portal_title":"Official Tax Audit & Compliance Portal",
        "portal_sub":"Authorised Access Only",
        "classified":"CLASSIFIED — GOVERNMENT USE ONLY",
        "login_prompt":"Enter your authorised credentials to access the system",
        "email_field":"Official Email / User ID","password_field":"Password",
        "sign_in":"Authenticate & Enter","sign_out":"Sign Out",
        "bad_creds":"Authentication failed. Check your credentials.",
        "theme":"Display Theme","language":"Interface Language",
        "workspace":"Active Case Register","overview":"Case Overview",
        "total":"Total Cases","processed":"Processed","outstanding":"Outstanding",
        "worklist_title":"Audit Worklist",
        "worklist_sub":"Active cases pending review and formal approval",
        "tab_worklist":"📋  Audit Worklist","tab_archive":"✅  Processed Archive",
        "tab_analytics":"📊  Analytics","tab_ws_mgr":"🗂️  Workspace Manager",
        "tab_users":"⚙️  User Admin",
        "select_case":"Select a case to inspect","audit_trail":"Audit Trail",
        "approve_save":"Approve & Commit Record","reopen":"Re-open Record (Admin)",
        "leaderboard":"Auditor Productivity Leaderboard","daily_trend":"Daily Processing Trend",
        "period":"Time Period","today":"Today","this_week":"This Week",
        "this_month":"This Month","all_time":"All Time",
        "add_auditor":"Register New Auditor","update_pw":"Update Password",
        "remove_user":"Revoke Access","staff_dir":"Authorised Staff",
        "no_records":"No records found for this period.",
        "empty_sheet":"This register contains no data.",
        "saved_ok":"Record approved and committed to the register.",
        "dup_email":"This email is already registered.",
        "fill_fields":"All fields are required.",
        "signed_as":"Authenticated as","role_admin":"System Administrator",
        "role_auditor":"Tax Auditor","processing":"Inspecting Case",
        "no_history":"No audit trail for this record.",
        "records_period":"Records (period)","active_days":"Active Days","avg_per_day":"Avg / Day",
        "adv_filters":"🔍 Advanced Filters","f_email":"Auditor Email",
        "f_binder":"Company Binder No.","f_company":"Company Name",
        "f_license":"License Number","f_status":"Status Filter",
        "clear_filters":"Clear All Filters","active_filters":"Active filters",
        "results_shown":"results shown","no_match":"No records match the applied filters.",
        "status_all":"All Statuses","status_pending":"Pending Only","status_done":"Processed Only",
        "ws_mgr_title":"Workspace Manager","ws_mgr_sub":"Control which sheets are visible to auditors",
        "ws_visible":"Currently Visible Worksheets","ws_add":"Add Worksheet to Visible List",
        "ws_remove":"Remove Worksheet","ws_add_btn":"Add to Visible List",
        "ws_remove_btn":"Remove","ws_available":"Available (hidden) sheets",
        "ws_none_hidden":"All sheets are already visible.",
        "ws_added":"Worksheet added.","ws_removed":"Worksheet removed.",
        "ws_already":"This sheet is already in the visible list.",
        "rbac_notice":"ℹ️  Auditor mode — Analytics and management tools are restricted to administrators.",
    },
    "ku": {
        "ministry":"وەزارەتی دارایی و گومرگ",
        "portal_title":"پۆرتەلی فەرمی وردبینی باج و پابەندبوون",
        "portal_sub":"تەنها دەستپێگەیشتنی مەرجدارکراو",
        "classified":"نهێنی — تەنها بەکارهێنانی حکومی",
        "login_prompt":"زانیارییە مەرجەکانت بنووسە بۆ چوونەژوورەوە",
        "email_field":"ئیمەیڵی فەرمی / ناساندن","password_field":"پاسۆرد",
        "sign_in":"دەستپێبکە","sign_out":"چوونەدەرەوە",
        "bad_creds":"ناسناوەکان هەڵەن. تکایە دووبارە هەوڵبدە.",
        "theme":"تیمی پیشاندان","language":"زمانی ڕووکار",
        "workspace":"تۆماری کیسە چالاکەکان","overview":"کورتەی کیسەکان",
        "total":"کۆی کیسەکان","processed":"کارکراوە","outstanding":"ماوە",
        "worklist_title":"لیستی کاری وردبینی",
        "worklist_sub":"کیسە چالاکەکانی چاوەڕوانی پشکنین و پەسەندکردنی فەرمی",
        "tab_worklist":"📋  لیستی کاری وردبینی","tab_archive":"✅  ئەرشیفی کارکراو",
        "tab_analytics":"📊  ئەنالیتیکس","tab_ws_mgr":"🗂️  بەڕێوەبردنی فضای کاری",
        "tab_users":"⚙️  بەڕێوەبردنی بەکارهێنەر",
        "select_case":"کیسێک هەڵبژێرە بۆ پشکنین","audit_trail":"مێژووی گۆڕانکاری",
        "approve_save":"پەسەندکردن و پاشەکەوتکردن","reopen":"کردنەوەی دووبارەی کیس (ئەدمین)",
        "leaderboard":"تەختەی بەرهەمهێنانی ئۆدیتۆر","daily_trend":"ترەندی بەرپرسانەی ڕۆژانە",
        "period":"ماوەی کات","today":"ئەمڕۆ","this_week":"ئەم هەفتەیە",
        "this_month":"ئەم مانگەیە","all_time":"هەموو کات",
        "add_auditor":"تۆمارکردنی ئۆدیتۆری نوێ","update_pw":"نوێکردنەوەی پاسۆرد",
        "remove_user":"هەڵوەشاندنەوەی دەستپێگەیشتن","staff_dir":"کارمەندە مەرجداركراوەکان",
        "no_records":"هیچ تۆماری نییە بۆ ئەم ماوەیە.",
        "empty_sheet":"ئەم تۆمارخانە داتای تێدا نییە.",
        "saved_ok":"کیسەکە پەسەندکرا و پاشەکەوتکرا.",
        "dup_email":"ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_fields":"هەموو خانەکان پەیوەندییانە.",
        "signed_as":"چووییتە ژوورەوە بەناوی","role_admin":"بەڕێوەبەری سیستەم",
        "role_auditor":"ئۆدیتۆری باج","processing":"پشکنینی کیسی",
        "no_history":"هیچ مێژوویەک بۆ ئەم تۆمارە نییە.",
        "records_period":"تۆمارەکان (ماوە)","active_days":"ڕۆژی چالاک","avg_per_day":"تێکڕای ڕۆژانە",
        "adv_filters":"🔍 فلتەرە پێشکەوتووەکان","f_email":"ئیمەیڵی ئۆدیتۆر",
        "f_binder":"ژمارەی بایندەری کۆمپانیا","f_company":"ناوی کۆمپانیا",
        "f_license":"ژمارەی مۆڵەتی کۆمپانیا","f_status":"فلتەری دەربار",
        "clear_filters":"سڕینەوەی هەموو فلتەرەکان","active_filters":"فلتەرە چالاکەکان",
        "results_shown":"ئەنجامی پیشاندراو","no_match":"هیچ تۆماریک لەگەڵ فلتەرەکان دەگونجێ.",
        "status_all":"هەموو دەرباریەکان","status_pending":"چاوەڕوان تەنها","status_done":"کارکراو تەنها",
        "ws_mgr_title":"بەڕێوەبردنی فضای کاری","ws_mgr_sub":"کنترۆڵ بکە کام شیتەکان بەرچاوی ئۆدیتۆرەکانن",
        "ws_visible":"فضاکانی کاری بەرچاو","ws_add":"زیادکردنی فضای کاری",
        "ws_remove":"سڕینەوەی فضای کاری","ws_add_btn":"زیادکردن",
        "ws_remove_btn":"سڕینەوە","ws_available":"شیتە بەردەستەکان (شاراوە)",
        "ws_none_hidden":"هەموو شیتەکان بەرچاون.",
        "ws_added":"فضای کاری زیادکرا.","ws_removed":"فضای کاری سڕایەوە.",
        "ws_already":"ئەم شیتە پێشتر لە لیستەدایە.",
        "rbac_notice":"ℹ️  دیمەنی ئۆدیتۆر — ئەنالیتیکس و ئامرازەکانی بەڕێوەبردن تەنها بۆ بەڕێوەبەرەکانن.",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)

COL_STATUS  = "Status"
COL_LOG     = "Audit_Log"
COL_AUDITOR = "Auditor_ID"
COL_DATE    = "Update_Date"
SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE]
VAL_DONE    = "Processed"
VAL_PENDING = "Pending"

_COL_KEYWORDS: dict[str, list[str]] = {
    "binder": [
        "رقم ملف الشركة","رقم_ملف_الشركة","رقم ملف","ملف الشركة",
        "ژمارەی بایندەری کۆمپانیا","ژمارەی بایندەری","بایندەری",
        "binder","file no","file_no",
    ],
    "company": [
        "اسم الشركة","اسم_الشركة","اسم الشركه",
        "کۆمپانیای","كومبانيا","شركة",
        "company name","company_name","company",
    ],
    "license": [
        "رقم الترخيص","رقم_الترخيص","الترخيص",
        "ژمارەی مۆڵەتی کۆمپانیا","ژمارەی مۆڵەتی","مۆڵەتی","مۆڵەت",
        "license no","license_no","license","licence",
    ],
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
    for ch in ("\u200b","\u200c","\u200d","\ufeff"):
        s = s.replace(ch,"")
    return s.replace("\xa0"," ").strip()

def apply_period_filter(df: pd.DataFrame, col: str, period: str) -> pd.DataFrame:
    if period == "all" or col not in df.columns:
        return df
    now = datetime.now(TZ)
    if period == "today":
        cutoff = now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_week":
        cutoff = (now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
    elif period == "this_month":
        cutoff = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    else:
        return df
    return df[df[col].apply(parse_dt) >= cutoff]

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
            raw_val = str(row[col]) if row[col] != "" else ""
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

def apply_filters(df, f_email, f_binder, f_company, f_license, f_status,
                  col_binder, col_company, col_license) -> pd.DataFrame:
    result = df.copy()
    if f_status == "pending":
        result = result[result[COL_STATUS] != VAL_DONE]
    elif f_status == "done":
        result = result[result[COL_STATUS] == VAL_DONE]
    if f_email.strip():
        email_cols = [c for c in result.columns
                      if "auditor_email" in c.lower() or c == COL_AUDITOR]
        if email_cols:
            mask = pd.Series(False, index=result.index)
            for ec in email_cols:
                mask |= result[ec].str.contains(f_email.strip(), case=False, na=False)
            result = result[mask]
    if f_binder.strip() and col_binder and col_binder in result.columns:
        result = result[result[col_binder].str.contains(f_binder.strip(),case=False,na=False)]
    if f_company.strip() and col_company and col_company in result.columns:
        result = result[result[col_company].str.contains(f_company.strip(),case=False,na=False)]
    if f_license.strip() and col_license and col_license in result.columns:
        result = result[result[col_license].str.contains(f_license.strip(),case=False,na=False)]
    return result

def _n_active(f_email, f_binder, f_company, f_license, f_status) -> int:
    return sum([bool(f_email.strip()),bool(f_binder.strip()),
                bool(f_company.strip()),bool(f_license.strip()),f_status!="all"])

@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    raw = json.loads(st.secrets["json_key"], strict=False)
    pk  = raw["private_key"]
    pk  = pk.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","")
    pk  = pk.replace("\\n","").replace("\n","")
    pk  = "".join(pk.split())
    pk  = "\n".join(textwrap.wrap(pk, 64))
    raw["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(raw, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

def _ensure_settings_sheet(spreadsheet):
    titles = [ws.title for ws in spreadsheet.worksheets()]
    if SETTINGS_SHEET not in titles:
        ws = spreadsheet.add_worksheet(title=SETTINGS_SHEET, rows="200", cols="2")
        ws.append_row([SETTINGS_COL])
        return ws
    return spreadsheet.worksheet(SETTINGS_SHEET)

def get_visible_worksheets(spreadsheet) -> list[str]:
    try:
        settings_ws = _ensure_settings_sheet(spreadsheet)
        all_values  = settings_ws.get_all_values()
        if len(all_values) < 2:
            return [ws.title for ws in spreadsheet.worksheets() if ws.title not in SYSTEM_SHEETS]
        visible = [clean_cell(row[0]) for row in all_values[1:] if row and clean_cell(row[0])]
        return visible if visible else [ws.title for ws in spreadsheet.worksheets() if ws.title not in SYSTEM_SHEETS]
    except Exception:
        return [ws.title for ws in spreadsheet.worksheets() if ws.title not in SYSTEM_SHEETS]

def add_visible_worksheet(spreadsheet, name: str) -> str:
    name = name.strip()
    if not name:
        return "empty"
    settings_ws = _ensure_settings_sheet(spreadsheet)
    if name in get_visible_worksheets(spreadsheet):
        return "already"
    settings_ws.append_row([name])
    return "added"

def remove_visible_worksheet(spreadsheet, name: str) -> None:
    settings_ws = _ensure_settings_sheet(spreadsheet)
    all_values  = settings_ws.get_all_values()
    for i, row in enumerate(all_values):
        if row and clean_cell(row[0]) == name:
            settings_ws.delete_rows(i + 1)
            return

def load_worksheet(ws):
    raw = ws.get_all_values()
    if not raw:
        return pd.DataFrame(), [], {}
    seen: dict[str,int] = {}
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
        row = (row + [""]*n_cols)[:n_cols]
        normalised.append(row)
    if not normalised:
        return pd.DataFrame(columns=headers), headers, {}
    df = pd.DataFrame(normalised, columns=headers)
    df = df[~(df=="").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns:
            df[sc] = ""
    df = df.fillna("").infer_objects(copy=False)
    col_map = {h:i+1 for i,h in enumerate(headers)}
    return df, headers, col_map

def ensure_system_cols(ws, headers, col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers)+1
            if new_pos > ws.col_count:
                ws.add_cols(max(4, new_pos-ws.col_count+1))
            ws.update_cell(1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map

def authenticate(email: str, password: str, users_ws) -> str | None:
    email = email.lower().strip()
    if email == "admin" and password == st.secrets.get("admin_password",""):
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

def render_login(users_ws) -> None:
    st.markdown("""<style>
      [data-testid="stSidebar"],[data-testid="collapsedControl"]{display:none!important;}
    </style>""", unsafe_allow_html=True)
    _g,c1,c2,c3,c4 = st.columns([5,.55,.55,.55,.55])
    with c1:
        if st.button("EN",key="lg_en"): st.session_state.lang="en"; st.rerun()
    with c2:
        if st.button("KU",key="lg_ku"): st.session_state.lang="ku"; st.rerun()
    with c3:
        if st.button("☀️",key="lg_lgt"): st.session_state.theme="light"; st.rerun()
    with c4:
        if st.button("🌙",key="lg_drk"): st.session_state.theme="dark"; st.rerun()
    _,mid,_ = st.columns([1,1.15,1])
    with mid:
        st.markdown(f"""
        <div class="gov-login-card">
          <div class="gov-seal-ring">🏛️</div>
          <div class="gov-ministry-band">{t('ministry')}</div>
          <div class="gov-portal-name">{t('portal_title')}</div>
          <div class="gold-rule"></div>
          <div style="text-align:center;margin-bottom:16px;">
            <span class="classification-badge">🔒 {t('classified')}</span>
          </div>
          <div class="gov-portal-tagline">{t('portal_sub')}</div>
        </div>
        """, unsafe_allow_html=True)
        with st.form("login_form", clear_on_submit=False):
            st.markdown(
                f"<p style='font-size:.76rem;color:{P['text_muted']};text-align:center;"
                f"margin-bottom:14px;'>{t('login_prompt')}</p>",
                unsafe_allow_html=True,
            )
            email_in = st.text_input(t("email_field"), placeholder="admin  ·  or  ·  auditor@mof.gov")
            pass_in  = st.text_input(t("password_field"), type="password", placeholder="••••••••")
            submitted = st.form_submit_button(f"🔐  {t('sign_in')}", use_container_width=True)
        if submitted:
            role = authenticate(email_in, pass_in, users_ws)
            if role:
                st.session_state.logged_in  = True
                st.session_state.user_email = "Admin" if role=="admin" else email_in.lower().strip()
                st.session_state.user_role  = role
                st.rerun()
            else:
                st.error(t("bad_creds"))

def render_sidebar(headers, col_binder, col_company, col_license, is_admin):
    with st.sidebar:
        st.markdown(f"""
        <div style="border-top:3px solid {P['gold']};padding:18px 16px 14px;">
          <div style="font-size:1.05rem;font-weight:700;color:{P['text_primary']};margin-bottom:2px;">
            🏛️&nbsp; {t('portal_title')}
          </div>
          <div style="font-size:.62rem;color:{P['text_muted']};letter-spacing:.13em;text-transform:uppercase;">
            {t('ministry')}
          </div>
        </div>
        <hr style="margin:0;border-color:{P['border']};"/>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown(f"<div class='sb-label'>{t('language')}</div>", unsafe_allow_html=True)
        lc1,lc2 = st.columns(2)
        if lc1.button("🇬🇧  EN", use_container_width=True, key="sb_en"):
            st.session_state.lang="en"; st.rerun()
        if lc2.button("🟡  KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang="ku"; st.rerun()
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown(f"<div class='sb-label'>{t('theme')}</div>", unsafe_allow_html=True)
        tc1,tc2 = st.columns(2)
        if tc1.button("☀️  Light", use_container_width=True, key="sb_lgt"):
            st.session_state.theme="light"; st.rerun()
        if tc2.button("🌙  Dark",  use_container_width=True, key="sb_drk"):
            st.session_state.theme="dark"; st.rerun()
        st.markdown(f"<hr style='border-color:{P['border']};margin:14px 0;'/>", unsafe_allow_html=True)
        st.markdown(f"<div class='adv-filter-header'>{t('adv_filters')}</div>", unsafe_allow_html=True)
        status_opts = {"all":t("status_all"),"pending":t("status_pending"),"done":t("status_done")}
        f_status = st.selectbox(t("f_status"), options=list(status_opts.keys()),
                                format_func=lambda k: status_opts[k], key="f_status")
        st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{t('f_email')}<span class='col-hint'> ({COL_AUDITOR})</span></div>", unsafe_allow_html=True)
        f_email = st.text_input(t("f_email"), placeholder="partial e.g. @mof", key="f_email", label_visibility="collapsed")
        b_hint = col_binder if col_binder else "not detected"
        st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{t('f_binder')}<span class='col-hint'> ({b_hint})</span></div>", unsafe_allow_html=True)
        f_binder = st.text_input(t("f_binder"), placeholder="e.g. 12345", key="f_binder", disabled=(col_binder is None), label_visibility="collapsed")
        c_hint = col_company if col_company else "not detected"
        st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{t('f_company')}<span class='col-hint'> ({c_hint})</span></div>", unsafe_allow_html=True)
        f_company = st.text_input(t("f_company"), placeholder="e.g. Al-Rasheed", key="f_company", disabled=(col_company is None), label_visibility="collapsed")
        l_hint = col_license if col_license else "not detected"
        st.markdown(f"<div class='sb-label' style='margin-top:10px;'>{t('f_license')}<span class='col-hint'> ({l_hint})</span></div>", unsafe_allow_html=True)
        f_license = st.text_input(t("f_license"), placeholder="e.g. LIC-2024", key="f_license", disabled=(col_license is None), label_visibility="collapsed")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button(f"✕  {t('clear_filters')}", use_container_width=True, key="clr_f"):
            for k in ("f_email","f_binder","f_company","f_license"):
                st.session_state[k] = ""
            st.session_state["f_status"] = "all"
            st.rerun()
        st.markdown(f"<hr style='border-color:{P['border']};margin:14px 0;'/>", unsafe_allow_html=True)
        role_label = t("role_admin") if is_admin else t("role_auditor")
        chip_cls   = "chip-admin"    if is_admin else "chip-audit"
        st.markdown(f"""
        <div class="sb-user-badge">
          <div class="sb-label">{t('signed_as')}</div>
          <div class="sb-email">{st.session_state.user_email}</div>
          <span class="chip {chip_cls}" style="margin-top:6px;">{role_label}</span>
        </div>""", unsafe_allow_html=True)
        if st.button(f"→  {t('sign_out')}", use_container_width=True, key="sb_logout"):
            for k,v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()
    return (
        st.session_state.get("f_email",""),
        st.session_state.get("f_binder",""),
        st.session_state.get("f_company",""),
        st.session_state.get("f_license",""),
        st.session_state.get("f_status","all"),
    )

def render_filter_bar(total, filtered, f_email, f_binder, f_company, f_license, f_status) -> None:
    n = _n_active(f_email, f_binder, f_company, f_license, f_status)
    if n == 0:
        return
    badges = ""
    if f_status!="all": badges+=f"<span class='filter-badge'>⚡ {f_status}</span> "
    if f_email.strip(): badges+=f"<span class='filter-badge'>📧 {f_email.strip()[:20]}</span> "
    if f_binder.strip(): badges+=f"<span class='filter-badge'>📁 {f_binder.strip()[:20]}</span> "
    if f_company.strip(): badges+=f"<span class='filter-badge'>🏢 {f_company.strip()[:20]}</span> "
    if f_license.strip(): badges+=f"<span class='filter-badge'>🪪 {f_license.strip()[:20]}</span> "
    st.markdown(f"""
    <div class="filter-result-bar">
      <span style="font-size:.70rem;font-weight:700;color:{P['gold']};text-transform:uppercase;letter-spacing:.10em;">
        {t('active_filters')} ({n})
      </span>
      {badges}
      <span class="result-count"><strong style="color:{P['gold']};">{filtered}</strong>/{total}&nbsp;{t('results_shown')}</span>
    </div>""", unsafe_allow_html=True)

def render_workspace_manager(spreadsheet) -> None:
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">🗂️ {t('ws_mgr_title')}</div>
        <div class="worklist-sub">{t('ws_mgr_sub')}</div>
      </div>
    </div>""", unsafe_allow_html=True)
    all_titles   = [ws.title for ws in spreadsheet.worksheets()]
    data_titles  = [tt for tt in all_titles if tt not in SYSTEM_SHEETS]
    visible_list = get_visible_worksheets(spreadsheet)
    hidden_list  = [tt for tt in data_titles if tt not in visible_list]
    col_a, col_b = st.columns([1.2,1], gap="large")
    with col_a:
        st.markdown(f"<div class='section-title'>✅ {t('ws_visible')}</div>", unsafe_allow_html=True)
        if not visible_list:
            st.info("No worksheets configured. All data sheets will be shown.")
        else:
            for ws_name in visible_list:
                exists    = ws_name in all_titles
                tag       = "Active" if exists else "⚠ Not Found"
                bg_c      = P['blue_bg'] if exists else P['amber_bg']
                txt_c     = P['blue_accent'] if exists else P['amber']
                st.markdown(f"""
                <div class="ws-item">
                  <span class="ws-name">📊 {ws_name}</span>
                  <span style="font-size:.60rem;font-weight:700;padding:2px 8px;border-radius:99px;
                               background:{bg_c};color:{txt_c}!important;
                               text-transform:uppercase;letter-spacing:.08em;">{tag}</span>
                </div>""", unsafe_allow_html=True)
        if visible_list:
            st.markdown(f"<div class='section-title'>🗑️ {t('ws_remove')}</div>", unsafe_allow_html=True)
            with st.form("ws_remove_form"):
                remove_sel = st.selectbox("Select worksheet to hide:", ["—"]+visible_list, key="ws_remove_sel")
                if st.form_submit_button(t("ws_remove_btn"), use_container_width=True):
                    if remove_sel != "—":
                        remove_visible_worksheet(spreadsheet, remove_sel)
                        st.success(t("ws_removed")); time.sleep(0.5); st.rerun()
    with col_b:
        st.markdown(f"<div class='section-title'>➕ {t('ws_add')}</div>", unsafe_allow_html=True)
        if hidden_list:
            st.markdown(f"<div class='sb-label'>{t('ws_available')}</div>", unsafe_allow_html=True)
            with st.form("ws_add_existing_form"):
                add_sel = st.selectbox("Available sheets:", ["—"]+hidden_list, key="ws_add_sel")
                if st.form_submit_button(t("ws_add_btn"), use_container_width=True):
                    if add_sel != "—":
                        result = add_visible_worksheet(spreadsheet, add_sel)
                        if result == "added": st.success(t("ws_added"))
                        elif result == "already": st.warning(t("ws_already"))
                        time.sleep(0.5); st.rerun()
        else:
            st.info(t("ws_none_hidden"))
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown("<div class='sb-label'>Or enter sheet name manually</div>", unsafe_allow_html=True)
        with st.form("ws_add_manual_form"):
            manual_name = st.text_input("Sheet name", placeholder="e.g. CIT_2024_Tranche5")
            if st.form_submit_button("Add by Name", use_container_width=True):
                result = add_visible_worksheet(spreadsheet, manual_name)
                if result=="added": st.success(t("ws_added"))
                elif result=="already": st.warning(t("ws_already"))
                elif result=="empty": st.error(t("fill_fields"))
                time.sleep(0.5); st.rerun()

def render_analytics(df: pd.DataFrame) -> None:
    pt=P["plotly_theme"];pb=P["plot_bg"];pg=P["plot_grid"];fc=P["text_primary"]
    st.markdown(f"<div class='section-title'>🗓️ {t('period')}</div>", unsafe_allow_html=True)
    periods=[("all",t("all_time")),("today",t("today")),("this_week",t("this_week")),("this_month",t("this_month"))]
    for cw,(pk,pl) in zip(st.columns(len(periods)),periods):
        lbl=f"✓  {pl}" if st.session_state.date_filter==pk else pl
        if cw.button(lbl, use_container_width=True, key=f"pf_{pk}"):
            st.session_state.date_filter=pk; st.rerun()
    done_base=df[df[COL_STATUS]==VAL_DONE].copy()
    done_f=apply_period_filter(done_base,COL_DATE,st.session_state.date_filter)
    if done_f.empty:
        st.info(t("no_records")); return
    ma,mb,mc=st.columns(3)
    ma.metric(t("records_period"),len(done_f))
    active=0
    if COL_DATE in done_f.columns:
        active=done_f[COL_DATE].apply(lambda s: parse_dt(s).date() if parse_dt(s) else None).nunique()
    mb.metric(t("active_days"),active)
    mc.metric(t("avg_per_day"),f"{len(done_f)/max(active,1):.1f}")
    left,right=st.columns([1,1.6],gap="large")
    with left:
        st.markdown(f"<div class='section-title'>🏅 {t('leaderboard')}</div>", unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb=(done_f[COL_AUDITOR].replace("","—").value_counts().reset_index())
            lb.columns=["Auditor","Count"]
            medals=["🥇","🥈","🥉","④","⑤","⑥","⑦","⑧","⑨","⑩"]
            for i,r in lb.head(10).iterrows():
                m=medals[i] if i<len(medals) else f"{i+1}."
                st.markdown(f"""<div class="lb-row"><span class="lb-medal">{m}</span>
                  <span class="lb-name">{r['Auditor']}</span>
                  <span class="lb-count">{r['Count']}</span></div>""", unsafe_allow_html=True)
            fig_lb=px.bar(lb.head(10),x="Count",y="Auditor",orientation="h",color="Count",
                          color_continuous_scale=[P["blue_accent"],P["gold"]],template=pt)
            fig_lb.update_layout(paper_bgcolor=pb,plot_bgcolor=pb,
                font=dict(family="IBM Plex Sans",color=fc,size=11),
                showlegend=False,coloraxis_showscale=False,
                margin=dict(l=8,r=8,t=10,b=8),
                xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=fc)),
                yaxis=dict(gridcolor="rgba(0,0,0,0)",categoryorder="total ascending",tickfont=dict(color=fc)),
                height=min(320,max(180,36*len(lb.head(10)))))
            fig_lb.update_traces(marker_line_width=0)
            st.plotly_chart(fig_lb, use_container_width=True)
    with right:
        st.markdown(f"<div class='section-title'>📈 {t('daily_trend')}</div>", unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            done_f=done_f.copy()
            done_f["_date"]=done_f[COL_DATE].apply(lambda s: parse_dt(s).date() if parse_dt(s) else None)
            trend=(done_f.dropna(subset=["_date"]).groupby("_date").size().reset_index(name="Records"))
            trend.columns=["Date","Records"]
            if not trend.empty:
                if len(trend)>1:
                    full_rng=pd.date_range(trend["Date"].min(),trend["Date"].max())
                    trend=(trend.set_index("Date").reindex(full_rng.date,fill_value=0).reset_index())
                    trend.columns=["Date","Records"]
                fig_line=go.Figure()
                fig_line.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],
                    mode="none",fill="tozeroy",fillcolor=P["gold_bg"],showlegend=False))
                fig_line.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],
                    mode="lines+markers",line=dict(color=P["gold"],width=2.5),
                    marker=dict(color=P["blue_accent"],size=7,line=dict(color=P["card"],width=2)),
                    name=t("records_period")))
                fig_line.update_layout(template=pt,paper_bgcolor=pb,plot_bgcolor=pb,
                    font=dict(family="IBM Plex Sans",color=fc,size=11),
                    showlegend=False,margin=dict(l=8,r=8,t=10,b=8),
                    xaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=P["text_secondary"])),
                    yaxis=dict(gridcolor=pg,zeroline=False,tickfont=dict(color=P["text_secondary"])),
                    height=380,hovermode="x unified")
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info(t("no_records"))

def render_user_admin(users_ws) -> None:
    col_left,col_right=st.columns([1,1],gap="large")
    with col_left:
        st.markdown(f"<div class='section-title'>➕ {t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_email=st.text_input("Email",placeholder="auditor@mof.gov")
            nu_pass=st.text_input("Password",type="password")
            if st.form_submit_button("Register Auditor",use_container_width=True):
                if nu_email.strip() and nu_pass.strip():
                    recs=pd.DataFrame(users_ws.get_all_records())
                    already=(not recs.empty and nu_email.lower().strip() in recs.get("email",pd.Series()).values)
                    if already: st.error(t("dup_email"))
                    else:
                        users_ws.append_row([nu_email.lower().strip(),hash_pw(nu_pass.strip()),now_str()])
                        st.success(f"✅  {nu_email} registered."); time.sleep(0.7); st.rerun()
                else: st.warning(t("fill_fields"))
        st.markdown(f"<div class='section-title'>🔑 {t('update_pw')}</div>", unsafe_allow_html=True)
        staff_df=pd.DataFrame(users_ws.get_all_records())
        if not staff_df.empty and "email" in staff_df.columns:
            with st.form("upd_pw_form"):
                sel_email=st.selectbox("Select staff",staff_df["email"].tolist())
                new_pw=st.text_input("New Password",type="password")
                if st.form_submit_button("Update Password",use_container_width=True):
                    if new_pw.strip():
                        cell=users_ws.find(sel_email)
                        if cell:
                            users_ws.update_cell(cell.row,2,hash_pw(new_pw.strip()))
                            st.success(f"✅  Updated for {sel_email}."); time.sleep(0.7); st.rerun()
    with col_right:
        st.markdown(f"<div class='section-title'>📋 {t('staff_dir')}</div>", unsafe_allow_html=True)
        staff_df=pd.DataFrame(users_ws.get_all_records())
        if not staff_df.empty and "email" in staff_df.columns:
            safe_cols=[c for c in ["email","created_at"] if c in staff_df.columns]
            render_html_table(staff_df[safe_cols].reset_index())
            st.markdown(f"<div class='section-title'>🚫 {t('remove_user')}</div>", unsafe_allow_html=True)
            del_email=st.selectbox("Select account to revoke",["—"]+staff_df["email"].tolist(),key="del_sel")
            if del_email!="—":
                if st.button(f"Revoke — {del_email}",key="del_btn"):
                    cell=users_ws.find(del_email)
                    if cell:
                        users_ws.delete_rows(cell.row)
                        st.success(f"✅  {del_email} revoked."); time.sleep(0.7); st.rerun()
        else: st.info("No auditor accounts registered.")

def render_worklist(pending_display, df, headers, col_map, current_ws,
                    f_email, f_binder, f_company, f_license, f_status) -> None:
    p_count=len(pending_display)
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">📋 {t('worklist_title')}</div>
        <div class="worklist-sub">{t('worklist_sub')}</div>
      </div>
      <span class="chip chip-pending">{p_count} {t('outstanding')}</span>
    </div>""", unsafe_allow_html=True)
    if pending_display.empty:
        msg=t("no_match") if _n_active(f_email,f_binder,f_company,f_license,f_status) else "✅  All cases have been processed."
        st.info(msg); return
    render_html_table(pending_display)
    st.markdown(f"<div class='section-title'>🔍 {t('select_case')}</div>", unsafe_allow_html=True)
    label_col=next((h for h in headers if h not in SYSTEM_COLS), headers[0] if headers else "Row")
    opts=["—"]+[f"Row {idx}  ·  {str(row.get(label_col,''))[:55]}" for idx,row in pending_display.iterrows()]
    row_sel=st.selectbox("",opts,key="row_sel",label_visibility="collapsed")
    if row_sel=="—": return
    sheet_row=int(row_sel.split("  ·  ")[0].replace("Row ","").strip())
    df_iloc=sheet_row-2
    if df_iloc<0 or df_iloc>=len(df):
        st.error("Row index out of range — please refresh."); return
    record=df.iloc[df_iloc].to_dict()
    with st.expander(f"📜  {t('audit_trail')}", expanded=False):
        history=str(record.get(COL_LOG,"")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip():
                    st.markdown(f'<div class="log-line">{line}</div>', unsafe_allow_html=True)
        else: st.caption(t("no_history"))
    st.markdown(f"<div class='section-title'>✏️ {t('processing')} #{sheet_row}</div>", unsafe_allow_html=True)
    SKIP=set(SYSTEM_COLS)
    fields={k:v for k,v in record.items() if k not in SKIP}
    with st.form("audit_form"):
        new_vals: dict[str,str]={}
        for fname,fval in fields.items():
            new_vals[fname]=st.text_input(fname,value=clean_cell(fval),key=f"field_{fname}")
        do_submit=st.form_submit_button(f"✅  {t('approve_save')}", use_container_width=True)
    if do_submit:
        with st.spinner("Committing to register…"):
            headers_u,col_map_u=ensure_system_cols(current_ws,headers,col_map)
            for fname,fval in new_vals.items():
                if fname in col_map_u and clean_cell(record.get(fname,""))!=fval:
                    current_ws.update_cell(sheet_row,col_map_u[fname],fval)
            ts_now=now_str()
            auditor=st.session_state.user_email
            old_log=str(record.get(COL_LOG,"")).strip()
            new_log=f"✔  {auditor}  |  {ts_now}\n{old_log}".strip()
            current_ws.update_cell(sheet_row,col_map_u[COL_STATUS],VAL_DONE)
            current_ws.update_cell(sheet_row,col_map_u[COL_LOG],new_log)
            current_ws.update_cell(sheet_row,col_map_u[COL_AUDITOR],auditor)
            current_ws.update_cell(sheet_row,col_map_u[COL_DATE],ts_now)
        st.success(t("saved_ok")); time.sleep(0.8); st.rerun()

def render_archive(done_view, col_map, current_ws, is_admin,
                   f_email, f_binder, f_company, f_license, f_status) -> None:
    d_count=len(done_view)
    st.markdown(f"""
    <div class="worklist-header">
      <div>
        <div class="worklist-title">✅ Processed Archive</div>
        <div class="worklist-sub">Completed and committed audit records</div>
      </div>
      <span class="chip chip-done">{d_count} {t('processed')}</span>
    </div>""", unsafe_allow_html=True)
    if done_view.empty:
        msg=t("no_match") if _n_active(f_email,f_binder,f_company,f_license,f_status) else "No processed records yet."
        st.info(msg)
    else:
        render_html_table(done_view)
    if is_admin and not done_view.empty:
        st.markdown("---")
        st.markdown(f"<div class='section-title'>↩️ {t('reopen')}</div>", unsafe_allow_html=True)
        reopen_opts=["—"]+[f"Row {idx}" for idx in done_view.index]
        reopen_sel=st.selectbox("Select record to re-open:",reopen_opts,key="reopen_sel")
        if reopen_sel!="—":
            ridx=int(reopen_sel.split(" ")[1])
            if st.button(t("reopen"),key="reopen_btn"):
                if COL_STATUS in col_map:
                    current_ws.update_cell(ridx,col_map[COL_STATUS],VAL_PENDING); st.rerun()

def main() -> None:
    try:
        spreadsheet   = get_spreadsheet()
        all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]
        if USERS_SHEET not in all_ws_titles:
            uw=spreadsheet.add_worksheet(title=USERS_SHEET,rows="500",cols="3")
            uw.append_row(["email","password","created_at"])
        users_ws=spreadsheet.worksheet(USERS_SHEET)
        _ensure_settings_sheet(spreadsheet)

        if not st.session_state.logged_in:
            render_login(users_ws); return

        st.markdown("<style>[data-testid='stSidebar']{display:flex!important;}</style>",
                    unsafe_allow_html=True)
        is_admin=st.session_state.user_role=="admin"
        visible_ws_names=get_visible_worksheets(spreadsheet)
        ts_str=datetime.now(TZ).strftime("%A, %d %B %Y  ·  %H:%M")
        st.markdown(f"""
        <div class="page-title">🏛️  {t('portal_title')}</div>
        <div class="page-sub">{ts_str}</div>""", unsafe_allow_html=True)

        if not visible_ws_names:
            st.warning("No worksheets are configured. Use the Workspace Manager tab to add sheets.")
            df,headers,col_map,current_ws=pd.DataFrame(),[],{},None
            ws_name=None
        else:
            ws_name=st.selectbox(t("workspace"),visible_ws_names,key="ws_sel")
            try:
                current_ws=spreadsheet.worksheet(ws_name)
                df,headers,col_map=load_worksheet(current_ws)
            except gspread.exceptions.WorksheetNotFound:
                st.error(f"Worksheet '{ws_name}' not found.")
                df,headers,col_map,current_ws=pd.DataFrame(),[],{},None

        col_binder  = detect_column(headers,"binder")
        col_company = detect_column(headers,"company")
        col_license = detect_column(headers,"license")

        f_email,f_binder,f_company,f_license,f_status=render_sidebar(
            headers,col_binder,col_company,col_license,is_admin)

        if not df.empty:
            st.markdown(f"<div class='section-title'>📊 {t('overview')}</div>", unsafe_allow_html=True)
            total_n=len(df)
            done_n=int((df[COL_STATUS]==VAL_DONE).sum())
            pending_n=total_n-done_n
            pct=done_n/total_n if total_n else 0
            m1,m2,m3=st.columns(3)
            m1.metric(t("total"),total_n)
            m2.metric(t("processed"),done_n,delta=f"{int(pct*100)}%")
            m3.metric(t("outstanding"),pending_n,delta=f"{100-int(pct*100)}% remaining",delta_color="inverse")
            st.markdown(f"""
            <div class="prog-labels"><span>{t('processed')}</span><span>{int(pct*100)}%</span></div>
            <div class="gov-progress-wrap">
              <div class="gov-progress-fill" style="width:{int(pct*100)}%;"></div>
            </div>""", unsafe_allow_html=True)
            filtered_df=apply_filters(df,f_email,f_binder,f_company,f_license,f_status,
                                      col_binder,col_company,col_license)
            render_filter_bar(total_n,len(filtered_df),f_email,f_binder,f_company,f_license,f_status)
        else:
            filtered_df=pd.DataFrame()

        # ── Role-based tab construction ───────────────────────────────────────
        if is_admin:
            tab_labels=[t("tab_worklist"),t("tab_archive"),t("tab_analytics"),t("tab_ws_mgr"),t("tab_users")]
            tabs=st.tabs(tab_labels)
            t_work,t_arch,t_anal,t_wsmgr,t_uadm=tabs
        else:
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>", unsafe_allow_html=True)
            tabs=st.tabs([t("tab_worklist"),t("tab_archive")])
            t_work,t_arch=tabs
            t_anal=t_wsmgr=t_uadm=None

        with t_work:
            if df.empty or current_ws is None:
                st.warning(t("empty_sheet"))
            else:
                pending_view=filtered_df[filtered_df[COL_STATUS]!=VAL_DONE].copy()
                pending_display=pending_view.copy()
                pending_display.index=pending_display.index+2
                render_worklist(pending_display,df,headers,col_map,current_ws,
                                f_email,f_binder,f_company,f_license,f_status)
        with t_arch:
            if df.empty or current_ws is None:
                st.warning(t("empty_sheet"))
            else:
                done_view=filtered_df[filtered_df[COL_STATUS]==VAL_DONE].copy()
                done_view.index=done_view.index+2
                render_archive(done_view,col_map,current_ws,is_admin,
                               f_email,f_binder,f_company,f_license,f_status)
        if is_admin and t_anal is not None:
            with t_anal:
                if df.empty: st.warning(t("empty_sheet"))
                else: render_analytics(df)
        if is_admin and t_wsmgr is not None:
            with t_wsmgr:
                render_workspace_manager(spreadsheet)
        if is_admin and t_uadm is not None:
            with t_uadm:
                render_user_admin(users_ws)

    except Exception as exc:
        st.error(f"🚨  System Error: {exc}")
        with st.expander("Technical Details", expanded=False):
            st.exception(exc)

if __name__ == "__main__":
    main()
