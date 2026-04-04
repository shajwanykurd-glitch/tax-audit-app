# =============================================================================
#  GOVERNMENT AUDIT PRO · v3.0
#  Full-featured Streamlit + Google Sheets Audit Platform
#  Requirements: streamlit, gspread, oauth2client, pandas, plotly, pytz
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
# 0 · PAGE CONFIG  (must be first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Audit Pro",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="collapsed",   # hidden on login page
)

# ─────────────────────────────────────────────────────────────────────────────
# 1 · SESSION STATE DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULTS = dict(
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

TZ = pytz.timezone("Asia/Baghdad")

# ─────────────────────────────────────────────────────────────────────────────
# 2 · THEME ENGINE
# ─────────────────────────────────────────────────────────────────────────────
_PALETTES = {
    "dark": {
        "page_bg":        "#07090F",
        "surface":        "#0E1420",
        "surface2":       "#131926",
        "card":           "#141C2B",
        "border":         "#1E2D45",
        "text":           "#D9E3F5",
        "subtext":        "#6B7FA3",
        "accent":         "#4F8EF7",
        "accent_glow":    "rgba(79,142,247,0.18)",
        "green":          "#22C55E",
        "amber":          "#F59E0B",
        "red":            "#F87171",
        "input_bg":       "#0A0F1A",
        "btn_bg":         "#4F8EF7",
        "btn_text":       "#FFFFFF",
        "progress_track": "#1E2D45",
        "plotly_theme":   "plotly_dark",
    },
    "light": {
        "page_bg":        "#F1F5FC",
        "surface":        "#FFFFFF",
        "surface2":       "#F8FAFF",
        "card":           "#FFFFFF",
        "border":         "#DDE5F5",
        "text":           "#0F172A",
        "subtext":        "#64748B",
        "accent":         "#2563EB",
        "accent_glow":    "rgba(37,99,235,0.12)",
        "green":          "#16A34A",
        "amber":          "#D97706",
        "red":            "#DC2626",
        "input_bg":       "#FFFFFF",
        "btn_bg":         "#2563EB",
        "btn_text":       "#FFFFFF",
        "progress_track": "#E2EAF8",
        "plotly_theme":   "plotly_white",
    },
}

P = _PALETTES[st.session_state.theme]

# ─────────────────────────────────────────────────────────────────────────────
# 3 · CSS INJECTION
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
/* ── Google Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

/* ── CSS Variables ── */
:root {{
  --page-bg:        {P['page_bg']};
  --surface:        {P['surface']};
  --surface2:       {P['surface2']};
  --card:           {P['card']};
  --border:         {P['border']};
  --text:           {P['text']};
  --subtext:        {P['subtext']};
  --accent:         {P['accent']};
  --accent-glow:    {P['accent_glow']};
  --green:          {P['green']};
  --amber:          {P['amber']};
  --red:            {P['red']};
  --input-bg:       {P['input_bg']};
  --btn-bg:         {P['btn_bg']};
  --btn-text:       {P['btn_text']};
  --progress-track: {P['progress_track']};
}}

/* ── Global ── */
*, *::before, *::after {{ box-sizing: border-box; }}
html, body, .stApp {{
  font-family: 'Plus Jakarta Sans', sans-serif !important;
  background-color: var(--page-bg) !important;
  color: var(--text) !important;
}}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header, .stDeployButton {{ display: none !important; }}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
  background: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
}}
[data-testid="stSidebar"] * {{ color: var(--text) !important; }}
[data-testid="stSidebarCollapseButton"] {{ display: none !important; }}

/* ── Metric Cards ── */
[data-testid="stMetricContainer"] {{
  background: var(--card) !important;
  border: 1px solid var(--border) !important;
  border-radius: 18px !important;
  padding: 22px 24px !important;
  box-shadow: 0 2px 12px rgba(0,0,0,0.10) !important;
  transition: transform 0.22s cubic-bezier(.34,1.56,.64,1),
              box-shadow 0.22s ease !important;
  cursor: default;
}}
[data-testid="stMetricContainer"]:hover {{
  transform: translateY(-4px) !important;
  box-shadow: 0 10px 30px var(--accent-glow) !important;
}}
[data-testid="stMetricValue"] {{
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 2.4rem !important;
  font-weight: 600 !important;
  color: var(--accent) !important;
}}
[data-testid="stMetricLabel"] {{
  font-size: 0.72rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.1em !important;
  text-transform: uppercase !important;
  color: var(--subtext) !important;
}}
[data-testid="stMetricDelta"] {{ font-size: 0.82rem !important; }}

/* ── Forms ── */
div[data-testid="stForm"] {{
  background: var(--card) !important;
  border: 1px solid var(--border) !important;
  border-radius: 20px !important;
  padding: 28px 32px !important;
}}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div > div {{
  background: var(--input-bg) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
  font-family: 'Plus Jakarta Sans', sans-serif !important;
  font-size: 0.9rem !important;
  transition: border-color 0.18s ease, box-shadow 0.18s ease !important;
}}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {{
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px var(--accent-glow) !important;
  outline: none !important;
}}
label, .stTextInput label, .stSelectbox label {{
  color: var(--subtext) !important;
  font-size: 0.78rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.07em !important;
  text-transform: uppercase !important;
}}

/* ── Buttons ── */
.stButton > button {{
  background: var(--btn-bg) !important;
  color: var(--btn-text) !important;
  border: none !important;
  border-radius: 10px !important;
  font-weight: 700 !important;
  font-size: 0.88rem !important;
  letter-spacing: 0.03em !important;
  padding: 10px 20px !important;
  transition: opacity 0.15s ease, transform 0.15s ease !important;
}}
.stButton > button:hover {{
  opacity: 0.87 !important;
  transform: translateY(-1px) !important;
}}
.stButton > button:active {{
  transform: translateY(0) !important;
}}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {{
  gap: 4px !important;
  background: transparent !important;
  border-bottom: 2px solid var(--border) !important;
}}
.stTabs [data-baseweb="tab"] {{
  background: transparent !important;
  color: var(--subtext) !important;
  border-radius: 10px 10px 0 0 !important;
  border: 1px solid transparent !important;
  border-bottom: none !important;
  padding: 10px 24px !important;
  font-weight: 600 !important;
  font-size: 0.88rem !important;
  transition: color 0.15s ease, background 0.15s ease !important;
}}
.stTabs [data-baseweb="tab"]:hover {{
  color: var(--text) !important;
}}
.stTabs [aria-selected="true"] {{
  background: var(--card) !important;
  color: var(--accent) !important;
  border-color: var(--border) !important;
  border-bottom-color: var(--card) !important;
  margin-bottom: -2px !important;
}}

/* ── DataFrames ── */
[data-testid="stDataFrame"] {{
  border: 1px solid var(--border) !important;
  border-radius: 14px !important;
  overflow: hidden !important;
}}
.dvn-scroller {{ background: var(--card) !important; }}

/* ── Expander ── */
.streamlit-expanderHeader {{
  background: var(--surface2) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
}}

/* ── Divider ── */
hr {{ border-color: var(--border) !important; opacity: 0.6 !important; }}

/* ── Custom Components ── */
.page-title {{
  font-size: 1.75rem;
  font-weight: 800;
  color: var(--text);
  letter-spacing: -0.02em;
  margin-bottom: 2px;
}}
.page-subtitle {{
  font-size: 0.88rem;
  color: var(--subtext);
  margin-bottom: 28px;
}}
.section-title {{
  font-size: 0.95rem;
  font-weight: 700;
  color: var(--text);
  letter-spacing: -0.01em;
  border-left: 3px solid var(--accent);
  padding-left: 10px;
  margin: 24px 0 14px;
}}
.info-chip {{
  display: inline-block;
  padding: 3px 10px;
  border-radius: 99px;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}}
.chip-done    {{ background: rgba(34,197,94,0.12); color: {P['green']}; }}
.chip-pending {{ background: rgba(245,158,11,0.12); color: {P['amber']}; }}
.chip-admin   {{ background: rgba(79,142,247,0.15); color: {P['accent']}; }}

.progress-wrap {{
  background: var(--progress-track);
  border-radius: 99px;
  height: 8px;
  overflow: hidden;
  margin: 6px 0 14px;
}}
.progress-fill {{
  height: 100%;
  border-radius: 99px;
  background: linear-gradient(90deg, var(--accent), var(--green));
  transition: width 0.7s cubic-bezier(.4,0,.2,1);
}}
.progress-label {{
  display: flex;
  justify-content: space-between;
  font-size: 0.75rem;
  color: var(--subtext);
  font-weight: 600;
}}

.lb-item {{
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 12px 18px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 12px;
  margin-bottom: 8px;
  transition: transform 0.18s ease;
}}
.lb-item:hover {{ transform: translateX(4px); }}
.lb-rank {{ font-family:'JetBrains Mono',monospace; font-weight:700; width:28px; text-align:center; color:var(--amber); }}
.lb-email {{ flex:1; font-weight:600; font-size:0.9rem; }}
.lb-count {{ font-family:'JetBrains Mono',monospace; color:var(--accent); font-weight:700; font-size:0.95rem; }}

.login-card {{
  max-width: 440px;
  margin: 0 auto;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 24px;
  padding: 44px 40px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.18);
}}
.login-logo {{
  font-size: 2.5rem;
  text-align: center;
  margin-bottom: 6px;
}}
.login-title {{
  font-size: 1.5rem;
  font-weight: 800;
  text-align: center;
  color: var(--text);
  letter-spacing: -0.02em;
  margin-bottom: 4px;
}}
.login-sub {{
  font-size: 0.82rem;
  text-align: center;
  color: var(--subtext);
  margin-bottom: 30px;
}}
.user-badge {{
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 12px 16px;
}}
.audit-field-label {{
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--subtext);
  margin-bottom: 2px;
}}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# 4 · TRANSLATIONS
# ─────────────────────────────────────────────────────────────────────────────
_LANG: dict[str, dict[str, str]] = {
    "en": {
        "app_name":       "Audit Pro",
        "app_tagline":    "Government Data Audit & Analytics Platform",
        "login_title":    "Secure Login",
        "login_sub":      "Enter your credentials to access the platform",
        "email":          "Email Address",
        "password":       "Password",
        "login_btn":      "Sign In",
        "logout_btn":     "Sign Out",
        "theme":          "Theme",
        "language":       "Language",
        "workspace":      "Select Workspace",
        "stats_title":    "Overview",
        "total":          "Total Records",
        "completed":      "Completed",
        "pending":        "Pending",
        "search":         "Search (company, license, email…)",
        "tab_queue":      "Task Queue",
        "tab_archive":    "Archive",
        "tab_analytics":  "Analytics",
        "tab_users":      "User Management",
        "select_record":  "Select a record to audit",
        "audit_history":  "Audit Trail",
        "submit":         "Approve & Save",
        "return_pending": "Return to Pending",
        "leaderboard":    "Leaderboard",
        "daily_trend":    "Daily Trend",
        "time_filter":    "Time Period",
        "today":          "Today",
        "this_week":      "This Week",
        "this_month":     "This Month",
        "all_time":       "All Time",
        "add_auditor":    "Add Auditor",
        "update_pass":    "Update Password",
        "remove_user":    "Remove User",
        "staff_list":     "Staff Directory",
        "no_data":        "No records found for this period.",
        "empty_sheet":    "This worksheet is empty.",
        "saved_ok":       "Record approved and saved successfully.",
        "invalid_creds":  "Invalid email or password. Please try again.",
        "duplicate_email":"This email is already registered.",
        "fill_all":       "Please fill in all fields.",
        "signed_in_as":   "Signed in as",
        "role_admin":     "Administrator",
        "role_auditor":   "Auditor",
        "processing":     "Processing Row",
        "filter_active":  "Filter active:",
    },
    "ku": {
        "app_name":       "ئۆدیت پرۆ",
        "app_tagline":    "پلاتفۆرمی وردبینی و ئەنالیتیکسی حکومی",
        "login_title":    "چوونەژوورەوەی پارێزراو",
        "login_sub":      "زانیارییەکانت بنووسە بۆ چوونەژوورەوە",
        "email":          "ئیمەیڵ",
        "password":       "پاسۆرد",
        "login_btn":      "چوونەژوورەوە",
        "logout_btn":     "چوونەدەرەوە",
        "theme":          "تیمی سایت",
        "language":       "زمان",
        "workspace":      "شیتەکە هەڵبژێرە",
        "stats_title":    "کورتەی گشتی",
        "total":          "کۆی گشتی",
        "completed":      "تەواوکراوە",
        "pending":        "چاوەڕوان",
        "search":         "گەڕان (کۆمپانیا، مۆڵەت، ئیمەیڵ…)",
        "tab_queue":      "ڕیزی کارەکان",
        "tab_archive":    "ئەرشیف",
        "tab_analytics":  "ئەنالیتیکس",
        "tab_users":      "بەڕێوەبردنی کارمەند",
        "select_record":  "ڕیزێک هەڵبژێرە بۆ پشکنین",
        "audit_history":  "مێژووی گۆڕانکاری",
        "submit":         "پەسەندکردن و پاشەکەوتکردن",
        "return_pending": "گەڕاندنەوە بۆ چاوەڕوان",
        "leaderboard":    "تەختەی پێشکەوتن",
        "daily_trend":    "ترەندی ڕۆژانە",
        "time_filter":    "ماوەی کات",
        "today":          "ئەمڕۆ",
        "this_week":      "ئەم هەفتەیە",
        "this_month":     "ئەم مانگەیە",
        "all_time":       "هەموو کات",
        "add_auditor":    "زیادکردنی ئۆدیتۆر",
        "update_pass":    "گۆڕینی پاسۆرد",
        "remove_user":    "سڕینەوەی بەکارهێنەر",
        "staff_list":     "لیستی کارمەندان",
        "no_data":        "هیچ داتایەک نییە بۆ ئەم ماوەیە.",
        "empty_sheet":    "ئەم شیتە بەتاڵە.",
        "saved_ok":       "تۆمار پەسەندکرا و پاشەکەوت کرا.",
        "invalid_creds":  "ئیمەیڵ یان پاسۆرد هەڵەیە.",
        "duplicate_email":"ئەم ئیمەیڵە پێشتر تۆمارکراوە.",
        "fill_all":       "تکایە هەموو خانەکان پڕبکەوە.",
        "signed_in_as":   "چووییتە ژوورەوە بەناوی",
        "role_admin":     "بەڕێوەبەر",
        "role_auditor":   "ئۆدیتۆر",
        "processing":     "ڕیزی",
        "filter_active":  "فیلتەری چالاک:",
    },
}

def t(key: str) -> str:
    return _LANG[st.session_state.lang].get(key, key)

# ─────────────────────────────────────────────────────────────────────────────
# 5 · SYSTEM COLUMN NAMES
# ─────────────────────────────────────────────────────────────────────────────
COL_STATUS   = "Status"
COL_LOG      = "Audit_Log"
COL_AUDITOR  = "Auditor_Email"
COL_DATE     = "Submission_Date"
SYSTEM_COLS  = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE]
VAL_DONE     = "Completed"
VAL_PENDING  = "Pending"

# ─────────────────────────────────────────────────────────────────────────────
# 6 · UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()


def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_date(s: str) -> datetime | None:
    """Safely parse a date string in YYYY-MM-DD HH:MM:SS format."""
    try:
        return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except Exception:
        return None


def apply_period_filter(df: pd.DataFrame, col: str, period: str) -> pd.DataFrame:
    """Return rows whose `col` date falls within `period`."""
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
    dates = df[col].apply(parse_date)
    return df[dates >= cutoff]


# ─────────────────────────────────────────────────────────────────────────────
# 7 · GOOGLE SHEETS CONNECTION
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    """Authenticate with Google Sheets and return the spreadsheet object."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    # Rebuild private key: strip headers, collapse whitespace, rewrap at 64 chars
    pk = key_dict["private_key"]
    pk = pk.replace("-----BEGIN PRIVATE KEY-----", "")
    pk = pk.replace("-----END PRIVATE KEY-----", "")
    pk = pk.replace("\\n", "").replace("\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = (
        f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    )
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client.open("site CIT QA - Tranche 4")


# ─────────────────────────────────────────────────────────────────────────────
# 8 · ROBUST DATA LOADING  (Critical Fix)
# ─────────────────────────────────────────────────────────────────────────────
def load_worksheet(ws) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    """
    Safely load a worksheet into a DataFrame.

    Returns
    -------
    df        : cleaned DataFrame (empty rows removed, system cols ensured)
    headers   : ordered list of column names (matches sheet columns)
    col_map   : {column_name: 1-based column index in Google Sheets}
    """
    raw = ws.get_all_values()

    # ── No data at all ──────────────────────────────────────
    if not raw:
        return pd.DataFrame(), [], {}

    # ── Normalise header row ─────────────────────────────────
    raw_headers = raw[0]
    headers: list[str] = []
    seen: dict[str, int] = {}
    for h in raw_headers:
        h = str(h).strip() or "Unnamed"
        if h in seen:
            seen[h] += 1
            headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers.append(h)

    # ── Build DataFrame from data rows ──────────────────────
    data_rows = raw[1:]

    # Pad / trim each row to match header length (prevents shape mismatch)
    n_cols = len(headers)
    normalised: list[list] = []
    for row in data_rows:
        row = list(row)
        if len(row) < n_cols:
            row += [""] * (n_cols - len(row))
        else:
            row = row[:n_cols]
        normalised.append(row)

    if not normalised:
        return pd.DataFrame(columns=headers), headers, {}

    df = pd.DataFrame(normalised, columns=headers)

    # ── Drop fully empty rows ────────────────────────────────
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.replace(pd.NA, "", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── Inject missing system columns (in-memory) ────────────
    for sc in SYSTEM_COLS:
        if sc not in df.columns:
            df[sc] = ""

    # ── Build col_map using original sheet headers ───────────
    col_map = {h: i + 1 for i, h in enumerate(headers)}

    return df, headers, col_map


def ensure_system_cols_in_sheet(
    ws,
    headers: list[str],
    col_map: dict[str, int],
) -> tuple[list[str], dict[str, int]]:
    """
    Guarantee that every SYSTEM_COL exists in the actual Google Sheet.
    Expands the grid first if the new column would exceed current bounds.
    Returns updated (headers, col_map).
    """
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers) + 1
            # Grid limit protection: add columns if needed
            if new_pos > ws.col_count:
                ws.add_cols(max(4, new_pos - ws.col_count + 1))
            ws.update_cell(1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map


# ─────────────────────────────────────────────────────────────────────────────
# 9 · AUTHENTICATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def check_login(email: str, password: str, users_ws) -> str | None:
    """
    Validate credentials. Returns role string ("admin"/"auditor") or None.
    """
    email = email.lower().strip()
    # Admin check (no Google Sheets lookup needed)
    if email == "admin" and password == st.secrets.get("admin_password", ""):
        return "admin"
    # Auditor lookup in UsersDB sheet
    try:
        records = users_ws.get_all_records()
        df_u = pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns:
            return None
        match = df_u[df_u["email"] == email]
        if match.empty:
            return None
        stored = str(match["password"].values[0])
        if hash_pw(password) == stored:
            return "auditor"
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 10 · LOGIN PAGE  (shown before any app content)
# ─────────────────────────────────────────────────────────────────────────────
def render_login_page(users_ws):
    """Full-page, centered login card. Blocks until authenticated."""

    # Collapse sidebar completely on login page
    st.markdown(
        "<style>[data-testid='stSidebar']{display:none!important;}"
        "[data-testid='collapsedControl']{display:none!important;}</style>",
        unsafe_allow_html=True,
    )

    # Theme / language toggles in top-right corner
    tcol1, tcol2, tcol3, tcol4 = st.columns([6, 1, 1, 1])
    with tcol2:
        if st.button("EN", key="login_en"):
            st.session_state.lang = "en"; st.rerun()
    with tcol3:
        if st.button("KU", key="login_ku"):
            st.session_state.lang = "ku"; st.rerun()
    with tcol4:
        icon = "☀️" if st.session_state.theme == "dark" else "🌙"
        if st.button(icon, key="login_theme"):
            st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
            st.rerun()

    # Vertically centre the card
    st.markdown("<div style='height:60px'></div>", unsafe_allow_html=True)

    _, center_col, _ = st.columns([1, 1.2, 1])
    with center_col:
        st.markdown(f"""
        <div class="login-card">
          <div class="login-logo">⚖️</div>
          <div class="login-title">{t('login_title')}</div>
          <div class="login-sub">{t('login_sub')}</div>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            email_in = st.text_input(t("email"), placeholder="admin  ·  or  ·  your@email.com")
            pass_in  = st.text_input(t("password"), type="password", placeholder="••••••••")
            submitted = st.form_submit_button(
                t("login_btn"), use_container_width=True
            )

        if submitted:
            role = check_login(email_in, pass_in, users_ws)
            if role:
                st.session_state.logged_in  = True
                st.session_state.user_email = "Admin" if role == "admin" else email_in.lower().strip()
                st.session_state.user_role  = role
                st.rerun()
            else:
                st.error(t("invalid_creds"))


# ─────────────────────────────────────────────────────────────────────────────
# 11 · SIDEBAR (shown only when logged in)
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        # App brand
        st.markdown(f"""
        <div style='padding:10px 0 18px;'>
          <div style='font-size:1.25rem;font-weight:800;letter-spacing:-0.02em;'>
            ⚖️ {t('app_name')}
          </div>
          <div style='font-size:0.72rem;color:{P['subtext']};margin-top:2px;'>
            {t('app_tagline')}
          </div>
        </div>
        """, unsafe_allow_html=True)
        st.divider()

        # Language
        st.markdown(f"<div style='font-size:0.7rem;font-weight:700;color:{P['subtext']};letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px;'>{t('language')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧 EN", use_container_width=True, key="sb_en"):
            st.session_state.lang = "en"; st.rerun()
        if lc2.button("🟡 KU", use_container_width=True, key="sb_ku"):
            st.session_state.lang = "ku"; st.rerun()
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Theme
        st.markdown(f"<div style='font-size:0.7rem;font-weight:700;color:{P['subtext']};letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px;'>{t('theme')}</div>", unsafe_allow_html=True)
        tc1, tc2 = st.columns(2)
        if tc1.button("☀️ Light", use_container_width=True, key="sb_light"):
            st.session_state.theme = "light"; st.rerun()
        if tc2.button("🌙 Dark", use_container_width=True, key="sb_dark"):
            st.session_state.theme = "dark"; st.rerun()
        st.divider()

        # User badge
        role_label = t("role_admin") if st.session_state.user_role == "admin" else t("role_auditor")
        chip_cls   = "chip-admin" if st.session_state.user_role == "admin" else ""
        st.markdown(f"""
        <div class="user-badge">
          <div style='font-size:0.68rem;font-weight:700;color:{P['subtext']};
                      letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;'>
            {t('signed_in_as')}
          </div>
          <div style='font-size:0.92rem;font-weight:700;'>{st.session_state.user_email}</div>
          <span class="info-chip {chip_cls}" style="margin-top:6px;display:inline-block;">
            {role_label}
          </span>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button(t("logout_btn"), use_container_width=True, key="sb_logout"):
            for k, v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 12 · ANALYTICS TAB
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics(df: pd.DataFrame):
    """Render leaderboard + daily trend with period filtering."""
    pt = P["plotly_theme"]
    pb = P["card"]

    # ── Period filter buttons ────────────────────────────────
    st.markdown(f"<div class='section-title'>{t('time_filter')}</div>", unsafe_allow_html=True)
    periods = ["all", "today", "this_week", "this_month"]
    btn_cols = st.columns(len(periods))
    for col_w, pkey in zip(btn_cols, periods):
        label = t(pkey)
        if st.session_state.date_filter == pkey:
            label = f"✓ {label}"
        if col_w.button(label, use_container_width=True, key=f"pf_{pkey}"):
            st.session_state.date_filter = pkey
            st.rerun()

    # Apply filter to completed records only
    done_all = df[df[COL_STATUS] == VAL_DONE].copy()
    done_f   = apply_period_filter(done_all, COL_DATE, st.session_state.date_filter)

    if done_f.empty:
        st.info(t("no_data"))
        return

    left_col, right_col = st.columns([1, 1.5], gap="large")

    # ── Leaderboard ─────────────────────────────────────────
    with left_col:
        st.markdown(f"<div class='section-title'>{t('leaderboard')}</div>", unsafe_allow_html=True)

        if COL_AUDITOR in done_f.columns:
            lb = (
                done_f[COL_AUDITOR]
                .replace("", "Unknown")
                .value_counts()
                .reset_index()
            )
            lb.columns = ["Auditor", "Count"]

            medals = ["🥇", "🥈", "🥉", "④", "⑤", "⑥", "⑦", "⑧"]
            for idx, row in lb.head(8).iterrows():
                medal = medals[idx] if idx < len(medals) else f"{idx+1}."
                st.markdown(f"""
                <div class="lb-item">
                  <span class="lb-rank">{medal}</span>
                  <span class="lb-email">{row['Auditor']}</span>
                  <span class="lb-count">{row['Count']}</span>
                </div>
                """, unsafe_allow_html=True)

            # Horizontal bar chart
            fig_lb = px.bar(
                lb.head(10),
                x="Count", y="Auditor",
                orientation="h",
                color="Count",
                color_continuous_scale=[P["accent"], P["green"]],
                template=pt,
            )
            fig_lb.update_layout(
                paper_bgcolor=pb, plot_bgcolor=pb,
                font=dict(family="Plus Jakarta Sans", color=P["text"]),
                showlegend=False, coloraxis_showscale=False,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(gridcolor=P["border"], zeroline=False),
                yaxis=dict(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending"),
                height=300,
            )
            fig_lb.update_traces(marker_line_width=0)
            st.plotly_chart(fig_lb, use_container_width=True)

    # ── Daily Trend ─────────────────────────────────────────
    with right_col:
        st.markdown(f"<div class='section-title'>{t('daily_trend')}</div>", unsafe_allow_html=True)

        if COL_DATE in done_f.columns:
            done_f = done_f.copy()
            done_f["_date"] = done_f[COL_DATE].apply(
                lambda s: parse_date(str(s)).date() if parse_date(str(s)) else None
            )
            trend = (
                done_f.dropna(subset=["_date"])
                .groupby("_date")
                .size()
                .reset_index(name="Records")
            )
            trend.columns = ["Date", "Records"]

            if not trend.empty:
                # Fill missing dates for a continuous line
                if len(trend) > 1:
                    full_range = pd.date_range(trend["Date"].min(), trend["Date"].max())
                    trend = (
                        trend.set_index("Date")
                        .reindex(full_range.date, fill_value=0)
                        .reset_index()
                    )
                    trend.columns = ["Date", "Records"]

                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=trend["Date"],
                    y=trend["Records"],
                    mode="lines+markers",
                    line=dict(color=P["accent"], width=3),
                    marker=dict(color=P["green"], size=7, line=dict(color=P["card"], width=2)),
                    fill="tozeroy",
                    fillcolor=P["accent_glow"],
                    name="Records",
                ))
                fig_trend.update_layout(
                    template=pt,
                    paper_bgcolor=pb, plot_bgcolor=pb,
                    font=dict(family="Plus Jakarta Sans", color=P["text"]),
                    showlegend=False,
                    margin=dict(l=10, r=10, t=10, b=10),
                    xaxis=dict(gridcolor=P["border"], zeroline=False),
                    yaxis=dict(gridcolor=P["border"], zeroline=False),
                    height=320,
                )
                st.plotly_chart(fig_trend, use_container_width=True)

                # Summary row
                s1, s2, s3 = st.columns(3)
                s1.metric("Records in Period", len(done_f))
                s2.metric("Days Active", int((trend["Records"] > 0).sum()))
                avg = len(done_f) / max(int((trend["Records"] > 0).sum()), 1)
                s3.metric("Avg / Day", f"{avg:.1f}")
            else:
                st.info(t("no_data"))
        else:
            st.info("Submit some records first to see the trend.")


# ─────────────────────────────────────────────────────────────────────────────
# 13 · USER MANAGEMENT TAB (Admin only)
# ─────────────────────────────────────────────────────────────────────────────
def render_user_management(users_ws):
    col_add, col_dir = st.columns([1, 1], gap="large")

    # ── Add Auditor ─────────────────────────────────────────
    with col_add:
        st.markdown(f"<div class='section-title'>{t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            new_email = st.text_input("Email", placeholder="auditor@example.com")
            new_pass  = st.text_input("Password", type="password")
            if st.form_submit_button("➕ Add Auditor", use_container_width=True):
                if new_email.strip() and new_pass.strip():
                    all_records = pd.DataFrame(users_ws.get_all_records())
                    if not all_records.empty and new_email.lower().strip() in all_records.get("email", pd.Series()).values:
                        st.error(t("duplicate_email"))
                    else:
                        users_ws.append_row([
                            new_email.lower().strip(),
                            hash_pw(new_pass.strip()),
                            now_str(),
                        ])
                        st.success(f"✅ {new_email} added.")
                        time.sleep(0.8); st.rerun()
                else:
                    st.warning(t("fill_all"))

        # ── Update Password ──────────────────────────────────
        st.markdown(f"<div class='section-title'>{t('update_pass')}</div>", unsafe_allow_html=True)
        all_staff = pd.DataFrame(users_ws.get_all_records())
        if not all_staff.empty and "email" in all_staff.columns:
            with st.form("upd_pass_form"):
                upd_email = st.selectbox("Select Staff", all_staff["email"].tolist())
                upd_pass  = st.text_input("New Password", type="password")
                if st.form_submit_button("🔑 Update", use_container_width=True):
                    if upd_pass.strip():
                        cell = users_ws.find(upd_email)
                        if cell:
                            users_ws.update_cell(cell.row, 2, hash_pw(upd_pass.strip()))
                            st.success(f"✅ Password updated for {upd_email}.")
                            time.sleep(0.8); st.rerun()

    # ── Staff Directory ──────────────────────────────────────
    with col_dir:
        st.markdown(f"<div class='section-title'>{t('staff_list')}</div>", unsafe_allow_html=True)
        all_staff = pd.DataFrame(users_ws.get_all_records())
        if not all_staff.empty and "email" in all_staff.columns:
            show_cols = [c for c in ["email", "created_at"] if c in all_staff.columns]
            st.dataframe(all_staff[show_cols], use_container_width=True, hide_index=True)

            # Remove user
            st.markdown(f"<div class='section-title'>{t('remove_user')}</div>", unsafe_allow_html=True)
            del_choice = st.selectbox(
                "Select to remove", ["—"] + all_staff["email"].tolist(), key="del_sel"
            )
            if del_choice != "—":
                if st.button(f"🗑️ Remove {del_choice}", key="del_btn"):
                    cell = users_ws.find(del_choice)
                    if cell:
                        users_ws.delete_rows(cell.row)
                        st.success(f"✅ {del_choice} removed.")
                        time.sleep(0.8); st.rerun()
        else:
            st.info("No auditor accounts yet.")


# ─────────────────────────────────────────────────────────────────────────────
# 14 · MAIN APPLICATION
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        spreadsheet   = get_spreadsheet()
        all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]

        # Ensure UsersDB worksheet exists
        if "UsersDB" not in all_ws_titles:
            users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="500", cols="3")
            users_ws.append_row(["email", "password", "created_at"])
        else:
            users_ws = spreadsheet.worksheet("UsersDB")

        # ── GATE: show login page if not authenticated ───────
        if not st.session_state.logged_in:
            render_login_page(users_ws)
            return

        # ── From here on: user is authenticated ─────────────
        # Expand sidebar
        st.markdown(
            "<style>[data-testid='stSidebar']{display:flex!important;}</style>",
            unsafe_allow_html=True,
        )
        render_sidebar()

        # Page header
        ts = datetime.now(TZ).strftime("%A, %d %b %Y · %H:%M")
        st.markdown(f"<div class='page-title'>{t('app_name')}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='page-subtitle'>{ts}</div>", unsafe_allow_html=True)

        # ── Workspace selector ───────────────────────────────
        data_sheets = [n for n in all_ws_titles if n != "UsersDB"]
        ws_choice   = st.selectbox(t("workspace"), data_sheets, key="ws_choice")

        current_ws           = spreadsheet.worksheet(ws_choice)
        df, headers, col_map = load_worksheet(current_ws)

        if df.empty:
            st.warning(t("empty_sheet"))
            return

        is_admin = (st.session_state.user_role == "admin")

        # ── Overview metrics ─────────────────────────────────
        st.markdown(f"<div class='section-title'>{t('stats_title')}</div>", unsafe_allow_html=True)

        total_n   = len(df)
        done_n    = int((df[COL_STATUS] == VAL_DONE).sum())
        pending_n = total_n - done_n
        pct       = done_n / total_n if total_n else 0

        m1, m2, m3 = st.columns(3)
        m1.metric(t("total"),     total_n)
        m2.metric(t("completed"), done_n,    delta=f"{int(pct*100)}%")
        m3.metric(t("pending"),   pending_n, delta=f"{100-int(pct*100)}% left",
                  delta_color="inverse")

        st.markdown(f"""
        <div class="progress-label">
          <span>{t('completed')}</span>
          <span>{int(pct*100)}%</span>
        </div>
        <div class="progress-wrap">
          <div class="progress-fill" style="width:{int(pct*100)}%"></div>
        </div>
        """, unsafe_allow_html=True)

        # ── Global search ─────────────────────────────────────
        search_q = st.text_input(t("search"), key="search_q", label_visibility="visible")
        if search_q.strip():
            mask = df.astype(str).apply(
                lambda col: col.str.contains(search_q.strip(), case=False, na=False)
            ).any(axis=1)
            view_df = df[mask].copy()
        else:
            view_df = df.copy()

        # ── Tabs ──────────────────────────────────────────────
        tab_names = [t("tab_queue"), t("tab_archive"), t("tab_analytics")]
        if is_admin:
            tab_names.append(t("tab_users"))
        tabs = st.tabs(tab_names)

        # ══════════════════════════════════════════════════════
        # TAB 1 — TASK QUEUE
        # ══════════════════════════════════════════════════════
        with tabs[0]:
            pending_view = view_df[view_df[COL_STATUS] != VAL_DONE].copy()
            # Offset index to reflect actual Google Sheets row numbers (header = row 1)
            pending_view.index = pending_view.index + 2

            st.dataframe(pending_view, use_container_width=True, height=320)

            st.markdown(f"<div class='section-title'>{t('select_record')}</div>",
                        unsafe_allow_html=True)

            # Build option labels using first meaningful column
            first_col = headers[0] if headers else "Row"
            options = ["—"] + [
                f"Row {idx}  ·  {str(row.get(first_col, ''))[:60]}"
                for idx, row in pending_view.iterrows()
            ]
            row_sel = st.selectbox("", options, label_visibility="collapsed", key="row_sel")

            if row_sel != "—":
                sheet_row  = int(row_sel.split("  ·  ")[0].replace("Row ", "").strip())
                record     = df.iloc[sheet_row - 2].to_dict()

                # ── Audit trail ──────────────────────────────
                with st.expander(t("audit_history"), expanded=False):
                    history = str(record.get(COL_LOG, ""))
                    if history.strip():
                        for line in history.split("\n"):
                            if line.strip():
                                st.markdown(
                                    f"<div style='font-family:JetBrains Mono,monospace;"
                                    f"font-size:0.78rem;color:{P['subtext']};padding:2px 0;'>"
                                    f"{line}</div>",
                                    unsafe_allow_html=True,
                                )
                    else:
                        st.caption("No history yet.")

                # ── Vertical edit form ────────────────────────
                st.markdown(
                    f"<div class='section-title'>{t('processing')} {sheet_row}</div>",
                    unsafe_allow_html=True,
                )
                SKIP = set(SYSTEM_COLS)
                editable = {k: v for k, v in record.items() if k not in SKIP}

                with st.form("audit_form"):
                    new_vals: dict[str, str] = {}
                    for field_name, field_val in editable.items():
                        # Each field occupies its own full-width row
                        new_vals[field_name] = st.text_input(
                            field_name,
                            value=str(field_val),
                            key=f"f_{field_name}",
                        )

                    do_submit = st.form_submit_button(
                        t("submit"), use_container_width=True
                    )

                if do_submit:
                    with st.spinner("Saving to Google Sheets…"):
                        # Ensure system columns exist in the real sheet
                        headers, col_map = ensure_system_cols_in_sheet(
                            current_ws, headers, col_map
                        )
                        # Write only changed user fields
                        for fname, fval in new_vals.items():
                            if fname in col_map and str(record.get(fname, "")) != str(fval):
                                current_ws.update_cell(sheet_row, col_map[fname], fval)

                        # ── Automated identity recording ──────
                        timestamp  = now_str()
                        auditor    = st.session_state.user_email   # never typed by user
                        old_log    = str(record.get(COL_LOG, "")).strip()
                        new_log    = f"✔ {auditor}  @  {timestamp}\n{old_log}".strip()

                        current_ws.update_cell(sheet_row, col_map[COL_STATUS],  VAL_DONE)
                        current_ws.update_cell(sheet_row, col_map[COL_LOG],     new_log)
                        current_ws.update_cell(sheet_row, col_map[COL_AUDITOR], auditor)
                        current_ws.update_cell(sheet_row, col_map[COL_DATE],    timestamp)

                    st.success(t("saved_ok"))
                    time.sleep(0.9)
                    st.rerun()

        # ══════════════════════════════════════════════════════
        # TAB 2 — ARCHIVE
        # ══════════════════════════════════════════════════════
        with tabs[1]:
            done_view = view_df[view_df[COL_STATUS] == VAL_DONE].copy()
            done_view.index = done_view.index + 2
            st.dataframe(done_view, use_container_width=True)

            # Admin: reopen a record
            if is_admin and not done_view.empty:
                st.markdown("---")
                st.markdown(
                    "<div class='section-title'>Admin — Reopen Record</div>",
                    unsafe_allow_html=True,
                )
                reopen_opts = ["—"] + [f"Row {idx}" for idx in done_view.index]
                reopen_sel  = st.selectbox("Select record:", reopen_opts, key="reopen_sel")
                if reopen_sel != "—":
                    ridx = int(reopen_sel.split(" ")[1])
                    if st.button(t("return_pending"), key="reopen_btn"):
                        if COL_STATUS in col_map:
                            current_ws.update_cell(ridx, col_map[COL_STATUS], VAL_PENDING)
                            st.rerun()

        # ══════════════════════════════════════════════════════
        # TAB 3 — ANALYTICS
        # ══════════════════════════════════════════════════════
        with tabs[2]:
            render_analytics(df)

        # ══════════════════════════════════════════════════════
        # TAB 4 — USER MANAGEMENT (admin only)
        # ══════════════════════════════════════════════════════
        if is_admin:
            with tabs[3]:
                render_user_management(users_ws)

    except Exception as exc:
        st.error(f"🚨 Application Error: {exc}")
        with st.expander("Traceback", expanded=False):
            st.exception(exc)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
