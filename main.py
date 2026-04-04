import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime, timedelta
import pytz
import time
import hashlib
import plotly.express as px
import plotly.graph_objects as go

# ============================================================
# ١. ڕێکخستنی سەرەکی (Page Config)
# ============================================================
st.set_page_config(
    page_title="Government Audit Pro Platform",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

tz = pytz.timezone('Asia/Baghdad')

# ============================================================
# ٢. دەسپێکردنی سێشنەکان (Session State Initialization)
# ============================================================
defaults = {
    'lang': 'ku',
    'theme': 'dark',
    'logged_in': False,
    'user_email': "",
    'date_filter': 'all'
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ============================================================
# ٣. تیمەکانی CSS (Dynamic Theme Engine)
# ============================================================
THEMES = {
    'dark': {
        'bg':           '#090E17',
        'sidebar_bg':   '#0D1321',
        'card_bg':      '#111827',
        'card_hover':   '#1a2235',
        'text':         '#E2E8F5',
        'subtext':      '#8B9CB8',
        'border':       '#1E2D45',
        'input_bg':     '#0A1020',
        'accent':       '#3B82F6',
        'accent2':      '#10B981',
        'accent3':      '#F59E0B',
        'danger':       '#EF4444',
        'success_bg':   '#052E16',
        'success_text': '#4ADE80',
        'tab_active':   '#1E3A5F',
        'metric_bg':    '#111827',
        'progress_bg':  '#1E2D45',
        'progress_fill':'linear-gradient(90deg, #3B82F6, #10B981)',
    },
    'light': {
        'bg':           '#F0F4FA',
        'sidebar_bg':   '#E4ECF7',
        'card_bg':      '#FFFFFF',
        'card_hover':   '#F8FAFF',
        'text':         '#1A2540',
        'subtext':      '#5A6A8A',
        'border':       '#D0DCF0',
        'input_bg':     '#FFFFFF',
        'accent':       '#2563EB',
        'accent2':      '#059669',
        'accent3':      '#D97706',
        'danger':       '#DC2626',
        'success_bg':   '#F0FDF4',
        'success_text': '#16A34A',
        'tab_active':   '#DBEAFE',
        'metric_bg':    '#FFFFFF',
        'progress_bg':  '#E2EAF8',
        'progress_fill':'linear-gradient(90deg, #2563EB, #059669)',
    }
}

C = THEMES[st.session_state.theme]

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

    :root {{
        --bg:           {C['bg']};
        --sidebar-bg:   {C['sidebar_bg']};
        --card:         {C['card_bg']};
        --card-hover:   {C['card_hover']};
        --text:         {C['text']};
        --subtext:      {C['subtext']};
        --border:       {C['border']};
        --input:        {C['input_bg']};
        --accent:       {C['accent']};
        --accent2:      {C['accent2']};
        --accent3:      {C['accent3']};
        --danger:       {C['danger']};
    }}

    /* ── Global Reset ── */
    html, body, .stApp {{
        background-color: var(--bg) !important;
        color: var(--text) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}
    
    /* ── Sidebar ── */
    [data-testid="stSidebar"] {{
        background: var(--sidebar-bg) !important;
        border-right: 1px solid var(--border) !important;
    }}
    [data-testid="stSidebar"] * {{ color: var(--text) !important; }}

    /* ── Metric Cards ── */
    [data-testid="stMetricContainer"] {{
        background: var(--card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 16px !important;
        padding: 20px !important;
        box-shadow: 0 4px 20px rgba(0,0,0,0.12) !important;
        transition: transform .2s, box-shadow .2s !important;
    }}
    [data-testid="stMetricContainer"]:hover {{
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 30px rgba(0,0,0,0.18) !important;
    }}
    [data-testid="stMetricValue"] {{ font-family: 'IBM Plex Mono', monospace !important; font-size: 2.2rem !important; color: var(--accent) !important; }}
    [data-testid="stMetricLabel"] {{ color: var(--subtext) !important; font-weight: 600 !important; font-size: 0.85rem !important; letter-spacing: 0.08em !important; text-transform: uppercase !important; }}

    /* ── Forms ── */
    div[data-testid="stForm"] {{
        background: var(--card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 20px !important;
        padding: 30px !important;
    }}

    /* ── Inputs ── */
    .stTextInput>div>div>input,
    .stSelectbox>div>div>div,
    .stTextArea>div>div>textarea {{
        background: var(--input) !important;
        color: var(--text) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}

    /* ── Buttons ── */
    .stButton>button {{
        background: var(--accent) !important;
        color: #fff !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        transition: opacity .15s, transform .15s !important;
    }}
    .stButton>button:hover {{ opacity: .88; transform: translateY(-1px); }}

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 6px !important;
        background: transparent !important;
        border-bottom: 2px solid var(--border) !important;
        padding-bottom: 0 !important;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent !important;
        color: var(--subtext) !important;
        border-radius: 10px 10px 0 0 !important;
        padding: 10px 22px !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
        border: 1px solid transparent !important;
    }}
    .stTabs [aria-selected="true"] {{
        background: {C['tab_active']} !important;
        color: var(--accent) !important;
        border-color: var(--border) !important;
        border-bottom-color: {C['tab_active']} !important;
    }}

    /* ── DataFrames ── */
    [data-testid="stDataFrame"] {{
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }}
    .dvn-scroller {{ background: var(--card) !important; }}
    .col_heading, .blank {{ background: var(--sidebar-bg) !important; color: var(--subtext) !important; }}

    /* ── Expander ── */
    .streamlit-expanderHeader {{
        background: var(--card) !important;
        color: var(--text) !important;
        border-radius: 10px !important;
        border: 1px solid var(--border) !important;
    }}

    /* ── Progress Custom ── */
    .custom-progress-wrap {{
        background: {C['progress_bg']};
        border-radius: 99px;
        height: 10px;
        overflow: hidden;
        margin: 8px 0 16px;
    }}
    .custom-progress-fill {{
        height: 100%;
        border-radius: 99px;
        background: {C['progress_fill']};
        transition: width 0.6s ease;
    }}

    /* ── Badge ── */
    .badge {{
        display: inline-block;
        padding: 3px 12px;
        border-radius: 99px;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: .05em;
    }}
    .badge-done   {{ background: {C['success_bg']}; color: {C['success_text']}; }}
    .badge-pending{{ background: rgba(245,158,11,0.15); color: #F59E0B; }}

    /* ── Section Header ── */
    .section-header {{
        font-size: 1.15rem;
        font-weight: 700;
        color: var(--text);
        border-left: 4px solid var(--accent);
        padding-left: 12px;
        margin: 24px 0 14px;
        letter-spacing: -.01em;
    }}

    /* ── Leaderboard Card ── */
    .lb-row {{
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 12px 18px;
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 12px;
        margin-bottom: 8px;
    }}
    .lb-rank {{ font-family:'IBM Plex Mono',monospace; font-size:1.1rem; font-weight:700; width:32px; text-align:center; color:var(--accent3); }}
    .lb-name {{ flex:1; font-weight:600; }}
    .lb-count{{ font-family:'IBM Plex Mono',monospace; font-size:1rem; color:var(--accent); font-weight:700; }}

    /* ── Hide Streamlit branding ── */
    #MainMenu, footer, header {{ visibility: hidden; }}
    .stDeployButton {{ display: none; }}
</style>
""", unsafe_allow_html=True)


# ============================================================
# ٤. فەرهەنگی وەرگێڕان (Translations)
# ============================================================
T = {
    "ku": {
        "title":          "⚖️ پلاتفۆرمی وردبینی و ئەنالیتیکسی حکومی",
        "login_h":        "🔐 چوونەژوورەوەی پارێزراو",
        "email_lbl":      "ئیمەیڵ (یان admin):",
        "pass_lbl":       "پاسۆرد:",
        "login_btn":      "چوونەژوورەوە",
        "logout_btn":     "چوونەدەرەوە",
        "theme_lbl":      "🎨 تیمی سایت:",
        "lang_lbl":       "🌐 زمان:",
        "stats_h":        "📈 ئامارە گشتییەکان",
        "total":          "کۆی گشتی فایلەکان",
        "done":           "تەواوکراوە ✅",
        "pending":        "چاوەڕوان ⏳",
        "search_lbl":     "🔍 گەڕان (کۆمپانیا، مۆڵەت، ئیمەیڵ...):",
        "tab_queue":      "📋 ڕیزی کارەکان",
        "tab_archive":    "✅ ئەرشیفی تەواوکراو",
        "tab_analytics":  "📊 ئەنالیتیکس",
        "tab_users":      "👥 بەڕێوەبردنی کارمەند",
        "select_row":     "📌 ڕیزێک هەڵبژێرە:",
        "submit_btn":     "پەسەندکردن و پاشەکەوتکردن ✅",
        "history_lbl":    "👁 مێژووی گۆڕانکاری",
        "add_user_h":     "👤 دروستکردنی هەژماری نوێ",
        "user_list_h":    "📋 لیستی کارمەندان",
        "success_msg":    "✅ سەرکەوتوو! داتا پاشەکەوت کرا.",
        "return_btn":     "↩️ گەڕاندنەوە بۆ چاوەڕوان",
        "lb_h":           "🏆 تەختەی پێشکەوتن",
        "trend_h":        "📅 ترەندی ڕۆژانە",
        "filter_lbl":     "🗓 فیلتەری کات:",
        "f_today":        "ئەمڕۆ",
        "f_week":         "ئەم هەفتەیە",
        "f_month":        "ئەم مانگەیە",
        "f_all":          "هەموو",
        "no_data":        "هیچ داتایەک نییە بۆ ئەم ماوەیە.",
        "invalid_login":  "❌ ئیمەیڵ یان پاسۆرد هەڵەیە!",
        "workspace_lbl":  "📂 Workspace هەڵبژێرە:",
        "sheet_empty":    "⚠️ ئەم شیتە بەتاڵە.",
        "update_pass_h":  "🔑 گۆڕینی پاسۆردی کارمەند",
        "delete_user_h":  "🗑️ سڕینەوەی کارمەند",
    },
    "en": {
        "title":          "⚖️ Government Audit & Analytics Platform",
        "login_h":        "🔐 Secure Login",
        "email_lbl":      "Email (or admin):",
        "pass_lbl":       "Password:",
        "login_btn":      "Login",
        "logout_btn":     "Logout",
        "theme_lbl":      "🎨 Theme:",
        "lang_lbl":       "🌐 Language:",
        "stats_h":        "📈 Global Statistics",
        "total":          "Total Records",
        "done":           "Completed ✅",
        "pending":        "Pending ⏳",
        "search_lbl":     "🔍 Search (Company, License, Email...):",
        "tab_queue":      "📋 Task Queue",
        "tab_archive":    "✅ Done Archive",
        "tab_analytics":  "📊 Analytics",
        "tab_users":      "👥 User Management",
        "select_row":     "📌 Select a record:",
        "submit_btn":     "Approve & Save ✅",
        "history_lbl":    "👁 Audit Trail",
        "add_user_h":     "👤 Create New Auditor",
        "user_list_h":    "📋 Staff Directory",
        "success_msg":    "✅ Success! Record saved.",
        "return_btn":     "↩️ Return to Pending",
        "lb_h":           "🏆 Leaderboard",
        "trend_h":        "📅 Daily Trend",
        "filter_lbl":     "🗓 Time Filter:",
        "f_today":        "Today",
        "f_week":         "This Week",
        "f_month":        "This Month",
        "f_all":          "All Time",
        "no_data":        "No data for this period.",
        "invalid_login":  "❌ Invalid email or password!",
        "workspace_lbl":  "📂 Select Workspace:",
        "sheet_empty":    "⚠️ This sheet is empty.",
        "update_pass_h":  "🔑 Update Staff Password",
        "delete_user_h":  "🗑️ Remove Staff Member",
    }
}

def t(key):
    return T[st.session_state.lang].get(key, key)


# ============================================================
# ٥. فەنکشنەکانی پەیوەندی و ئەمنییەت (Auth & Connection)
# ============================================================
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    pk = key_dict["private_key"]
    pk = pk.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
    pk = pk.replace("\\n", "").replace("\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")


def ensure_system_columns(ws, headers: list) -> tuple[list, dict]:
    """
    Guarantee STATUS_COL, LOG_COL, AUDITOR_COL, DATE_COL exist.
    Returns updated (headers, col_map).
    """
    SYSTEM_COLS = [STATUS_COL, LOG_COL, AUDITOR_COL, DATE_COL]
    col_map = {h: i + 1 for i, h in enumerate(headers)}
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            new_pos = len(headers) + 1
            # Expand grid if needed (Google Sheets max 18278 cols)
            if new_pos > ws.col_count:
                ws.add_cols(4)
            ws.update_cell(1, new_pos, sc)
            headers.append(sc)
            col_map[sc] = new_pos
    return headers, col_map


def get_ws_dataframe(ws) -> tuple[pd.DataFrame, list, dict]:
    """Fetch all values, normalize headers, return df + headers + col_map."""
    raw = ws.get_all_values()
    if len(raw) < 1:
        return pd.DataFrame(), [], {}

    # De-duplicate header names
    headers = []
    seen = {}
    for h in raw[0]:
        h = h.strip() or "Unnamed"
        if h in seen:
            seen[h] += 1
            headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers.append(h)

    df = pd.DataFrame(raw[1:], columns=headers)

    # Inject empty system columns if missing (in-memory only until ensure_system_columns)
    for sc in [STATUS_COL, LOG_COL, AUDITOR_COL, DATE_COL]:
        if sc not in df.columns:
            df[sc] = ""

    col_map = {h: i + 1 for i, h in enumerate(headers)}
    return df, headers, col_map


def apply_date_filter(df: pd.DataFrame, date_col: str, period: str) -> pd.DataFrame:
    """Filter dataframe rows by submission date."""
    if period == 'all' or date_col not in df.columns:
        return df
    now = datetime.now(tz)
    if period == 'today':
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == 'week':
        cutoff = now - timedelta(days=now.weekday())
        cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == 'month':
        cutoff = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return df

    def parse_date(s):
        try:
            return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
        except:
            return None

    dates = df[date_col].apply(parse_date)
    return df[dates >= cutoff]


# ============================================================
# ٦. نموونەی ستوونەکانی سیستم (System Column Names)
# ============================================================
STATUS_COL  = "دۆخی فایل"
LOG_COL     = "مێژووی گۆڕانکارییەکان (Audit Log)"
AUDITOR_COL = "ئیمەیڵی ئۆدیتۆر (Auditor Email)"
DATE_COL    = "بەرواری پەسەندکردن (Submission Date)"


# ============================================================
# ٧. سایدبار: زمان، تیم، لۆگین (Sidebar)
# ============================================================
try:
    spreadsheet = get_spreadsheet()
    all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]

    # Ensure UsersDB exists
    if "UsersDB" not in all_ws_titles:
        users_sheet = spreadsheet.add_worksheet(title="UsersDB", rows="200", cols="3")
        users_sheet.append_row(["email", "password", "created_at"])
        all_ws_titles.append("UsersDB")
    else:
        users_sheet = spreadsheet.worksheet("UsersDB")

    # ── Sidebar ──────────────────────────────────────────────
    with st.sidebar:
        st.markdown(f"<div style='font-size:1.5rem;font-weight:800;padding:10px 0 4px;'>{t('title')}</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='color:{C['subtext']};font-size:0.78rem;margin-bottom:18px;'>v2.0 · Enterprise</div>", unsafe_allow_html=True)
        st.divider()

        # Language Toggle
        st.markdown(f"<div style='font-size:0.8rem;font-weight:700;color:{C['subtext']};letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px;'>{t('lang_lbl')}</div>", unsafe_allow_html=True)
        lc1, lc2 = st.columns(2)
        if lc1.button("🇬🇧 English", use_container_width=True, key="en_btn"):
            st.session_state.lang = 'en'; st.rerun()
        if lc2.button("🟡 کوردی", use_container_width=True, key="ku_btn"):
            st.session_state.lang = 'ku'; st.rerun()

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # Theme Toggle
        st.markdown(f"<div style='font-size:0.8rem;font-weight:700;color:{C['subtext']};letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px;'>{t('theme_lbl')}</div>", unsafe_allow_html=True)
        tc1, tc2 = st.columns(2)
        if tc1.button("☀️ Light", use_container_width=True, key="light_btn"):
            st.session_state.theme = 'light'; st.rerun()
        if tc2.button("🌙 Dark", use_container_width=True, key="dark_btn"):
            st.session_state.theme = 'dark'; st.rerun()

        st.divider()

        # ── Authentication ──
        if not st.session_state.logged_in:
            st.markdown(f"### {t('login_h')}")
            inp_email = st.text_input(t("email_lbl"), key="inp_email").lower().strip()
            inp_pass  = st.text_input(t("pass_lbl"), type="password", key="inp_pass")

            if st.button(t("login_btn"), use_container_width=True, key="login_btn"):
                # Admin shortcut
                if inp_email == "admin" and inp_pass == st.secrets["admin_password"]:
                    st.session_state.logged_in = True
                    st.session_state.user_email = "Admin"
                    st.rerun()
                else:
                    # Look up in UsersDB
                    staff_df = pd.DataFrame(users_sheet.get_all_records())
                    if not staff_df.empty and 'email' in staff_df.columns:
                        match = staff_df[staff_df['email'] == inp_email]
                        if not match.empty:
                            stored_hash = str(match['password'].values[0])
                            if hash_password(inp_pass) == stored_hash:
                                st.session_state.logged_in = True
                                st.session_state.user_email = inp_email
                                st.rerun()
                    st.error(t("invalid_login"))
            st.stop()

        else:
            # Logged-in user info
            is_admin = st.session_state.user_email == "Admin"
            role_badge = "🔴 Admin" if is_admin else "🔵 Auditor"
            st.markdown(f"""
            <div style='background:{C['card_bg']};border:1px solid {C['border']};border-radius:12px;padding:14px 16px;'>
                <div style='font-size:0.75rem;font-weight:700;color:{C['subtext']};letter-spacing:.1em;text-transform:uppercase;'>SIGNED IN AS</div>
                <div style='font-size:0.95rem;font-weight:700;margin-top:4px;'>{st.session_state.user_email}</div>
                <div style='font-size:0.75rem;margin-top:2px;color:{C['accent']};'>{role_badge}</div>
            </div>
            """, unsafe_allow_html=True)
            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
            if st.button(t("logout_btn"), use_container_width=True, key="logout_btn"):
                for k in ['logged_in', 'user_email']:
                    st.session_state[k] = defaults[k]
                st.rerun()

    # ============================================================
    # ٨. لاپەڕەی سەرەکی (Main Content)
    # ============================================================
    st.markdown(f"<h1 style='font-size:1.7rem;font-weight:800;margin-bottom:4px;'>{t('title')}</h1>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:{C['subtext']};margin-bottom:22px;font-size:0.9rem;'>Baghdad · {datetime.now(tz).strftime('%A, %d %b %Y · %H:%M')}</div>", unsafe_allow_html=True)

    # Workspace selector (hide UsersDB)
    data_ws_list = [n for n in all_ws_titles if n != "UsersDB"]
    selected_sheet = st.selectbox(t("workspace_lbl"), data_ws_list, key="ws_sel")

    current_ws          = spreadsheet.worksheet(selected_sheet)
    df, headers, col_map = get_ws_dataframe(current_ws)

    if df.empty:
        st.warning(t("sheet_empty"))
        st.stop()

    # ── Dashboard Metrics ─────────────────────────────────────
    st.markdown(f"<div class='section-header'>{t('stats_h')}</div>", unsafe_allow_html=True)

    total_n   = len(df)
    done_n    = len(df[df[STATUS_COL] == "تەواوکراوە"])
    pending_n = total_n - done_n
    pct       = done_n / total_n if total_n > 0 else 0

    m1, m2, m3 = st.columns(3)
    m1.metric(t("total"),   total_n)
    m2.metric(t("done"),    done_n)
    m3.metric(t("pending"), pending_n)

    st.markdown(f"""
    <div class='custom-progress-wrap'>
        <div class='custom-progress-fill' style='width:{int(pct*100)}%'></div>
    </div>
    <div style='text-align:right;font-size:0.82rem;color:{C['subtext']};margin-top:-10px;margin-bottom:10px;'>
        {int(pct*100)}% {t('done')}
    </div>
    """, unsafe_allow_html=True)

    # ── Global Search ─────────────────────────────────────────
    search_q = st.text_input(t("search_lbl"), key="search_box")
    if search_q:
        mask = df.astype(str).apply(
            lambda col: col.str.contains(search_q, case=False, na=False)
        ).any(axis=1)
        display_df = df[mask]
    else:
        display_df = df

    # ── Tabs ─────────────────────────────────────────────────
    is_admin = st.session_state.user_email == "Admin"
    tab_names = [t("tab_queue"), t("tab_archive"), t("tab_analytics")]
    if is_admin:
        tab_names.append(t("tab_users"))

    tabs = st.tabs(tab_names)

    # =========================================================
    # TAB 1 — Task Queue
    # =========================================================
    with tabs[0]:
        pending_df = display_df[display_df[STATUS_COL] != "تەواوکراوە"].copy()
        pending_df.index = pending_df.index + 2  # align with Google Sheets row numbers

        st.dataframe(pending_df, use_container_width=True, height=320)
        st.markdown(f"<div class='section-header'>{t('select_row')}</div>", unsafe_allow_html=True)

        row_options = ["---"] + [
            f"Row {idx} | {row.get('Company Name', row.iloc[0])}"
            for idx, row in pending_df.iterrows()
        ]
        row_sel = st.selectbox("", row_options, label_visibility="collapsed", key="row_sel")

        if row_sel != "---":
            r_idx = int(row_sel.split(" | ")[0].replace("Row ", ""))
            original = df.iloc[r_idx - 2].to_dict()

            # Audit trail expander
            with st.expander(t("history_lbl"), expanded=False):
                history_text = original.get(LOG_COL, "")
                if history_text:
                    for line in str(history_text).split("\n"):
                        if line.strip():
                            st.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;color:{C['subtext']};padding:2px 0;'>{line}</div>", unsafe_allow_html=True)
                else:
                    st.info("No history yet.")

            # Vertical edit form (one field per row)
            with st.form("audit_form"):
                st.markdown(f"<div style='font-weight:700;font-size:1rem;margin-bottom:16px;'>📝 Row {r_idx} — {selected_sheet}</div>", unsafe_allow_html=True)
                new_values = {}
                SKIP_COLS = {STATUS_COL, LOG_COL, AUDITOR_COL, DATE_COL}
                for col_key, col_val in original.items():
                    if col_key not in SKIP_COLS:
                        new_values[col_key] = st.text_input(
                            f"**{col_key}**",
                            value=str(col_val),
                            key=f"field_{col_key}"
                        )

                submitted = st.form_submit_button(t("submit_btn"), use_container_width=True)

            if submitted:
                with st.spinner("Saving to Cloud…"):
                    # Ensure system columns exist in sheet
                    headers, col_map = ensure_system_columns(current_ws, headers)

                    # Write only changed user fields
                    for k, v in new_values.items():
                        if k in col_map and str(original.get(k, "")) != str(v):
                            current_ws.update_cell(r_idx, col_map[k], v)

                    # System columns — automatic, no manual input
                    now_str  = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_text = (
                        f"🔹 Approved by {st.session_state.user_email} at {now_str}\n"
                        + str(original.get(LOG_COL, ""))
                    ).strip()

                    current_ws.update_cell(r_idx, col_map[STATUS_COL],  "تەواوکراوە")
                    current_ws.update_cell(r_idx, col_map[LOG_COL],     log_text)
                    current_ws.update_cell(r_idx, col_map[AUDITOR_COL], st.session_state.user_email)
                    current_ws.update_cell(r_idx, col_map[DATE_COL],    now_str)

                st.success(t("success_msg"))
                time.sleep(1)
                st.rerun()

    # =========================================================
    # TAB 2 — Archive
    # =========================================================
    with tabs[1]:
        done_df = display_df[display_df[STATUS_COL] == "تەواوکراوە"].copy()
        done_df.index = done_df.index + 2

        # Show last-auditor badge inline
        if AUDITOR_COL in done_df.columns and DATE_COL in done_df.columns:
            st.dataframe(done_df, use_container_width=True)
        else:
            st.dataframe(done_df, use_container_width=True)

        if is_admin and not done_df.empty:
            st.markdown("---")
            st.markdown(f"<div class='section-header'>Admin: Reopen Record</div>", unsafe_allow_html=True)
            ret_opts = ["---"] + [f"Row {idx}" for idx in done_df.index]
            ret_sel  = st.selectbox("Select record to reopen:", ret_opts, key="ret_sel")
            if ret_sel != "---":
                ret_idx = int(ret_sel.split(" ")[1])
                if st.button(t("return_btn")):
                    if STATUS_COL in col_map:
                        current_ws.update_cell(ret_idx, col_map[STATUS_COL], "نەکراوە")
                        st.rerun()

    # =========================================================
    # TAB 3 — Analytics
    # =========================================================
    with tabs[2]:
        # ── Date Filter ───────────────────────────────────────
        st.markdown(f"<div class='section-header'>{t('filter_lbl')}</div>", unsafe_allow_html=True)
        f_options = {
            'all':   t("f_all"),
            'today': t("f_today"),
            'week':  t("f_week"),
            'month': t("f_month"),
        }
        fc1, fc2, fc3, fc4 = st.columns(4)
        for col_widget, (period_key, period_label) in zip([fc1, fc2, fc3, fc4], f_options.items()):
            if col_widget.button(period_label, use_container_width=True, key=f"f_{period_key}"):
                st.session_state.date_filter = period_key
                st.rerun()

        # Apply date filter to completed records only
        done_all = df[df[STATUS_COL] == "تەواوکراوە"].copy()
        if DATE_COL in done_all.columns:
            done_filtered = apply_date_filter(done_all, DATE_COL, st.session_state.date_filter)
        else:
            done_filtered = done_all

        if done_filtered.empty:
            st.info(t("no_data"))
        else:
            plotly_theme = "plotly_dark" if st.session_state.theme == 'dark' else "plotly_white"
            paper_bg  = C['card_bg']
            plot_bg   = C['card_bg']
            font_col  = C['text']

            # ── Leaderboard ───────────────────────────────────
            st.markdown(f"<div class='section-header'>{t('lb_h')}</div>", unsafe_allow_html=True)

            if AUDITOR_COL in done_filtered.columns:
                lb_data = (
                    done_filtered[AUDITOR_COL]
                    .replace("", "Unknown")
                    .value_counts()
                    .reset_index()
                )
                lb_data.columns = ["Auditor", "Count"]

                # Text leaderboard (top 5)
                medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
                for i, row_lb in lb_data.head(5).iterrows():
                    rank_icon = medals[i] if i < len(medals) else f"{i+1}."
                    st.markdown(f"""
                    <div class='lb-row'>
                        <span class='lb-rank'>{rank_icon}</span>
                        <span class='lb-name'>{row_lb['Auditor']}</span>
                        <span class='lb-count'>{row_lb['Count']} records</span>
                    </div>
                    """, unsafe_allow_html=True)

                # Plotly bar chart
                fig_bar = px.bar(
                    lb_data,
                    x="Auditor", y="Count",
                    color="Count",
                    color_continuous_scale=["#3B82F6", "#10B981"],
                    title=t("lb_h"),
                    template=plotly_theme
                )
                fig_bar.update_layout(
                    paper_bgcolor=paper_bg,
                    plot_bgcolor=plot_bg,
                    font=dict(color=font_col, family="Plus Jakarta Sans"),
                    title_font_size=16,
                    showlegend=False,
                    coloraxis_showscale=False,
                    margin=dict(l=20, r=20, t=50, b=20),
                    xaxis=dict(gridcolor=C['border']),
                    yaxis=dict(gridcolor=C['border']),
                )
                fig_bar.update_traces(marker_line_width=0)
                st.plotly_chart(fig_bar, use_container_width=True)

            # ── Daily Trend ───────────────────────────────────
            st.markdown(f"<div class='section-header'>{t('trend_h')}</div>", unsafe_allow_html=True)

            if DATE_COL in done_filtered.columns:
                def to_date(s):
                    try:
                        return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").date()
                    except:
                        return None

                done_filtered = done_filtered.copy()
                done_filtered["_date"] = done_filtered[DATE_COL].apply(to_date)
                trend_data = (
                    done_filtered.dropna(subset=["_date"])
                    .groupby("_date")
                    .size()
                    .reset_index(name="Records Processed")
                )
                trend_data.columns = ["Date", "Records Processed"]

                if not trend_data.empty:
                    fig_line = px.line(
                        trend_data,
                        x="Date", y="Records Processed",
                        markers=True,
                        title=t("trend_h"),
                        template=plotly_theme
                    )
                    fig_line.update_layout(
                        paper_bgcolor=paper_bg,
                        plot_bgcolor=plot_bg,
                        font=dict(color=font_col, family="Plus Jakarta Sans"),
                        title_font_size=16,
                        margin=dict(l=20, r=20, t=50, b=20),
                        xaxis=dict(gridcolor=C['border']),
                        yaxis=dict(gridcolor=C['border']),
                    )
                    fig_line.update_traces(
                        line=dict(color=C['accent'], width=3),
                        marker=dict(color=C['accent2'], size=8)
                    )
                    st.plotly_chart(fig_line, use_container_width=True)

                    # Summary stats for filtered period
                    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                    sa1, sa2, sa3 = st.columns(3)
                    sa1.metric("Records in Period", len(done_filtered))
                    sa2.metric("Days Active", len(trend_data))
                    avg_per_day = len(done_filtered) / max(len(trend_data), 1)
                    sa3.metric("Avg / Day", f"{avg_per_day:.1f}")
                else:
                    st.info(t("no_data"))
            else:
                st.info("Submission Date column not yet available (records need to be processed first).")

    # =========================================================
    # TAB 4 — User Management (Admin only)
    # =========================================================
    if is_admin:
        with tabs[3]:
            col_a, col_b = st.columns([1, 1], gap="large")

            with col_a:
                # ── Add New User ──────────────────────────────
                st.markdown(f"<div class='section-header'>{t('add_user_h')}</div>", unsafe_allow_html=True)
                with st.form("add_user_form"):
                    nu_email = st.text_input("Email:").lower().strip()
                    nu_pass  = st.text_input("Password:", type="password")
                    if st.form_submit_button("➕ Save Staff Member", use_container_width=True):
                        if nu_email and nu_pass:
                            # Check duplicate
                            existing = pd.DataFrame(users_sheet.get_all_records())
                            if not existing.empty and nu_email in existing.get('email', pd.Series()).values:
                                st.error("❌ Email already exists.")
                            else:
                                created_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                                users_sheet.append_row([nu_email, hash_password(nu_pass), created_at])
                                st.success(f"✅ {nu_email} added.")
                                time.sleep(0.8); st.rerun()
                        else:
                            st.warning("Please fill in both fields.")

                # ── Update Password ───────────────────────────
                st.markdown(f"<div class='section-header'>{t('update_pass_h')}</div>", unsafe_allow_html=True)
                with st.form("update_pass_form"):
                    staff_df_up = pd.DataFrame(users_sheet.get_all_records())
                    if not staff_df_up.empty and 'email' in staff_df_up.columns:
                        email_to_update = st.selectbox("Select Staff:", staff_df_up['email'].tolist())
                        new_pass = st.text_input("New Password:", type="password")
                        if st.form_submit_button("🔑 Update Password", use_container_width=True):
                            if new_pass:
                                cell = users_sheet.find(email_to_update)
                                if cell:
                                    users_sheet.update_cell(cell.row, 2, hash_password(new_pass))
                                    st.success(f"✅ Password updated for {email_to_update}.")
                                    time.sleep(0.8); st.rerun()
                    else:
                        st.info("No staff members yet.")

            with col_b:
                # ── Staff Directory ───────────────────────────
                st.markdown(f"<div class='section-header'>{t('user_list_h')}</div>", unsafe_allow_html=True)
                staff_all = pd.DataFrame(users_sheet.get_all_records())
                if not staff_all.empty and 'email' in staff_all.columns:
                    # Show only email + created_at, never expose password hashes
                    show_cols = [c for c in ['email', 'created_at'] if c in staff_all.columns]
                    st.dataframe(staff_all[show_cols], use_container_width=True)

                    # ── Delete User ───────────────────────────
                    st.markdown(f"<div class='section-header'>{t('delete_user_h')}</div>", unsafe_allow_html=True)
                    del_email = st.selectbox("Select to Remove:", ["---"] + staff_all['email'].tolist(), key="del_sel")
                    if del_email != "---":
                        if st.button(f"🗑️ Remove {del_email}", key="del_btn"):
                            cell = users_sheet.find(del_email)
                            if cell:
                                users_sheet.delete_rows(cell.row)
                                st.success(f"✅ {del_email} removed.")
                                time.sleep(0.8); st.rerun()
                else:
                    st.info("No staff members have been added yet.")

except Exception as e:
    st.error(f"🚨 Error: {e}")
    st.exception(e)
