import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz
import time
import hashlib

# ==========================================
# ١. ڕێکخستنی سەرەکی پەیج (دەبێت لە سەرەتا بێت)
# ==========================================
st.set_page_config(
    page_title="Government Audit System Pro",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ڕێکخستنی کات بۆ ناوچەی هەولێر/بەغداد
tz = pytz.timezone('Asia/Baghdad')

# ==========================================
# ٢. سیستمی گۆڕینی زمان و تێم (Theme & Language)
# ==========================================
if 'lang' not in st.session_state:
    st.session_state.lang = 'ku'
if 'theme' not in st.session_state:
    st.session_state.theme = 'dark'
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""

def change_lang(l):
    st.session_state.lang = l

def change_theme(t):
    st.session_state.theme = t

# دیزاینی CSS بۆ ڕەنگەکان (Dark/Light) بە شێوەیەکی درێژ و ورد
if st.session_state.theme == 'dark':
    main_bg = "#0E1117"
    txt_color = "#FFFFFF"
    card_color = "rgba(255, 255, 255, 0.05)"
    brd_color = "rgba(255, 255, 255, 0.1)"
    inp_bg = "#262730"
else:
    main_bg = "#FFFFFF"
    txt_color = "#000000"
    card_color = "rgba(0, 0, 0, 0.03)"
    brd_color = "rgba(0, 0, 0, 0.1)"
    inp_bg = "#f0f2f6"

st.markdown(f"""
<style>
    .stApp {{ background-color: {main_bg}; color: {txt_color}; }}
    [data-testid="stMetricContainer"] {{
        background-color: {card_color};
        border: 1px solid {brd_color};
        padding: 20px; border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }}
    div[data-testid="stForm"] {{
        background-color: {card_color};
        border-radius: 20px; padding: 40px;
        border: 1px solid {brd_color};
    }}
    .stTextInput>div>div>input {{ background-color: {inp_bg}; color: {txt_color}; border-radius: 8px; }}
    #MainMenu {{visibility: hidden;}} footer {{visibility: hidden;}} header {{visibility: hidden;}}
    .stTabs [data-baseweb="tab-list"] {{ gap: 15px; }}
    .stTabs [data-baseweb="tab"] {{
        background-color: {card_color};
        border-radius: 10px 10px 0px 0px;
        padding: 12px 25px;
        font-weight: bold;
    }}
</style>
""", unsafe_allow_html=True)

# ==========================================
# ٣. فەرهەنگی وشەکان (Translations)
# ==========================================
words = {
    "ku": {
        "title": "📊 پلاتفۆرمی وردبینی و بەڕێوەبردنی داتاکان",
        "login": "🔑 چوونەژوورەوە",
        "email": "ئیمەیڵ (یان admin):",
        "pass": "پاسۆرد:",
        "login_btn": "چوونەژوورەوە",
        "lang_sel": "🌐 زمان (Language):",
        "theme_sel": "🎨 ڕەنگی سایت (Theme):",
        "logout": "چوونەدەەرەوە 🏃",
        "stats": "📈 ئامارە گشتییەکان",
        "total": "کۆی گشتی داتاکان",
        "done": "تەواوکراوەکان",
        "pending": "ماوە (Pending)",
        "progress": "ڕێژەی تەواوبوونی کارەکان",
        "search": "🔍 بزوێنەری گەڕان (ناوی کۆمپانیا، مۆڵەت، ئیمەیڵ...):",
        "audit_tab": "📝 لیستی کارەکان",
        "archive_tab": "✅ ئەرشیفی تەواوکراو",
        "users_tab": "👥 بەڕێوەبردنی کارمەند",
        "select_row": "📌 ڕیزێک هەڵبژێرە بۆ پشکنین:",
        "submit_btn": "سەبمیت و پەسەندکردن ✅",
        "history": "👁️‍🗨️ مێژووی گۆڕانکارییەکان (Audit Log)",
        "add_user": "👤 زیادکردنی ئۆدیتۆری نوێ",
        "user_list": "📋 لیستی کارمەندەکان",
        "success": "سەرکەوتوو بوو! داتاکان پارێزران.",
        "return": "گەڕاندنەوە بۆ لیستی کارەکان ↩️"
    },
    "en": {
        "title": "📊 Data Audit & Management Platform",
        "login": "🔑 Secure Login",
        "email": "Email (or admin):",
        "pass": "Password:",
        "login_btn": "Login",
        "lang_sel": "🌐 Language:",
        "theme_sel": "🎨 Site Theme:",
        "logout": "Logout 🏃",
        "stats": "📈 Global Statistics",
        "total": "Total Records",
        "done": "Completed",
        "pending": "Pending",
        "progress": "Work Progress Rate",
        "search": "🔍 Global Search (Company, License, Email...):",
        "audit_tab": "📝 Task Queue",
        "archive_tab": "✅ Archive (Done)",
        "users_tab": "👥 User Management",
        "select_row": "📌 Select a row to process:",
        "submit_btn": "Submit & Approve ✅",
        "history": "👁️‍🗨️ Audit Log (History)",
        "add_user": "👤 Add New Auditor Account",
        "user_list": "📋 Staff List",
        "success": "Success! Data has been secured.",
        "return": "Return to Task Queue ↩️"
    }
}

def t(key):
    return words[st.session_state.lang][key]

# ==========================================
# ٤. پەیوەندی بە گۆگڵ شیت (Google Connection)
# ==========================================
@st.cache_resource
def connect_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    pk = key_dict["private_key"].replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

def hash_password(p):
    return hashlib.sha256(str.encode(p)).hexdigest()

# ==========================================
# ٥. لۆجیکی بەڕێوەبردنی سیستم
# ==========================================
try:
    spreadsheet = connect_google()
    all_ws_names = [ws.title for ws in spreadsheet.worksheets()]
    
    # دڵنیابوون لە داتابەیسی یوزەرەکان (UsersDB)
    if "UsersDB" not in all_ws_names:
        users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="100", cols="2")
        users_ws.append_row(["email", "password"])
    else:
        users_ws = spreadsheet.worksheet("UsersDB")

    # --- بەشی سایتبار (Sidebar) ---
    with st.sidebar:
        st.markdown(f"### {t('lang_sel')}")
        c1, c2 = st.columns(2)
        if c1.button("English", use_container_width=True): change_lang('en'); st.rerun()
        if c2.button("کوردی", use_container_width=True): change_lang('ku'); st.rerun()
        
        st.markdown(f"### {t('theme_sel')}")
        t1, t2 = st.columns(2)
        if t1.button("☀️ Light", use_container_width=True): change_theme('light'); st.rerun()
        if t2.button("🌙 Dark", use_container_width=True): change_theme('dark'); st.rerun()
        st.markdown("---")
        
        if not st.session_state.logged_in:
            st.title(t("login"))
            e_input = st.text_input(t("email")).lower().strip()
            p_input = st.text_input(t("pass"), type="password")
            if st.button(t("login_btn"), use_container_width=True):
                # لۆگینی ئەدمین لە Secrets
                if e_input == "admin" and p_input == st.secrets["admin_password"]:
                    st.session_state.logged_in = True
                    st.session_state.user_email = "Admin"
                    st.rerun()
                else:
                    # لۆگینی کارمەند لەناو گۆگڵ شیت
                    u_df = pd.DataFrame(users_ws.get_all_records())
                    if not u_df.empty and e_input in u_df['email'].values:
                        stored_p = u_df[u_df['email'] == e_input]['password'].values[0]
                        if hash_password(p_input) == stored_p:
                            st.session_state.logged_in = True
                            st.session_state.user_email = e_input
                            st.rerun()
                    st.error("❌ Invalid Email or Password!")
            st.stop()
        else:
            st.success(f"👤 {st.session_state.user_email}")
            if st.button(t("logout"), use_container_width=True):
                st.session_state.logged_in = False
                st.rerun()

    # --- بەشی سەرەکی دوای لۆگین ---
    st.title(t("title"))
    
    # دەرنەخستنی شیتی یوزەرەکان لە لیستی کارەکان
    data_sheets = [n for n in all_ws_names if n != "UsersDB"]
    target_sheet = st.selectbox("📂 Workspace (Google Sheet Tabs)", data_sheets)
    
    current_ws = spreadsheet.worksheet(target_sheet)
    raw_data = current_ws.get_all_values()
    
    if len(raw_data) < 1:
        st.warning("Empty Sheet!")
    else:
        # چارەسەری ستوونە دووبارەکان بە درێژی
        headers = []
        counts = {}
        for h in raw_data[0]:
            h = h.strip() or "Untitled"
            if h in counts:
                counts[h] += 1
                headers.append(f"{h}_{counts[h]}")
            else:
                counts[h] = 0
                headers.append(h)
        
        df = pd.DataFrame(raw_data[1:], columns=headers)
        
        # ستوونەکانی وردبینی
        STATUS_COL = "دۆخی فایل"
        LOG_COL = "مێژووی گۆڕانکارییەکان (Audit Log)"
        
        if STATUS_COL not in df.columns:
            df[STATUS_COL] = "نەکراوە"
        
        # --- داشبۆرد (Dashboard) ---
        st.subheader(t("stats"))
        total = len(df)
        completed = len(df[df[STATUS_COL] == "تەواوکراوە"])
        pending = total - completed
        
        col1, col2, col3 = st.columns(3)
        col1.metric(t("total"), total)
        col2.metric(t("done"), completed)
        col3.metric(t("pending"), pending)
        
        p_val = completed / total if total > 0 else 0
        st.progress(p_val, text=f"{t('progress')}: {int(p_val*100)}%")
        
        # --- گەڕان (Search Engine) ---
        st.markdown("---")
        q = st.text_input(t("search"))
        if q:
            mask = df.astype(str).apply(lambda x: x.str.contains(q, case=False, na=False)).any(axis=1)
            filtered_df = df[mask]
        else:
            filtered_df = df

        # --- تابەکان (Tabs) ---
        tab_list = [t("audit_tab"), t("archive_tab")]
        if st.session_state.user_email == "Admin":
            tab_list.append(t("users_tab"))
            
        tabs = st.tabs(tab_list)

        # ١. تابی ئۆدیت و پشکنین
        with tabs[0]:
            pending_list = filtered_df[filtered_df[STATUS_COL] != "تەواوکراوە"]
            st.dataframe(pending_list, use_container_width=True, height=350)
            
            st.markdown("---")
            row_options = ["---"] + [f"Row {i+2} | {row.get('اسم الشركة / کۆمپانیای / Company Name', 'No Name')}" for i, row in pending_list.iterrows()]
            choice = st.selectbox(t("select_row"), row_options)
            
            if choice != "---":
                row_idx = int(choice.split(" | ")[0].replace("Row ", "").strip())
                row_data = df.iloc[row_idx-2].to_dict()
                
                with st.expander(t("history"), expanded=False):
                    st.text(row_data.get(LOG_COL, "No history yet."))
                
                with st.form("edit_form"):
                    st.write(f"### 📝 Editing: Row {row_idx}")
                    updated_values = {}
                    # نیشاندانی هەموو ستوونەکان بە ڕیزبەندی ستوونی
                    for k, v in row_data.items():
                        if k not in [STATUS_COL, LOG_COL]:
                            updated_values[k] = st.text_input(f"{k}", value=str(v))
                    
                    if st.form_submit_button(t("submit_btn"), use_container_width=True):
                        # لێرەدا ئیمەیڵەکە بە ئۆتۆماتیکی لە سێشنەوە وەردەگیرێت
                        current_user = st.session_state.user_email
                        now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                        
                        col_map = {h: i+1 for i, h in enumerate(headers)}
                        
                        # پشکنینی ستوونەکانی سیستم لە گۆگڵ شیت
                        for sys_c in [STATUS_COL, LOG_COL]:
                            if sys_c not in headers:
                                new_pos = len(headers) + 1
                                if new_pos > current_ws.col_count:
                                    current_ws.add_cols(2)
                                current_ws.update_cell(1, new_pos, sys_c)
                                headers.append(sys_c)
                                col_map[sys_c] = new_pos

                        # نوێکردنەوەی خانەکان
                        for k, v in updated_values.items():
                            if str(row_data[k]) != str(v):
                                current_ws.update_cell(row_idx, col_map[k], v)
                        
                        # دروستکردنی مێژووی نوێ
                        new_log = f"🔹 Verified by {current_user} @ {now_str}\n{row_data.get(LOG_COL, '')}"
                        current_ws.update_cell(row_idx, col_map[STATUS_COL], "تەواوکراوە")
                        current_ws.update_cell(row_idx, col_map[LOG_COL], new_log.strip())
                        
                        st.success(t("success"))
                        time.sleep(1)
                        st.rerun()

        # ٢. تابی ئەرشیف (بۆ بینینی ئەوانەی تەواو بوون)
        with tabs[1]:
            done_list = filtered_df[filtered_df[STATUS_COL] == "تەواوکراوە"]
            st.dataframe(done_list, use_container_width=True)
            
            if st.session_state.user_email == "Admin" and not done_list.empty:
                st.markdown("---")
                ret_row = st.selectbox(t("select_row") + " (Admin Override)", ["---"] + [f"Row {i+2}" for i in done_list.index])
                if ret_row != "---":
                    ret_idx = int(ret_row.split(" ")[1])
                    if st.button(t("return"), type="secondary"):
                        current_ws.update_cell(ret_idx, headers.index(STATUS_COL)+1, "نەکراوە")
                        st.rerun()

        # ٣. تابی بەڕێوەبردنی کارمەند (تەنها ئەدمین)
        if st.session_state.user_email == "Admin":
            with tabs[2]:
                st.subheader(t("add_user"))
                new_e = st.text_input("New Email").lower().strip()
                new_p = st.text_input("New Password", type="password")
                if st.button("Save Staff Account"):
                    if new_e and new_e:
                        users_ws.append_row([new_e, hash_password(new_p)])
                        st.success("Staff member added!")
                        time.sleep(1); st.rerun()
                
                st.divider()
                st.subheader(t("user_list"))
                staff_data = pd.DataFrame(users_ws.get_all_records())
                st.table(staff_data[['email']])

except Exception as e:
    st.error(f"⚠️ System Error: {e}")
