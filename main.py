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

# ============================================================
# ١. ڕێکخستنی سەرەکی و دیزاینی پێشکەوتوو (CSS & UI)
# ============================================================
st.set_page_config(
    page_title="Government Audit Pro Platform",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ڕێکخستنی کاتی هەولێر/بەغداد
tz = pytz.timezone('Asia/Baghdad')

# دەسپێکردنی سێشنەکان (Session States)
if 'lang' not in st.session_state:
    st.session_state.lang = 'ku'
if 'theme' not in st.session_state:
    st.session_state.theme = 'dark'
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = ""

# --- دیزاینی ڕەنگەکان (Custom CSS for Dark/Light Theme) ---
if st.session_state.theme == 'dark':
    bg_color = "#0B1016"      # Deep Space Blue
    card_bg = "#161B22"       # Dark Grey
    text_color = "#E6EDF3"    # Off-White
    border_color = "#30363D"  # Border Grey
    input_bg = "#0D1117"      # Deep Black
    accent_color = "#58A6FF"  # Soft Blue
else:
    bg_color = "#F8F9FA"      # Soft Light Grey
    card_bg = "#FFFFFF"       # Pure White
    text_color = "#1F2328"    # Black Text
    border_color = "#D0D7DE"  # Light Border
    input_bg = "#FFFFFF"      # White Input
    accent_color = "#0969DA"  # Deep Blue

st.markdown(f"""
<style>
    /* بنچینەی سایت */
    .stApp {{ background-color: {bg_color}; color: {text_color}; }}
    
    /* کارتەکانی داشبۆرد */
    [data-testid="stMetricContainer"] {{
        background-color: {card_bg};
        border: 1px solid {border_color};
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }}
    
    /* فۆڕمەکان */
    div[data-testid="stForm"] {{
        background-color: {card_bg};
        border-radius: 20px;
        padding: 35px;
        border: 1px solid {border_color};
        box-shadow: 0 8px 24px rgba(0,0,0,0.15);
    }}
    
    /* خانەکانی نووسین */
    .stTextInput>div>div>input {{
        background-color: {input_bg} !important;
        color: {text_color} !important;
        border: 1px solid {border_color} !important;
        border-radius: 8px;
    }}
    
    /* تابەکان */
    .stTabs [data-baseweb="tab-list"] {{ gap: 15px; }}
    .stTabs [data-baseweb="tab"] {{
        background-color: {card_bg};
        border-radius: 10px 10px 0px 0px;
        padding: 10px 25px;
        border: 1px solid {border_color};
    }}

    /* لابردنی بەشە زیادەکانی ستریملێت */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)

# ============================================================
# ٢. فەرهەنگی وەرگێڕان (Bilingual Translations)
# ============================================================
ui_dict = {
    "ku": {
        "title": "📊 پلاتفۆرمی وردبینی و داشبۆردی حکومی",
        "login_h": "🔐 چوونەژوورەوەی پارێزراو",
        "email_label": "ئیمەیڵ (یان admin):",
        "pass_label": "پاسۆرد:",
        "login_btn": "چوونەژوورەوە",
        "logout_btn": "چوونەدەەرەوە",
        "theme_label": "🎨 تێمی سایت (Theme):",
        "lang_label": "🌐 زمان (Language):",
        "stats_h": "📈 ئامارە گشتییەکانی ئەم شیتە",
        "total_files": "کۆی گشتی فایلەکان",
        "done_files": "تەواوکراوەکان ✅",
        "pending_files": "ماوە (Pending) ⏳",
        "search_label": "🔍 گەڕانی خێرا (ناوی کۆمپانیا، مۆڵەت، ئیمەیڵ...):",
        "audit_tab": "📝 لیستی کارەکان",
        "archive_tab": "✅ ئەرشیفی تەواوکراو",
        "user_mgmt_tab": "👥 بەڕێوەبردنی کارمەند",
        "select_row": "📌 ڕیزێک هەڵبژێرە بۆ دەستکاریکردن:",
        "submit_btn": "سەبمیت و پەسەندکردن ✅",
        "history_label": "👁️‍🗨️ مێژووی گۆڕانکارییەکان (Audit Trail)",
        "add_user_h": "👤 دروستکردنی هەژماری نوێ بۆ ئۆدیتۆر",
        "user_list_h": "📋 لیستی کارمەندە دەسەڵاتپێدراوەکان",
        "success_msg": "سەرکەوتوو بوو! داتاکان بە ناوی تۆوە پاشەکەوت کران.",
        "return_btn": "گەڕاندنەوە بۆ لیستی کارەکان ↩️"
    },
    "en": {
        "title": "📊 Government Audit & Analytics Platform",
        "login_h": "🔐 Secure Login",
        "email_label": "Email (or admin):",
        "pass_label": "Password:",
        "login_btn": "Login",
        "logout_btn": "Logout",
        "theme_label": "🎨 Site Theme:",
        "lang_label": "🌐 Language:",
        "stats_h": "📈 Workspace General Statistics",
        "total_files": "Total Records",
        "done_files": "Completed ✅",
        "pending_files": "Pending ⏳",
        "search_label": "🔍 Global Search (Company, License, Email...):",
        "audit_tab": "📝 Task Queue",
        "archive_tab": "✅ Done Archive",
        "user_mgmt_tab": "👥 User Management",
        "select_row": "📌 Select a record to inspect:",
        "submit_btn": "Submit & Approve ✅",
        "history_label": "👁️‍🗨️ Audit Trail (History)",
        "add_user_h": "👤 Create New Auditor Account",
        "user_list_h": "📋 Authorized Staff List",
        "success_msg": "Success! Data saved under your identity.",
        "return_btn": "Return to Task Queue ↩️"
    }
}

def t(key):
    return ui_dict[st.session_state.lang][key]

# ============================================================
# ٣. پەیوەندی بە گۆگڵ شیت و فەنکشنەکان (Logic)
# ============================================================
@st.cache_resource
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    pk = key_dict["private_key"].replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

def hash_password(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# ============================================================
# ٤. جێبەجێکردنی پڕۆژە و شاشەی لۆگین
# ============================================================
try:
    spreadsheet = get_gspread_client()
    all_ws_titles = [ws.title for ws in spreadsheet.worksheets()]
    
    # دروستکردنی شیتیک بۆ یوزەرەکان ئەگەر نەبێت
    if "UsersDB" not in all_ws_titles:
        users_sheet = spreadsheet.add_worksheet(title="UsersDB", rows="100", cols="2")
        users_sheet.append_row(["email", "password"])
    else:
        users_sheet = spreadsheet.worksheet("UsersDB")

    # --- Sidebar (لای چەپ) ---
    with st.sidebar:
        st.markdown(f"### {t('lang_label')}")
        c1, c2 = st.columns(2)
        if c1.button("English", use_container_width=True): st.session_state.lang = 'en'; st.rerun()
        if c2.button("کوردی", use_container_width=True): st.session_state.lang = 'ku'; st.rerun()
        
        st.markdown(f"### {t('theme_label')}")
        t1, t2 = st.columns(2)
        if t1.button("☀️ Light", use_container_width=True): st.session_state.theme = 'light'; st.rerun()
        if t2.button("🌙 Dark", use_container_width=True): st.session_state.theme = 'dark'; st.rerun()
        st.markdown("---")
        
        if not st.session_state.logged_in:
            st.title(t("login_h"))
            input_email = st.text_input(t("email_label")).lower().strip()
            input_pass = st.text_input(t("pass_label"), type="password")
            if st.button(t("login_btn"), use_container_width=True):
                # لۆگینی ئەدمین
                if input_email == "admin" and input_pass == st.secrets["admin_password"]:
                    st.session_state.logged_in = True
                    st.session_state.user_email = "Admin"
                    st.rerun()
                else:
                    # لۆگینی کارمەند لە گۆگڵ شیت
                    staff_data = pd.DataFrame(users_sheet.get_all_records())
                    if not staff_data.empty and input_email in staff_data['email'].values:
                        correct_hash = staff_data[staff_data['email'] == input_email]['password'].values[0]
                        if hash_password(input_pass) == correct_hash:
                            st.session_state.logged_in = True
                            st.session_state.user_email = input_email
                            st.rerun()
                    st.error("❌ Invalid Email or Password!")
            st.stop()
        else:
            st.success(f"👤 User: {st.session_state.user_email}")
            if st.button(t("logout_btn"), use_container_width=True):
                st.session_state.logged_in = False
                st.rerun()

    # --- لاپەڕەی سەرەکی دوای چوونەژوورەوە ---
    st.title(t("title"))
    
    # شاردنەوەی شیتەکانی سیستم
    data_workspaces = [n for n in all_ws_titles if n != "UsersDB"]
    selected_sheet = st.selectbox("📂 Workspace / شیتەکە هەڵبژێرە", data_workspaces)
    
    current_ws = spreadsheet.worksheet(selected_sheet)
    raw_data = current_ws.get_all_values()
    
    if len(raw_data) < 1:
        st.warning("This sheet is empty.")
    else:
        # چارەسەری درێژی ستوونەکان
        headers = []
        seen_cols = {}
        for h in raw_data[0]:
            h = h.strip() or "Unnamed"
            if h in seen_cols:
                seen_cols[h] += 1
                headers.append(f"{h}_{seen_cols[h]}")
            else:
                seen_cols[h] = 0
                headers.append(h)
        
        df = pd.DataFrame(raw_data[1:], columns=headers)
        
        # ستوونەکانی سیستم
        STATUS_COL = "دۆخی فایل"
        LOG_COL = "مێژووی گۆڕانکارییەکان (Audit Log)"
        
        if STATUS_COL not in df.columns:
            df[STATUS_COL] = "نەکراوە"
        
        # --- داشبۆردی ئاماری (Dashboard) ---
        st.subheader(t("stats_h"))
        total_count = len(df)
        done_count = len(df[df[STATUS_COL] == "تەواوکراوە"])
        pending_count = total_count - done_count
        
        m1, m2, m3 = st.columns(3)
        m1.metric(t("total_files"), total_count)
        m2.metric(t("done_files"), done_count)
        m3.metric(t("pending_files"), pending_count)
        
        prog = done_count / total_count if total_count > 0 else 0
        st.progress(prog, text=f"{int(prog*100)}%")
        
        # --- بزوێنەری گەڕان (Search Engine) ---
        st.markdown("---")
        search_val = st.text_input(t("search_label"))
        if search_val:
            mask = df.astype(str).apply(lambda x: x.str.contains(search_val, case=False, na=False)).any(axis=1)
            filtered_df = df[mask]
        else:
            filtered_df = df

        # --- تابەکان (The Main Tabs) ---
        tab_titles = [t("audit_tab"), t("archive_tab")]
        if st.session_state.user_email == "Admin":
            tab_titles.append(t("user_mgmt_tab"))
            
        tabs = st.tabs(tab_titles)

        # ١. تابی پشکنین و نوێکردنەوە
        with tabs[0]:
            pending_df = filtered_df[filtered_df[STATUS_COL] != "تەواوکراوە"]
            st.dataframe(pending_df, use_container_width=True, height=350)
            
            st.markdown("---")
            # هەڵبژاردنی یەک فایل
            row_list = ["---"] + [f"Row {i+2} | {row.get('Company Name', 'No Name')}" for i, row in pending_df.iterrows()]
            row_sel = st.selectbox(t("select_row"), row_list)
            
            if row_sel != "---":
                r_idx = int(row_sel.split(" | ")[0].replace("Row ", ""))
                original_row = df.iloc[r_idx-2].to_dict()
                
                with st.expander(t("history_label"), expanded=False):
                    st.text(original_row.get(LOG_COL, "No history available."))
                
                # فۆڕمی نوێکردنەوە بە شێوەی ستوونی درێژ
                with st.form("long_audit_form"):
                    st.write(f"### 📝 Processing Row: {r_idx}")
                    new_values = {}
                    for col_key, col_val in original_row.items():
                        if col_key not in [STATUS_COL, LOG_COL]:
                            new_values[col_key] = st.text_input(f"{col_key}", value=str(col_val))
                    
                    if st.form_submit_button(t("submit_btn"), use_container_width=True):
                        # لۆجیکی ناردنی داتا بۆ گۆگڵ
                        with st.spinner("Saving to Cloud..."):
                            c_map = {h: i+1 for i, h in enumerate(headers)}
                            
                            # دڵنیابوون لەوەی ستوونی سیستم هەیە
                            for sc in [STATUS_COL, LOG_COL]:
                                if sc not in headers:
                                    new_p = len(headers) + 1
                                    if new_p > current_ws.col_count: current_ws.add_cols(2)
                                    current_ws.update_cell(1, new_p, sc)
                                    headers.append(sc); c_map[sc] = new_p

                            # نوێکردنەوەی هەموو خانەکان
                            for k, v in new_values.items():
                                if str(original_row[k]) != str(v):
                                    current_ws.update_cell(r_idx, c_map[k], v)
                            
                            # مێژووی ئۆتۆماتیکی بەبێ ناو نووسین
                            now_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                            log_text = f"🔹 Verified by {st.session_state.user_email} at {now_time}\n{original_row.get(LOG_COL, '')}"
                            current_ws.update_cell(r_idx, c_map[STATUS_COL], "تەواوکراوە")
                            current_ws.update_cell(r_idx, c_map[LOG_COL], log_text.strip())
                            
                            st.success(t("success_msg"))
                            time.sleep(1)
                            st.rerun()

        # ٢. تابی ئەرشیف
        with tabs[1]:
            done_df = filtered_df[filtered_df[STATUS_COL] == "تەواوکراوە"]
            st.dataframe(done_df, use_container_width=True)
            
            if st.session_state.user_email == "Admin" and not done_df.empty:
                st.markdown("---")
                ret_row = st.selectbox("↩️ Return to Pending (Admin only):", ["---"] + [f"Row {i+2}" for i in done_df.index])
                if ret_row != "---":
                    idx_ret = int(ret_row.split(" ")[1])
                    if st.button(t("return_btn")):
                        current_ws.update_cell(idx_ret, headers.index(STATUS_COL)+1, "نەکراوە")
                        st.rerun()

        # ٣. تابی بەڕێوەبردنی کارمەند (تەنها ئەدمین دەیبینێت)
        if st.session_state.user_email == "Admin":
            with tabs[2]:
                st.subheader(t("add_user_h"))
                new_u_email = st.text_input("Staff Email:").lower().strip()
                new_u_pass = st.text_input("Staff Password:", type="password")
                if st.button("Save Staff Member"):
                    if new_u_email and new_u_pass:
                        users_sheet.append_row([new_u_email, hash_password(new_u_pass)])
                        st.success("New auditor added to system!")
                        time.sleep(1); st.rerun()
                
                st.divider()
                st.subheader(t("user_list_h"))
                staff_list_df = pd.DataFrame(users_sheet.get_all_records())
                st.table(staff_list_df[['email']])

except Exception as e:
    st.error(f"🚨 Connection or Logic Error: {e}")
