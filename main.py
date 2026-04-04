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

# --- ڕێکخستنی شاشە ---
st.set_page_config(page_title="Secure Audit Platform", page_icon="🔐", layout="wide")
tz = pytz.timezone('Asia/Baghdad')

# فەرهەنگی وشەکان
if 'lang' not in st.session_state: st.session_state.lang = 'ku'
ui = {
    "login_title": {"ku": "🔑 چوونەژوورەوەی کارمەندان", "en": "🔑 Staff Login"},
    "email": {"ku": "ئیمەیڵ:", "en": "Email:"},
    "password": {"ku": "پاسۆرد:", "en": "Password:"},
    "login_btn": {"ku": "چوونەژوورەوە", "en": "Login"},
    "logout_btn": {"ku": "چوونەدەەرەوە", "en": "Logout"},
    "auth_error": {"ku": "❌ ئیمەیڵ یان پاسۆرد هەڵەیە!", "en": "❌ Invalid Email or Password!"},
    "app_title": {"ku": "⚡ پلاتفۆرمی وردبینی (پارێزراو)", "en": "⚡ Secure Audit Platform"},
    "user_info": {"ku": "👤 بەکارهێنەر:", "en": "👤 User:"},
    "submit_msg": {"ku": "سەرکەوتوو بوو! تۆمارکرا بەناوی:", "en": "Success! Recorded as:"}
}
def t(key): return ui.get(key, {}).get(st.session_state.lang, 'ku')

@st.cache_resource
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    pk = key_dict["private_key"].replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client.open("site CIT QA - Tranche 4")

# --- سیستمی لۆگین و ناسنامە ---
def make_hashes(password): return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    if make_hashes(password) == hashed_text: return True
    return False

try:
    spreadsheet = get_spreadsheet()
    all_sheets = [ws.title for ws in spreadsheet.worksheets()]
    
    # دروستکردنی شیتی بەکارهێنەران ئەگەر نەبێت
    if "UsersDB" not in all_sheets:
        users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="100", cols="2")
        users_ws.append_row(["email", "password"])
    else:
        users_ws = spreadsheet.worksheet("UsersDB")

    # هێنانی داتای بەکارهێنەران
    users_data = pd.DataFrame(users_ws.get_all_records())

    if 'logged_in' not in st.session_state: st.session_state.logged_in = False
    if 'user_email' not in st.session_state: st.session_state.user_email = ""

    # --- پیشاندانی شاشەی لۆگین ئەگەر نەچووبێتە ژوورەوە ---
    if not st.session_state.logged_in:
        st.sidebar.markdown("### 🌐 Language")
        if st.sidebar.button("English / کوردی"):
            st.session_state.lang = 'en' if st.session_state.lang == 'ku' else 'ku'
            st.rerun()

        with st.container():
            st.title(t("login_title"))
            login_email = st.text_input(t("email")).lower().strip()
            login_pass = st.text_input(t("password"), type="password")
            
            if st.button(t("login_btn"), use_container_width=True):
                # پشکنینی ئەوەی ئایا ئەدەمینە یان کارمەند
                if login_email == "admin" and login_pass == st.secrets["admin_password"]:
                    st.session_state.logged_in = True
                    st.session_state.user_email = "Admin"
                    st.rerun()
                elif not users_data.empty and login_email in users_data['email'].values:
                    stored_hash = users_data[users_data['email'] == login_email]['password'].values[0]
                    if check_hashes(login_pass, stored_hash):
                        st.session_state.logged_in = True
                        st.session_state.user_email = login_email
                        st.rerun()
                    else: st.error(t("auth_error"))
                else: st.error(t("auth_error"))
        st.stop() # دەوەستێت لێرە و ناهێڵێت داتا ببینێت

    # --- ئەگەر لۆگین کرابوو، ئەم بەشەی خوارەوە کار دەکات ---
    st.sidebar.success(f"{t('user_info')} {st.session_state.user_email}")
    if st.sidebar.button(t("logout_btn")):
        st.session_state.logged_in = False
        st.rerun()

    # --- تابی بەڕێوەبردن تەنها بۆ ئەدمین ---
    tabs_list = ["📊 Audit", "👥 Manage Users"] if st.session_state.user_email == "Admin" else ["📊 Audit"]
    main_tabs = st.tabs(tabs_list)

    with main_tabs[0]:
        data_sheets = [n for n in all_sheets if n not in ["UsersDB", "Auditors"]]
        selected_sheet_name = st.selectbox("📂 Select Sheet", data_sheets)
        sheet = spreadsheet.worksheet(selected_sheet_name)
        
        # لۆجیکی خوێندنەوەی داتاکان (هەمان کۆدەکەی پێشوو بە کەمێک دەستکارییەوە)
        raw_data = sheet.get_all_values()
        headers = raw_data[0]
        # دروستکردنی ناوی جیاواز بۆ ستوونەکان
        unique_headers = []
        seen = {}
        for h in headers:
            h = str(h).strip() or "Col"
            if h in seen: seen[h] += 1; unique_headers.append(f"{h}_{seen[h]}")
            else: seen[h] = 0; unique_headers.append(h)
        
        df = pd.DataFrame(raw_data[1:], columns=unique_headers)
        STATUS_COL, LOG_COL = "دۆخی فایل", "مێژووی گۆڕانکارییەکان (Audit Log)"
        if STATUS_COL not in df.columns: df[STATUS_COL] = "نەکراوە"
        
        pending_df = df[df[STATUS_COL] != "تەواوکراوە"]
        st.dataframe(pending_df, use_container_width=True)

        selected_row = st.selectbox("📌 Select Row", ["---"] + [f"Row {i+2}" for i in pending_df.index])
        
        if selected_row != "---":
            row_idx = int(selected_row.split(" ")[1])
            current_row_data = df.iloc[row_idx-2].to_dict()
            
            with st.form("audit_form"):
                st.write(f"### Editing as: **{st.session_state.user_email}**")
                new_data = {}
                for k, v in current_row_data.items():
                    if k not in [STATUS_COL, LOG_COL]:
                        new_data[k] = st.text_input(k, value=str(v))
                
                if st.form_submit_button("Submit ✅"):
                    # لێرەدا ئیمەیڵەکە بە ئۆتۆماتیکی وەردەگیرێت
                    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_entry = f"🔹 Verified by {st.session_state.user_email} at {now}"
                    
                    # ئاپدەیتکردنی گۆگڵ شیت
                    col_map = {h: i+1 for i, h in enumerate(unique_headers)}
                    # دڵنیابوون لە بوونی ستوونەکان
                    for c in [STATUS_COL, LOG_COL]:
                        if c not in unique_headers:
                            sheet.update_cell(1, len(unique_headers)+1, c)
                            unique_headers.append(c)
                            col_map[c] = len(unique_headers)

                    for k, v in new_data.items():
                        if str(current_row_data[k]) != str(v):
                            sheet.update_cell(row_idx, col_map[k], v)
                    
                    old_log = current_row_data.get(LOG_COL, "")
                    sheet.update_cell(row_idx, col_map[STATUS_COL], "تەواوکراوە")
                    sheet.update_cell(row_idx, col_map[LOG_COL], f"{log_entry}\n{old_log}")
                    
                    st.success(f"Done! Saved by {st.session_state.user_email}")
                    time.sleep(1); st.rerun()

    # --- بەشی بەڕێوەبردنی بەکارهێنەران (تەنها ئەدمین دەیبینێت) ---
    if st.session_state.user_email == "Admin":
        with main_tabs[1]:
            st.subheader("👥 User Management")
            new_user_email = st.text_input("New User Email:").lower().strip()
            new_user_pass = st.text_input("New User Password:", type="password")
            if st.button("Add Staff Account"):
                if new_user_email and new_user_pass:
                    users_ws.append_row([new_user_email, make_hashes(new_user_pass)])
                    st.success("User Added!")
                    time.sleep(1); st.rerun()
            st.divider()
            st.write("Current Staff Members:")
            st.table(users_data[['email']])

except Exception as e:
    st.error(f"System Error: {e}")
