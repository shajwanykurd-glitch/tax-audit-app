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

# --- ١. ڕێکخستنی شاشە و دیزاین ---
st.set_page_config(page_title="Audit Pro Platform", page_icon="⚡", layout="wide")
tz = pytz.timezone('Asia/Baghdad')

# دیزاینی CSS بۆ دارک مۆد و جوانکردنی ڕووکار
st.markdown("""
<style>
    .stApp { max-width: 100%; }
    [data-testid="stMetricContainer"] {
        background-color: rgba(28, 131, 225, 0.1);
        border: 1px solid rgba(28, 131, 225, 0.2);
        padding: 15px;
        border-radius: 15px;
    }
    .stDataFrame { border-radius: 10px; overflow: hidden; }
    div[data-testid="stForm"] {
        background-color: rgba(130, 130, 130, 0.05);
        border-radius: 20px;
        padding: 30px;
        border: 1px solid rgba(130, 130, 130, 0.1);
    }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- ٢. سیستمی زمان ---
if 'lang' not in st.session_state: st.session_state.lang = 'ku'

ui = {
    "login_h": {"ku": "🔑 چوونەژوورەوەی پارێزراو", "en": "🔑 Secure Login"},
    "email": {"ku": "ئیمەیڵ (یان admin):", "en": "Email (or admin):"},
    "pass": {"ku": "پاسۆرد:", "en": "Password:"},
    "login_btn": {"ku": "چوونەژوورەوە", "en": "Login"},
    "logout": {"ku": "چوونەدەەرەوە", "en": "Logout"},
    "title": {"ku": "📊 پلاتفۆرمی وردبینی و داشبۆرد", "en": "📊 Audit Platform & Dashboard"},
    "stats": {"ku": "📈 ئامارەکان", "en": "📈 Analytics"},
    "total": {"ku": "کۆی گشتی", "en": "Total"},
    "done": {"ku": "تەواوکراو", "en": "Completed"},
    "pending": {"ku": "ماوە (Pending)", "en": "Pending"},
    "search": {"ku": "🔍 گەڕانی خێرا (ناو، ئیمەیڵ، کۆمپانیا...):", "en": "🔍 Fast Search (Name, Email, Company...):"},
    "user_mgmt": {"ku": "👥 بەڕێوەبردنی کارمەند", "en": "👥 User Management"},
    "audit_tab": {"ku": "📝 پشکنینی داتا", "en": "📝 Data Audit"},
    "archive_tab": {"ku": "✅ ئەرشیف", "en": "✅ Archive"},
    "submit": {"ku": "سەبمیت و پەسەندکردن ✅", "en": "Submit & Approve ✅"},
    "select_row": {"ku": "📌 ڕیزێک هەڵبژێرە بۆ دەستکاریکردن:", "en": "📌 Select a row to edit:"}
}
def t(key): return ui.get(key, {}).get(st.session_state.lang, key)

# --- ٣. بەستنەوەی گۆگڵ ---
@st.cache_resource
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    pk = key_dict["private_key"].replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    return gspread.authorize(creds).open("site CIT QA - Tranche 4")

def hash_pass(password): return hashlib.sha256(str.encode(password)).hexdigest()

# --- ٤. جێبەجێکردنی سیستم ---
try:
    spreadsheet = get_spreadsheet()
    all_sheets = [ws.title for ws in spreadsheet.worksheets()]
    
    if "UsersDB" not in all_sheets:
        users_ws = spreadsheet.add_worksheet(title="UsersDB", rows="100", cols="2")
        users_ws.append_row(["email", "password"])
    else:
        users_ws = spreadsheet.worksheet("UsersDB")

    if 'logged_in' not in st.session_state: st.session_state.logged_in = False

    # --- لای ڕاست/چەپ (Sidebar) ---
    with st.sidebar:
        st.markdown("### 🌐 Language / زمان")
        c1, c2 = st.columns(2)
        if c1.button("English"): st.session_state.lang = 'en'; st.rerun()
        if c2.button("کوردی"): st.session_state.lang = 'ku'; st.rerun()
        st.markdown("---")
        
        if not st.session_state.logged_in:
            st.title(t("login_h"))
            l_email = st.text_input(t("email")).lower().strip()
            l_pass = st.text_input(t("pass"), type="password")
            if st.button(t("login_btn"), use_container_width=True):
                if l_email == "admin" and l_pass == st.secrets["admin_password"]:
                    st.session_state.logged_in = True; st.session_state.user = "Admin"; st.rerun()
                else:
                    u_df = pd.DataFrame(users_ws.get_all_records())
                    if l_email in u_df['email'].values:
                        if hash_pass(l_pass) == u_df[u_df['email'] == l_email]['password'].values[0]:
                            st.session_state.logged_in = True; st.session_state.user = l_email; st.rerun()
                    st.error("Error!")
            st.stop()
        else:
            st.success(f"👤 {st.session_state.user}")
            if st.button(t("logout")): st.session_state.logged_in = False; st.rerun()

    # --- ناورۆکی سەرەکی دوای لۆگین ---
    st.title(t("title"))
    
    data_sheets = [n for n in all_sheets if n not in ["UsersDB", "Auditors"]]
    sel_sheet = st.selectbox("📂 Workspace / شیت", data_sheets)
    sheet = spreadsheet.worksheet(sel_sheet)
    
    raw = sheet.get_all_values()
    headers = []
    seen = {}
    for h in raw[0]:
        h = h.strip() or "Column"
        if h in seen: seen[h]+=1; headers.append(f"{h}_{seen[h]}")
        else: seen[h]=0; headers.append(h)
    
    df = pd.DataFrame(raw[1:], columns=headers)
    S_COL, L_COL = "دۆخی فایل", "مێژووی گۆڕانکارییەکان (Audit Log)"
    if S_COL not in df.columns: df[S_COL] = "نەکراوە"
    
    # --- داشبۆرد ---
    st.subheader(t("stats"))
    done_count = len(df[df[S_COL]=="تەواوکراوە"])
    pending_count = len(df[df[S_COL]!="تەواوکراوە"])
    m1, m2, m3 = st.columns(3)
    m1.metric(t("total"), len(df))
    m2.metric(t("done"), done_count)
    m3.metric(t("pending"), pending_count)
    st.progress(done_count/len(df) if len(df)>0 else 0)

    # --- بەشی گەڕان (گەڕاوەتەوە) ---
    st.markdown("---")
    search_q = st.text_input(t("search"), placeholder="Type here to filter...")
    
    if search_q:
        mask = df.astype(str).apply(lambda x: x.str.contains(search_q, case=False, na=False)).any(axis=1)
        filtered_df = df[mask]
    else:
        filtered_df = df

    # --- تابەکان ---
    t_list = [t("audit_tab"), t("archive_tab"), t("user_mgmt")] if st.session_state.user == "Admin" else [t("audit_tab")]
    tabs = st.tabs(t_list)

    with tabs[0]:
        display_df = filtered_df[filtered_df[S_COL] != "تەواوکراوە"]
        st.dataframe(display_df, use_container_width=True)
        
        row_sel = st.selectbox(t("select_row"), ["---"] + [f"Row {i+2}" for i in display_df.index])
        if row_sel != "---":
            idx = int(row_sel.split(" ")[1])
            curr_data = df.iloc[idx-2].to_dict()
            with st.form("audit_form"):
                st.write(f"📝 Editing: **Row {idx}**")
                new_vals = {}
                for k, v in curr_data.items():
                    if k not in [S_COL, L_COL]: new_vals[k] = st.text_input(k, value=str(v))
                
                if st.form_submit_button(t("submit"), use_container_width=True):
                    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                    # پشکنینی ستوونەکان
                    col_map = {h: i+1 for i, h in enumerate(headers)}
                    for c in [S_COL, L_COL]:
                        if c not in headers:
                            if len(headers)+1 > sheet.col_count: sheet.add_cols(2)
                            sheet.update_cell(1, len(headers)+1, c)
                            headers.append(c); col_map[c] = len(headers)

                    for k, v in new_vals.items():
                        if str(curr_data[k]) != str(v):
                            sheet.update_cell(idx, col_map[k], v)
                    
                    new_log = f"🔹 {st.session_state.user} @ {now}\n{curr_data.get(L_COL, '')}"
                    sheet.update_cell(idx, col_map[S_COL], "تەواوکراوە")
                    sheet.update_cell(idx, col_map[L_COL], new_log)
                    st.success("Success!"); time.sleep(1); st.rerun()

    if st.session_state.user == "Admin":
        with tabs[1]:
            st.dataframe(filtered_df[filtered_df[S_COL]=="تەواوکراوە"], use_container_width=True)
        with tabs[2]:
            st.subheader("👥 Add New Auditor")
            nu = st.text_input("New User Email").lower().strip()
            np = st.text_input("New User Password", type="password")
            if st.button("Save User"):
                users_ws.append_row([nu, hash_pass(np)])
                st.success("Done!"); time.sleep(1); st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
