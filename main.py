import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz
import time

# --- ڕێکخستنی شاشە دەبێت یەکەم کۆد بێت ---
st.set_page_config(page_title="Audit Platform", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

# --- سیستمی زمان (کوردی و ئینگلیزی) ---
if 'lang' not in st.session_state:
    st.session_state.lang = 'ku'

def set_lang(language):
    st.session_state.lang = language

# فەرهەنگی وشەکان
ui = {
    "app_title": {"ku": "⚡ پلاتفۆرمی مۆدێرنی وردبینی داتاکان", "en": "⚡ Modern Data Audit Platform"},
    "admin_panel": {"ku": "🔒 کۆنتڕۆڵی ئەدمین", "en": "🔒 Admin Panel"},
    "admin_pass": {"ku": "پاسۆردی ئەدمین:", "en": "Admin Password:"},
    "welcome_admin": {"ku": "بەخێربێیت! دەسەڵاتی تەواوت هەیە.", "en": "Welcome! Full access granted."},
    "wrong_pass": {"ku": "پاسۆرد هەڵەیە!", "en": "Incorrect Password!"},
    "select_sheet": {"ku": "📂 شیتێک هەڵبژێرە", "en": "📂 Select a Workspace"},
    "no_data": {"ku": "هیچ داتایەک نەدۆزرایەوە.", "en": "No data found."},
    "stats": {"ku": "📈 ئامارەکانی شیت", "en": "📈 Workspace Analytics"},
    "total_files": {"ku": "کۆی گشتی داتاکان", "en": "Total Records"},
    "completed": {"ku": "تەواوکراو ✅", "en": "Completed ✅"},
    "pending": {"ku": "نەکراو ⏳", "en": "Pending ⏳"},
    "progress": {"ku": "ڕێژەی تەواوبوون", "en": "Completion Rate"},
    "tab_pending": {"ku": "📝 لیستی کارەکان (Pending)", "en": "📝 Task Queue (Pending)"},
    "tab_completed": {"ku": "✅ ئەرشیف و دابەزاندن", "en": "✅ Archive & Export"},
    "search": {"ku": "🔍 گەڕان بەدوای زانیاری...", "en": "🔍 Search records..."},
    "select_file": {"ku": "📌 فایلێک هەڵبژێرە بۆ کارکردن:", "en": "📌 Select a record to process:"},
    "history": {"ku": "👁️‍🗨️ مێژووی گۆڕانکارییەکان (Audit Trail)", "en": "👁️‍🗨️ Audit Trail"},
    "edit_info": {"ku": "پشکنین و نوێکردنەوەی داتا", "en": "Data Inspection & Update"},
    "submit_btn": {"ku": "سەبمیتکردن و پەسەندکردن ✅", "en": "Submit & Approve ✅"},
    "download_btn": {"ku": "📥 دابەزاندنی داتابەیس بە Excel", "en": "📥 Export Database (CSV)"},
    "return_btn": {"ku": "گەڕاندنەوە بۆ کارمەند ↩️", "en": "Return to Queue ↩️"},
    "success_submit": {"ku": "سەرکەوتوو بوو! داتاکە نوێکرایەوە.", "en": "Success! Record updated."},
    "status_done": {"ku": "تەواوکراوە", "en": "Completed"},
    "status_pending": {"ku": "نەکراوە", "en": "Pending"},
}

def t(key):
    return ui.get(key, {}).get(st.session_state.lang, key)

# --- دیزاینی CSSی پێشکەوتوو (گونجاو بۆ دارک مۆد و لایت مۆد) ---
st.markdown("""
<style>
    /* شاردنەوەی لۆگۆ و بەشەکانی Streamlit بۆ ئەوەی وەک سایتێکی تایبەت دەربکەوێت */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* دیزاینی داشبۆردەکە */
    div[data-testid="metric-container"] {
        background-color: rgba(130, 130, 130, 0.08);
        border: 1px solid rgba(130, 130, 130, 0.2);
        border-radius: 12px;
        padding: 15px 20px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.05);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 15px rgba(0,0,0,0.1);
    }
    
    /* دیزاینی فۆڕمەکان */
    div[data-testid="stForm"] {
        background-color: rgba(130, 130, 130, 0.03);
        border: 1px solid rgba(130, 130, 130, 0.15);
        border-radius: 16px;
        padding: 25px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.04);
    }
    
    /* دیزاینی دوگمەکان */
    .stButton>button {
        border-radius: 8px;
        font-weight: 600;
        letter-spacing: 0.5px;
        transition: all 0.2s;
    }
    .stButton>button:hover {
        opacity: 0.85;
        transform: scale(1.01);
    }
</style>
""", unsafe_allow_html=True)

tz = pytz.timezone('Asia/Baghdad')

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

# --- سایتبار (بەشی لای چەپ) ---
with st.sidebar:
    st.markdown("### 🌐 Language / زمان")
    lang_col1, lang_col2 = st.columns(2)
    if lang_col1.button("کوردی 🇹🇯", use_container_width=True): set_lang("ku")
    if lang_col2.button("English 🇬🇧", use_container_width=True): set_lang("en")
    
    st.markdown("---")
    st.title(t("admin_panel"))
    admin_input = st.text_input(t("admin_pass"), type="password")
    is_admin = False
    if admin_input == st.secrets.get("admin_password", ""):
        is_admin = True
        st.success(t("welcome_admin"))
    elif admin_input:
        st.error(t("wrong_pass"))

# --- ڕووکاری سەرەکی سایتەکە ---
st.title(t("app_title"))
st.markdown("---")

try:
    spreadsheet = get_spreadsheet()
    sheet_names = [ws.title for ws in spreadsheet.worksheets()]
    
    col_sel, _ = st.columns([1, 2])
    with col_sel:
        selected_sheet_name = st.selectbox(t("select_sheet"), sheet_names)
    
    sheet = spreadsheet.worksheet(selected_sheet_name)
    raw_data = sheet.get_all_values()
    
    if not raw_data or len(raw_data) < 2:
        st.info(t("no_data"))
    else:
        headers = raw_data[0]
        unique_headers = []
        seen = {}
        for h in headers:
            h = str(h).strip() or "Empty_Column"
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h} ({seen[h]})")
            else:
                seen[h] = 0
                unique_headers.append(h)
                
        col_index_map = {unique_headers[i]: i + 1 for i in range(len(unique_headers))}
        df = pd.DataFrame(raw_data[1:], columns=unique_headers)
        
        STATUS_COL = "دۆخی فایل" # بۆ ئەوەی گۆگڵ شیتەکە تێک نەچێت بە کوردی دەمێنێتەوە
        LOG_COL = "مێژووی گۆڕانکارییەکان (Audit Log)"
        
        if STATUS_COL not in df.columns:
            df[STATUS_COL] = "نەکراوە"
        else:
            df[STATUS_COL] = df[STATUS_COL].fillna("نەکراوە").replace("", "نەکراوە")
            
        pending_df = df[df[STATUS_COL] != "تەواوکراوە"]
        completed_df = df[df[STATUS_COL] == "تەواوکراوە"]
        
        # --- داشبۆردی ئامارەکان (مۆدێرن) ---
        st.subheader(t("stats"))
        m1, m2, m3 = st.columns(3)
        m1.metric(t("total_files"), len(df))
        m2.metric(t("completed"), len(completed_df))
        m3.metric(t("pending"), len(pending_df))
        
        progress = len(completed_df) / len(df) if len(df) > 0 else 0
        st.progress(progress, text=f"{t('progress')}: {int(progress * 100)}%")
        st.markdown("---")
        
        # --- بەشی تابەکان ---
        if is_admin:
            tab1, tab2 = st.tabs([t("tab_pending"), t("tab_completed")])
        else:
            tab1 = st.container()
            st.subheader(t("tab_pending"))
            tab2 = None

        # بەشی فایلی کارمەندەکان
        with tab1:
            search_query1 = st.text_input(t("search"), key="s1")
            show_pending_df = pending_df[pending_df.astype(str).apply(lambda x: x.str.contains(search_query1, case=False, na=False)).any(axis=1)] if search_query1 else pending_df

            st.dataframe(show_pending_df, use_container_width=True, height=250)

            if not show_pending_df.empty:
                st.markdown("<br>", unsafe_allow_html=True)
                options = [f"Row {idx + 2} | {row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'No Name'))}" for idx, row in show_pending_df.iterrows()]
                selected_option = st.selectbox(t("select_file"), ["---"] + options)
                
                if selected_option != "---":
                    actual_df_index = int(selected_option.split(" | ")[0].replace("Row ", "").strip()) - 2
                    actual_row_in_sheet = actual_df_index + 2
                    current_data = df.iloc[actual_df_index].to_dict()
                    
                    if is_admin:
                        with st.expander(t("history")):
                            st.text(current_data.get(LOG_COL, "No History"))
                    
                    with st.form("edit_form"):
                        st.write(f"##### 📝 {t('edit_info')}")
                        cols = st.columns(2)
                        new_data = {}
                        col_idx = 0
                        for key, value in current_data.items():
                            if key not in [LOG_COL, STATUS_COL] and not key.startswith("Empty_Column"):
                                new_data[key] = cols[col_idx % 2].text_input(f"{key}", value=str(value))
                                col_idx += 1
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        submit_button = st.form_submit_button(t("submit_btn"), use_container_width=True)
                        
                        if submit_button:
                            changes = [f"[{k}] changed from '{current_data[k]}' to '{new_data[k]}'" for k in new_data if str(current_data[k]) != str(new_data[k])]
                            
                            with st.spinner('Saving Data...'):
                                for col_to_check in [LOG_COL, STATUS_COL]:
                                    if col_to_check not in unique_headers:
                                        new_idx = len(unique_headers) + 1
                                        sheet.update_cell(1, new_idx, col_to_check)
                                        unique_headers.append(col_to_check)
                                        col_index_map[col_to_check] = new_idx
                                
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                                updated_log = f"🔹 Verified ({now_str}):\n" + "\n".join(changes) + f"\n\n{current_data.get(LOG_COL, '')}".strip()
                                
                                for key in new_data:
                                    if str(current_data[key]) != str(new_data[key]):
                                        sheet.update_cell(actual_row_in_sheet, col_index_map[key], new_data[key])
                                        
                                sheet.update_cell(actual_row_in_sheet, col_index_map[STATUS_COL], "تەواوکراوە")
                                sheet.update_cell(actual_row_in_sheet, col_index_map[LOG_COL], updated_log)
                                
                                st.success(t("success_submit"))
                                time.sleep(1)
                                st.rerun()

        # بەشی ئەدمین و ئەرشیف
        if is_admin and tab2 is not None:
            with tab2:
                csv_data = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(label=t("download_btn"), data=csv_data, file_name=f"Audit_Report.csv", mime="text/csv", type="primary")
                st.markdown("---")
                
                search_query2 = st.text_input(t("search"), key="s2")
                show_completed_df = completed_df[completed_df.astype(str).apply(lambda x: x.str.contains(search_query2, case=False, na=False)).any(axis=1)] if search_query2 else completed_df

                st.dataframe(show_completed_df, use_container_width=True, height=250)
                
                if not show_completed_df.empty:
                    options2 = [f"Row {idx + 2} | {row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'No Name'))}" for idx, row in show_completed_df.iterrows()]
                    selected_completed = st.selectbox(t("select_file"), ["---"] + options2)
                    
                    if selected_completed != "---":
                        actual_df_index2 = int(selected_completed.split(" | ")[0].replace("Row ", "").strip()) - 2
                        
                        with st.expander(t("history"), expanded=True):
                            st.text(df.iloc[actual_df_index2].to_dict().get(LOG_COL, ""))
                            
                        if st.button(t("return_btn")):
                            with st.spinner('Returning to queue...'):
                                sheet.update_cell(actual_df_index2 + 2, col_index_map[STATUS_COL], "نەکراوە")
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                                old_log = df.iloc[actual_df_index2].to_dict().get(LOG_COL, "")
                                sheet.update_cell(actual_df_index2 + 2, col_index_map[LOG_COL], f"❌ Rejected by Admin ({now_str})\n\n{old_log}")
                                st.success(t("success_submit"))
                                time.sleep(1)
                                st.rerun()

except Exception as e:
    st.error(f"Error connecting to server: {e}")
