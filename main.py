import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz
import time

st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", layout="wide", initial_sidebar_state="expanded")
tz = pytz.timezone('Asia/Baghdad')

@st.cache_resource
def get_spreadsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    
    pk = key_dict["private_key"]
    pk = pk.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
    pk = pk.replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client.open("site CIT QA - Tranche 4")

# --- بەشی تەنیشت (Sidebar) بۆ ئەدمین ---
st.sidebar.title("🔒 کۆنتڕۆڵی ئەدمین")
admin_input = st.sidebar.text_input("پاسۆردی ئەدمین بنووسە:", type="password")
is_admin = False
if admin_input == st.secrets.get("admin_password", ""):
    is_admin = True
    st.sidebar.success("بەخێربێیت بەڕێوەبەر! دەسەڵاتی تەواوت هەیە.")
else:
    if admin_input:
        st.sidebar.error("پاسۆرد هەڵەیە!")

st.title("🏢 پلاتفۆرمی وردبینی و نوێکردنەوەی داتاکان")
st.markdown("---")

try:
    spreadsheet = get_spreadsheet()
    worksheets = spreadsheet.worksheets()
    sheet_names = [ws.title for ws in worksheets]
    
    col1, col2 = st.columns([1, 2])
    with col1:
        selected_sheet_name = st.selectbox("📂 کام شیت دەتەوێت بیکەیتەوە؟", sheet_names)
    
    sheet = spreadsheet.worksheet(selected_sheet_name)
    raw_data = sheet.get_all_values()
    
    if not raw_data or len(raw_data) < 2:
        st.warning(f"هیچ داتایەک لەناو شیتی '{selected_sheet_name}' نەدۆزرایەوە.")
    else:
        headers = raw_data[0]
        
        # ڕێکخستنی ستوونەکان
        unique_headers = []
        seen = {}
        for h in headers:
            h = str(h).strip()
            if not h: h = "ستوونی_بەتاڵ"
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h} ({seen[h]})")
            else:
                seen[h] = 0
                unique_headers.append(h)
                
        col_index_map = {unique_headers[i]: i + 1 for i in range(len(unique_headers))}
        data_rows = raw_data[1:]
        df = pd.DataFrame(data_rows, columns=unique_headers)
        
        # دانانی ستوونی دۆخی فایل ئەگەر نەبوو
        STATUS_COL = "دۆخی فایل"
        LOG_COL = "مێژووی گۆڕانکارییەکان (Audit Log)"
        
        if STATUS_COL not in df.columns:
            df[STATUS_COL] = "نەکراوە"
        else:
            df[STATUS_COL] = df[STATUS_COL].fillna("نەکراوە").replace("", "نەکراوە")
            
        # جیاکردنەوەی داتاکان بەپێی دۆخ
        pending_df = df[df[STATUS_COL] != "تەواوکراوە"]
        completed_df = df[df[STATUS_COL] == "تەواوکراوە"]
        
        st.markdown("---")
        
        # ئەگەر ئەدمین بێت دوو تاب دەبینێت، ئەگەر نا تەنها یەک تاب
        if is_admin:
            tab1, tab2 = st.tabs(["📝 لیستی نەکراوەکان (بۆ کارمەند)", "✅ لیستی کراوەکان (تایبەت بە ئەدمین)"])
        else:
            tab1 = st.container()
            st.subheader("📝 لیستی نەکراوەکان (Pending)")
            tab2 = None # کارمەند ئەمە نابینێت

        # ==========================================
        # بەشی یەکەم: لیستی نەکراوەکان (Pending)
        # ==========================================
        with tab1:
            search_query1 = st.text_input("🔍 گەڕان لەناو فایلە نەکراوەکان:", placeholder="ناوی کۆمپانیا، مۆڵەت...")
            
            if search_query1:
                mask = pending_df.astype(str).apply(lambda x: x.str.contains(search_query1, case=False, na=False)).any(axis=1)
                show_pending_df = pending_df[mask]
            else:
                show_pending_df = pending_df

            st.caption(f"📊 {len(show_pending_df)} فایلی نەکراوە ماوە")
            st.dataframe(show_pending_df, use_container_width=True, height=200)

            if not show_pending_df.empty:
                st.markdown("---")
                options = []
                for idx, row in show_pending_df.iterrows():
                    company_name = row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))
                    options.append(f"ڕیزی {idx + 2} - {company_name}")
                    
                selected_option = st.selectbox("📌 هەڵبژاردنی فایلێک بۆ پشکنین و سەبمیتکردن:", ["هەڵبژێرە..."] + options, key="pending_select")
                
                if selected_option != "هەڵبژێرە...":
                    actual_df_index = int(selected_option.split(" - ")[0].replace("ڕیزی", "").strip()) - 2
                    actual_row_in_sheet = actual_df_index + 2
                    current_data = df.iloc[actual_df_index].to_dict()
                    
                    if is_admin:
                        log_data = current_data.get(LOG_COL, "هیچ مێژوویەک نییە")
                        with st.expander("👁️‍🗨️ مێژووی ئەم فایلە", expanded=False):
                            st.text(log_data)
                    
                    with st.form("edit_form"):
                        cols = st.columns(2)
                        col_idx = 0
                        new_data = {}
                        
                        for key, value in current_data.items():
                            if key not in [LOG_COL, STATUS_COL] and not key.startswith("ستوونی_بەتاڵ"):
                                new_val = cols[col_idx % 2].text_input(f"{key}", value=str(value))
                                new_data[key] = new_val
                                col_idx += 1
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        submit_button = st.form_submit_button("سەبمیتکردن و ناردن بۆ لیستی کراوەکان ✅", use_container_width=True)
                        
                        if submit_button:
                            changes_made = ["[دۆخی فایل] گۆڕدرا بۆ 'تەواوکراوە'"]
                            for key in new_data:
                                if str(current_data[key]) != str(new_data[key]):
                                    changes_made.append(f"[{key}] گۆڕدرا لە '{current_data[key]}' بۆ '{new_data[key]}'")
                            
                            with st.spinner('خەریکی نوێکردنەوە و گواستنەوەیە...'):
                                # دروستکردنی ستوونەکان ئەگەر نەبوون
                                for col_to_check in [LOG_COL, STATUS_COL]:
                                    if col_to_check not in unique_headers:
                                        new_idx = len(unique_headers) + 1
                                        sheet.update_cell(1, new_idx, col_to_check)
                                        unique_headers.append(col_to_check)
                                        col_index_map[col_to_check] = new_idx
                                
                                log_col_index = col_index_map[LOG_COL]
                                status_col_index = col_index_map[STATUS_COL]
                                
                                current_log = current_data.get(LOG_COL, "")
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                new_log_entry = f"🔹 پەسەندکرا ({now_str}):\n" + "\n".join(changes_made)
                                updated_log = f"{new_log_entry}\n\n{current_log}".strip()
                                
                                # ناردنی داتاکان
                                for key in new_data:
                                    if str(current_data[key]) != str(new_data[key]):
                                        sheet.update_cell(actual_row_in_sheet, col_index_map[key], new_data[key])
                                        
                                sheet.update_cell(actual_row_in_sheet, status_col_index, "تەواوکراوە")
                                sheet.update_cell(actual_row_in_sheet, log_col_index, updated_log)
                                
                                st.success("سەرکەوتوو بوو! فایلەکە چوو بۆ لیستی کراوەکان.")
                                time.sleep(1.5)
                                st.rerun() # ئەمە لاپەڕەکە ڕیفرێش دەکات تا فایلەکە ون بێت

        # ==========================================
        # بەشی دووەم: لیستی کراوەکان (تایبەت بە ئەدمین)
        # ==========================================
        if is_admin and tab2 is not None:
            with tab2:
                search_query2 = st.text_input("🔍 گەڕان لەناو فایلە تەواوکراوەکان:", placeholder="ناوی کۆمپانیا...")
                
                if search_query2:
                    mask = completed_df.astype(str).apply(lambda x: x.str.contains(search_query2, case=False, na=False)).any(axis=1)
                    show_completed_df = completed_df[mask]
                else:
                    show_completed_df = completed_df

                st.caption(f"📊 {len(show_completed_df)} فایل پێداچوونەوەیان بۆ کراوە")
                st.dataframe(show_completed_df, use_container_width=True, height=200)
                
                if not show_completed_df.empty:
                    st.markdown("---")
                    options2 = []
                    for idx, row in show_completed_df.iterrows():
                        company_name = row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))
                        options2.append(f"ڕیزی {idx + 2} - {company_name}")
                        
                    selected_completed = st.selectbox("📌 هەڵبژاردنی فایلێک بۆ گەڕاندنەوە:", ["هەڵبژێرە..."] + options2, key="completed_select")
                    
                    if selected_completed != "هەڵبژێرە...":
                        actual_df_index2 = int(selected_completed.split(" - ")[0].replace("ڕیزی", "").strip()) - 2
                        actual_row_in_sheet2 = actual_df_index2 + 2
                        current_data2 = df.iloc[actual_df_index2].to_dict()
                        
                        log_data2 = current_data2.get(LOG_COL, "هیچ مێژوویەک نییە")
                        with st.expander("👁️‍🗨️ بینینی مێژووی کاری کارمەندەکان لەسەر ئەم فایلە", expanded=True):
                            st.text(log_data2)
                            
                        st.warning("ئایا دڵنیایت کە دەتەوێت ئەم فایلە بگەڕێنیتەوە بۆ کارمەندەکان تا دووبارە پشکنینی بۆ بکەن؟")
                        if st.button("گەڕاندنەوە بۆ لیستی نەکراوەکان ↩️", type="primary"):
                            with st.spinner('خەریکی گەڕاندنەوەیە...'):
                                status_col_index = col_index_map[STATUS_COL]
                                log_col_index = col_index_map[LOG_COL]
                                
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                new_log_entry = f"❌ ڕەتکرایەوە لەلایەن ئەدمینەوە ({now_str}):\n[دۆخی فایل] گەڕێندرایەوە بۆ 'نەکراوە'"
                                updated_log = f"{new_log_entry}\n\n{log_data2}".strip()
                                
                                sheet.update_cell(actual_row_in_sheet2, status_col_index, "نەکراوە")
                                sheet.update_cell(actual_row_in_sheet2, log_col_index, updated_log)
                                
                                st.success("فایلەکە گەڕێندرایەوە بۆ لیستی کارمەندەکان!")
                                time.sleep(1.5)
                                st.rerun()

except Exception as e:
    st.error(f"کێشەیەک هەیە لە بەستنەوە: {e}")
