import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz

st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", layout="wide", initial_sidebar_state="expanded")
tz = pytz.timezone('Asia/Baghdad')

@st.cache_resource
def get_sheet():
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
    return client.open("site CIT QA - Tranche 4").sheet1 

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

# --- بەشی سەرەکی سایتەکە ---
st.title("🏢 پلاتفۆرمی وردبینی و نوێکردنەوەی داتاکان")
st.markdown("---")

try:
    sheet = get_sheet()
    records = sheet.get_all_records()
    
    if not records:
        st.warning("هیچ داتایەک نەدۆزرایەوە.")
    else:
        df = pd.DataFrame(records)
        
        # --- سیستمی گەڕانی نوێ (Table View) ---
        st.subheader("🔍 بزوێنەری گەڕان")
        search_query = st.text_input("وشەیەک بنووسە (ناوی کۆمپانیا، بریکار، ژمارەی مۆڵەت...):", placeholder="لێرە بگەڕێ...")
        
        if search_query:
            mask = df.astype(str).apply(lambda x: x.str.contains(search_query, case=False, na=False)).any(axis=1)
            filtered_df = df[mask]
        else:
            filtered_df = df

        st.caption(f"📊 ئەنجام: {len(filtered_df)} فایل دۆزرایەوە")
        
        # نیشاندانی داتاکان بە شێوەی خشتەیەکی جوان نەک لیست
        st.dataframe(filtered_df, use_container_width=True, height=200)

        if not filtered_df.empty:
            st.markdown("---")
            st.subheader("📝 دەستکاریکردنی فایل")
            
            # دروستکردنی لیستی ژمارەی ڕیزەکان بۆ ئەوەی کارمەند هەڵیبژێرێت
            options = []
            for idx, row in filtered_df.iterrows():
                company_name = row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))
                options.append(f"ڕیزی {idx + 2} - {company_name}")
                
            selected_option = st.selectbox("📌 ئەو ڕیزە هەڵبژێرە کە دەتەوێت دەستکاری بکەیت:", ["هەڵبژێرە..."] + options)
            
            if selected_option != "هەڵبژێرە...":
                actual_df_index = int(selected_option.split(" - ")[0].replace("ڕیزی", "").strip()) - 2
                actual_row_in_sheet = actual_df_index + 2
                current_data = df.iloc[actual_df_index].to_dict()
                
                # ئەگەر ئەدمین بێت، مێژووەکەی پێشان دەدەین
                if is_admin:
                    log_data = current_data.get("مێژووی گۆڕانکارییەکان (Audit Log)", "هیچ مێژوویەک نییە")
                    with st.expander("👁️‍🗨️ بینینی مێژووی گۆڕانکارییەکانی ئەم فایلە (تایبەت بە ئەدمین)", expanded=True):
                        st.text(log_data)
                
                with st.form("edit_form"):
                    cols = st.columns(2)
                    col_idx = 0
                    new_data = {}
                    
                    for key, value in current_data.items():
                        if key != "مێژووی گۆڕانکارییەکان (Audit Log)":
                            new_val = cols[col_idx % 2].text_input(f"{key}", value=str(value))
                            new_data[key] = new_val
                            col_idx += 1
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    submit_button = st.form_submit_button("سەبمیتکردن و نوێکردنەوە 💾", use_container_width=True)
                    
                    if submit_button:
                        changes_made = []
                        for key in new_data:
                            old_val = str(current_data[key])
                            new_val = str(new_data[key])
                            if old_val != new_val:
                                changes_made.append(f"[{key}] گۆڕدرا لە '{old_val}' بۆ '{new_val}'")
                        
                        if changes_made:
                            with st.spinner('خەریکی نوێکردنەوەیە...'):
                                headers = sheet.row_values(1)
                                log_col_name = "مێژووی گۆڕانکارییەکان (Audit Log)"
                                if log_col_name not in headers:
                                    sheet.update_cell(1, len(headers) + 1, log_col_name)
                                    headers.append(log_col_name)
                                
                                log_col_index = headers.index(log_col_name) + 1
                                current_log = current_data.get(log_col_name, "")
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                new_log_entry = f"🔹 ڤێرژنی نوێ ({now_str}):\n" + "\n".join(changes_made)
                                updated_log = f"{new_log_entry}\n\n{current_log}".strip()
                                
                                for key in new_data:
                                    if str(current_data[key]) != str(new_data[key]):
                                        col_index = headers.index(key) + 1
                                        sheet.update_cell(actual_row_in_sheet, col_index, new_data[key])
                                
                                sheet.update_cell(actual_row_in_sheet, log_col_index, updated_log)
                                st.success("کارەکە سەرکەوتوو بوو! گۆڕانکارییەکان تۆمارکران. ✅")
                        else:
                            st.warning("هیچ گۆڕانکارییەک نەکراوە.")

except Exception as e:
    st.error(f"کێشەیەک هەیە لە بەستنەوە: {e}")
