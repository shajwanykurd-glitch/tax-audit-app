import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz

# دیزاینی لاپەڕە (بۆ شاشەی گەورە)
st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", layout="wide")

# ڕێکخستنی کات بەپێی کاتی عێراق/هەولێر
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

st.title("🏢 پلاتفۆرمی وردبینی و نوێکردنەوەی داتاکان")
st.markdown("---")

try:
    sheet = get_sheet()
    # هێنانی داتاکان و دروستکردنی داتافڕەیم
    records = sheet.get_all_records()
    
    if not records:
        st.warning("هیچ داتایەک نەدۆزرایەوە.")
    else:
        df = pd.DataFrame(records)
        
        # --- بەشی یەکەم: بزوێنەری گەڕانی پێشکەوتوو ---
        st.subheader("🔍 گەڕانی پێشکەوتوو")
        search_query = st.text_input("لێرە بگەڕێ بەپێی (ناوی بریکار، ئیمەیڵ، ژمارەی بایندەر، ناوی کۆمپانیا، یان ژمارەی مۆڵەت):", placeholder="وشەیەک بنووسە بۆ گەڕان...")
        
        # فلتەرکردنی داتاکان ئەگەر شتێک نووسرابێت
        if search_query:
            # گەڕان بەناو هەموو ستوونەکاندا دەکات بۆ ئەوەی دڵنیا بین هیچیمان لەدەست ناچێت
            mask = df.astype(str).apply(lambda x: x.str.contains(search_query, case=False, na=False)).any(axis=1)
            filtered_df = df[mask]
        else:
            filtered_df = df

        st.caption(f"ئەنجام: {len(filtered_df)} فایل دۆزرایەوە")

        if not filtered_df.empty:
            # دروستکردنی لیستێکی جوان بۆ ئەنجامەکان
            options = []
            for idx, row in filtered_df.iterrows():
                # هەوڵدەدات ناوێک دروست بکات بۆ لیستەکە کە ڕوون بێت
                company_name = row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))
                agent_name = row.get('Agent name', row.get('Name', 'بێ بریکار'))
                options.append(f"ڕیزی {idx + 2} | کۆمپانیا: {company_name} | بریکار: {agent_name}")
            
            selected_option = st.selectbox("📌 یەکێک لەم فایلانەی خوارەوە هەڵبژێرە بۆ دەستکاریکردن:", options)
            
            # --- بەشی دووەم: فۆڕمی دەستکاریکردن (Edit Form) ---
            if selected_option:
                # دۆزینەوەی ئیندێکسی ڕاستەقینە لەناو داتابەیسەکە
                actual_df_index = int(selected_option.split("|")[0].replace("ڕیزی", "").strip()) - 2
                actual_row_in_sheet = actual_df_index + 2
                current_data = df.iloc[actual_df_index].to_dict()
                
                st.markdown("---")
                st.subheader(f"📝 دەستکاریکردنی زانیارییەکان (فایلی ڕیزی {actual_row_in_sheet})")
                
                with st.form("edit_form"):
                    st.info("دەتوانیت هەر خانەیەک کە کێشەی هەیە دەستکاری بکەیت. گۆڕانکارییەکان بە ناوی ئەم کاتەوە تۆمار دەکرێن.")
                    
                    new_data = {}
                    # دروستکردنی خانەی دەستکاریکردن بە شێوەی دوو ستوونی بۆ ئەوەی جوانتر بێت
                    cols = st.columns(2)
                    col_idx = 0
                    
                    for key, value in current_data.items():
                        # نامانەوێت خانەی مێژوو دەستکاری بکرێت
                        if key != "مێژووی گۆڕانکارییەکان (Audit Log)":
                            # پیشاندانی داتای کۆن لەناو خانەکەدا بۆ ئەوەی ئاسان بێت
                            new_val = cols[col_idx % 2].text_input(f"لەبەرنامەدا: {key}", value=str(value))
                            new_data[key] = new_val
                            col_idx += 1
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    submit_button = st.form_submit_button("سەبمیتکردن و نوێکردنەوەی فایلەکە 💾", use_container_width=True)
                    
                    if submit_button:
                        # --- بەشی سێیەم: بەراوردکردن و تۆمارکردنی ڤێرژنەکان ---
                        changes_made = []
                        for key in new_data:
                            old_val = str(current_data[key])
                            new_val = str(new_data[key])
                            if old_val != new_val:
                                changes_made.append(f"[{key}] گۆڕدرا لە '{old_val}' بۆ '{new_val}'")
                        
                        if changes_made:
                            with st.spinner('خەریکی پاشەکەوتکردن و دروستکردنی ڤێرژنی نوێیە...'):
                                headers = sheet.row_values(1)
                                
                                # دڵنیابوونەوە لە بوونی ستوونی مێژوو
                                log_col_name = "مێژووی گۆڕانکارییەکان (Audit Log)"
                                if log_col_name not in headers:
                                    sheet.update_cell(1, len(headers) + 1, log_col_name)
                                    headers.append(log_col_name)
                                
                                log_col_index = headers.index(log_col_name) + 1
                                
                                # هێنانی مێژووی پێشوو ئەگەر هەبێت
                                current_log = current_data.get(log_col_name, "")
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                new_log_entry = f"🔹 ڤێرژنی نوێ ({now_str}):\n" + "\n".join(changes_made)
                                
                                updated_log = f"{new_log_entry}\n\n{current_log}".strip()
                                
                                # نوێکردنەوەی خانەکان لە گۆگڵ شیت
                                for key in new_data:
                                    if str(current_data[key]) != str(new_data[key]):
                                        col_index = headers.index(key) + 1
                                        sheet.update_cell(actual_row_in_sheet, col_index, new_data[key])
                                
                                # نوێکردنەوەی ستوونی مێژوو
                                sheet.update_cell(actual_row_in_sheet, log_col_index, updated_log)
                                
                                st.success("کارەکە سەرکەوتوو بوو! هەموو گۆڕانکارییەکان بە کات و بەروارەوە تۆمارکران. ✅")
                                st.balloons()
                        else:
                            st.warning("هیچ گۆڕانکارییەک نەکراوە بۆ ئەوەی سەیڤ بکرێت.")

except Exception as e:
    st.error(f"کێشەیەک هەیە لە بەستنەوە: {e}")
