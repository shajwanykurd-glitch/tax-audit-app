import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap
from datetime import datetime
import pytz
import time

# ڕێکخستنی شاشە و ئایکۆن
st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", page_icon="📊", layout="wide", initial_sidebar_state="expanded")
tz = pytz.timezone('Asia/Baghdad')

# زیادکردنی دیزاینی تایبەت (CSS) بۆ جوانکردنی ڕووکارەکە
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .metric-value { font-size: 2rem; font-weight: bold; color: #1f77b4; }
    .metric-label { font-size: 1rem; color: #555; }
    div[data-testid="stForm"] { border: 2px solid #e6e6e6; border-radius: 10px; padding: 20px; }
</style>
""", unsafe_allow_html=True)

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

# --- سایتبار (Sidebar) ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3135/3135679.png", width=100) # ئایکۆنێکی جوان
    st.title("🔒 کۆنتڕۆڵی ئەدمین")
    admin_input = st.text_input("پاسۆردی ئەدمین بنووسە:", type="password")
    is_admin = False
    if admin_input == st.secrets.get("admin_password", ""):
        is_admin = True
        st.success("بەخێربێیت بەڕێوەبەر! دەسەڵاتی تەواوت هەیە.")
    elif admin_input:
        st.error("پاسۆرد هەڵەیە!")

# --- بەشی سەرەکی ---
st.title("📊 پلاتفۆرمی وردبینی و داتا ئەنەلەیسس")
st.markdown("---")

try:
    spreadsheet = get_spreadsheet()
    worksheets = spreadsheet.worksheets()
    sheet_names = [ws.title for ws in worksheets]
    
    col_sel, _ = st.columns([1, 2])
    with col_sel:
        selected_sheet_name = st.selectbox("📂 کام شیت دەتەوێت بیکەیتەوە؟", sheet_names)
    
    sheet = spreadsheet.worksheet(selected_sheet_name)
    raw_data = sheet.get_all_values()
    
    if not raw_data or len(raw_data) < 2:
        st.info(f"هیچ داتایەک لەناو شیتی '{selected_sheet_name}' نەدۆزرایەوە.")
    else:
        headers = raw_data[0]
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
        df = pd.DataFrame(raw_data[1:], columns=unique_headers)
        
        STATUS_COL = "دۆخی فایل"
        LOG_COL = "مێژووی گۆڕانکارییەکان (Audit Log)"
        if STATUS_COL not in df.columns:
            df[STATUS_COL] = "نەکراوە"
        else:
            df[STATUS_COL] = df[STATUS_COL].fillna("نەکراوە").replace("", "نەکراوە")
            
        pending_df = df[df[STATUS_COL] != "تەواوکراوە"]
        completed_df = df[df[STATUS_COL] == "تەواوکراوە"]
        
        # --- بەشی داشبۆرد و ئامارەکان (نوێ) ---
        total_files = len(df)
        total_completed = len(completed_df)
        total_pending = len(pending_df)
        progress = total_completed / total_files if total_files > 0 else 0
        
        st.subheader("📈 ئامارەکانی ئەم شیتە")
        m1, m2, m3 = st.columns(3)
        m1.markdown(f'<div class="metric-card"><div class="metric-value">{total_files}</div><div class="metric-label">کۆی گشتی فایلەکان</div></div>', unsafe_allow_html=True)
        m2.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #2ca02c;">{total_completed}</div><div class="metric-label">تەواوکراوەکان ✅</div></div>', unsafe_allow_html=True)
        m3.markdown(f'<div class="metric-card"><div class="metric-value" style="color: #d62728;">{total_pending}</div><div class="metric-label">نەکراوەکان ⏳</div></div>', unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.progress(progress, text=f"ڕێژەی تەواوبوونی کارەکان: {int(progress * 100)}%")
        st.markdown("---")
        
        # تابی کارمەند و ئەدمین
        if is_admin:
            tab1, tab2 = st.tabs(["📝 لیستی نەکراوەکان (کارمەند)", "✅ لیستی کراوەکان و دابەزاندن (ئەدمین)"])
        else:
            tab1 = st.container()
            st.subheader("📝 لیستی نەکراوەکان (Pending)")
            tab2 = None

        with tab1:
            search_query1 = st.text_input("🔍 گەڕان (ناوی کۆمپانیا، مۆڵەت...):", placeholder="لێرە بگەڕێ...", key="s1")
            show_pending_df = pending_df[pending_df.astype(str).apply(lambda x: x.str.contains(search_query1, case=False, na=False)).any(axis=1)] if search_query1 else pending_df

            st.dataframe(show_pending_df, use_container_width=True, height=200)

            if not show_pending_df.empty:
                st.markdown("<br>", unsafe_allow_html=True)
                options = [f"ڕیزی {idx + 2} - {row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))}" for idx, row in show_pending_df.iterrows()]
                selected_option = st.selectbox("📌 هەڵبژاردنی فایلێک بۆ سەبمیتکردن:", ["هەڵبژێرە..."] + options)
                
                if selected_option != "هەڵبژێرە...":
                    actual_df_index = int(selected_option.split(" - ")[0].replace("ڕیزی", "").strip()) - 2
                    actual_row_in_sheet = actual_df_index + 2
                    current_data = df.iloc[actual_df_index].to_dict()
                    
                    if is_admin:
                        with st.expander("👁️‍🗨️ مێژووی ئەم فایلە"):
                            st.text(current_data.get(LOG_COL, "هیچ مێژوویەک نییە"))
                    
                    with st.form("edit_form"):
                        st.write("##### 📝 پشکنین و نوێکردنەوەی زانیارییەکان")
                        cols = st.columns(2)
                        new_data = {}
                        col_idx = 0
                        for key, value in current_data.items():
                            if key not in [LOG_COL, STATUS_COL] and not key.startswith("ستوونی_بەتاڵ"):
                                new_data[key] = cols[col_idx % 2].text_input(f"{key}", value=str(value))
                                col_idx += 1
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        submit_button = st.form_submit_button("سەبمیتکردن و ناردن بۆ لیستی کراوەکان ✅", use_container_width=True)
                        
                        if submit_button:
                            changes = ["[دۆخی فایل] گۆڕدرا بۆ 'تەواوکراوە'"] + [f"[{k}] گۆڕدرا لە '{current_data[k]}' بۆ '{new_data[k]}'" for k in new_data if str(current_data[k]) != str(new_data[k])]
                            
                            with st.spinner('پاشەکەوت دەکرێت...'):
                                for col_to_check in [LOG_COL, STATUS_COL]:
                                    if col_to_check not in unique_headers:
                                        new_idx = len(unique_headers) + 1
                                        sheet.update_cell(1, new_idx, col_to_check)
                                        unique_headers.append(col_to_check)
                                        col_index_map[col_to_check] = new_idx
                                
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                updated_log = f"🔹 پەسەندکرا ({now_str}):\n" + "\n".join(changes) + f"\n\n{current_data.get(LOG_COL, '')}".strip()
                                
                                for key in new_data:
                                    if str(current_data[key]) != str(new_data[key]):
                                        sheet.update_cell(actual_row_in_sheet, col_index_map[key], new_data[key])
                                        
                                sheet.update_cell(actual_row_in_sheet, col_index_map[STATUS_COL], "تەواوکراوە")
                                sheet.update_cell(actual_row_in_sheet, col_index_map[LOG_COL], updated_log)
                                
                                st.success("سەرکەوتوو بوو! فایلەکە چوو بۆ لیستی کراوەکان.")
                                time.sleep(1.5)
                                st.rerun()

        # --- تابی ئەدمین (کراوەکان و دابەزاندن) ---
        if is_admin and tab2 is not None:
            with tab2:
                # بەشی دابەزاندنی فایل
                csv_data = df.to_csv(index=False).encode('utf-8-sig') # utf-8-sig بۆ ئەوەیە کوردییەکەی تێک نەچێت لە ئێکسڵ
                st.download_button(
                    label="📥 دابەزاندنی هەموو داتاکان بە فۆرماتی Excel (CSV)",
                    data=csv_data,
                    file_name=f"Audit_Report_{selected_sheet_name}.csv",
                    mime="text/csv",
                    type="primary"
                )
                st.markdown("---")
                
                search_query2 = st.text_input("🔍 گەڕان لەناو تەواوکراوەکان:", key="s2")
                show_completed_df = completed_df[completed_df.astype(str).apply(lambda x: x.str.contains(search_query2, case=False, na=False)).any(axis=1)] if search_query2 else completed_df

                st.dataframe(show_completed_df, use_container_width=True, height=200)
                
                if not show_completed_df.empty:
                    options2 = [f"ڕیزی {idx + 2} - {row.get('اسم الشركة / کۆمپانیای / Company Name', row.get('Company Name', 'بێ ناو'))}" for idx, row in show_completed_df.iterrows()]
                    selected_completed = st.selectbox("📌 گەڕاندنەوەی فایل بۆ کارمەندەکان:", ["هەڵبژێرە..."] + options2)
                    
                    if selected_completed != "هەڵبژێرە...":
                        actual_df_index2 = int(selected_completed.split(" - ")[0].replace("ڕیزی", "").strip()) - 2
                        
                        with st.expander("👁️‍🗨️ مێژووی ئەم فایلە", expanded=True):
                            st.text(df.iloc[actual_df_index2].to_dict().get(LOG_COL, ""))
                            
                        if st.button("گەڕاندنەوە بۆ لیستی نەکراوەکان ↩️"):
                            with st.spinner('خەریکی گەڕاندنەوەیە...'):
                                sheet.update_cell(actual_df_index2 + 2, col_index_map[STATUS_COL], "نەکراوە")
                                now_str = datetime.now(tz).strftime("%Y-%m-%d %I:%M:%S %p")
                                old_log = df.iloc[actual_df_index2].to_dict().get(LOG_COL, "")
                                sheet.update_cell(actual_df_index2 + 2, col_index_map[LOG_COL], f"❌ ڕەتکرایەوە لەلایەن ئەدمینەوە ({now_str})\n\n{old_log}")
                                st.success("فایلەکە گەڕێندرایەوە بۆ لیستی کارمەندەکان!")
                                time.sleep(1.5)
                                st.rerun()

except Exception as e:
    st.error(f"کێشەیەک هەیە: {e}")
