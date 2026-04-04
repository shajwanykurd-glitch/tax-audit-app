import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import textwrap

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_dict = json.loads(st.secrets["json_key"], strict=False)
    
    # چارەسەری کێشەی کلیلەکەی گۆگڵ
    pk = key_dict["private_key"]
    pk = pk.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
    pk = pk.replace("\\n", "")
    pk = "".join(pk.split())
    pk = "\n".join(textwrap.wrap(pk, 64))
    key_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    
    # ناوی گۆگڵ شیتەکەت
    return client.open("site CIT QA - Tranche 4").sheet1 

st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", layout="wide")
st.title("🔎 سیستمی وردبینی و ڤالیدەیشن (دەستی)")

try:
    sheet = get_sheet()
    data = sheet.get_all_records()

    if not data:
        st.warning("هیچ داتایەک لە گۆگڵ شیتەکەدا نییە.")
    else:
        # ١. بەشی هەڵبژاردنی فایل بۆ ئۆدیتۆر
        st.subheader("١. هەڵبژاردنی فایل")
        options = []
        for i, row in enumerate(data):
            # هەوڵدەدات ناوی کەسەکە بهێنێت بۆ ناو لیستەکە، ئەگەر نا ڕیزی ژمارەکە دەهێنێت
            identifier = row.get('Name', row.get('ناوی سیانی', row.get('ID', f"فایلی بێ ناو")))
            options.append(f"ڕیزی {i+2} | {identifier}")
            
        selected_option = st.selectbox("📌 ناو یان ژمارەی فایلێک هەڵبژێرە بۆ وردبینی:", options)
        
        # دۆزینەوەی ئەو ڕیزەی کە هەڵبژێردراوە
        selected_index = options.index(selected_option)
        actual_row_in_sheet = selected_index + 2 # لەبەر ئەوەی ڕیزی یەکەم هێدەرە
        row_data = data[selected_index]

        # ٢. نیشاندانی زانیارییەکان بۆ پشکنین
        st.markdown("---")
        st.subheader("٢. زانیارییەکانی ئەم فایلە")
        st.info("سەیری ئەم زانیارییانەی خوارەوە بکە و بڕیار بدە کە دروستن یان نا.")
        st.json(row_data) # داتاکە بە شێوەیەکی جوان و ڕوون نیشان دەدات

        # ٣. فۆڕمی سەبمیتکردن و بڕیاردان
        st.markdown("---")
        st.subheader("٣. بڕیاری کۆتایی ئۆدیتۆر")
        
        with st.form("audit_form"):
            status = st.radio("ئەنجامی وردبینی:", ["پەسەندکرا ✅", "ڕەتکرایەوە ❌ (کێشەی هەیە)"])
            notes = st.text_area("تێبینی ئۆدیتۆر (ئەگەر کێشەی هەیە لێرە هۆکارەکەی بنووسە):")
            
            # دوگمەی سەبمیت
            submit = st.form_submit_button("سەبمیت کردنی بڕیار 💾")

            if submit:
                # کاتێک کلیک لە سەبمیت دەکرێت، دەچێت لە گۆگڵ شیتەکەدا ستوونی بڕیار دروست دەکات (ئەگەر نەبێت) و داتاکە سەیڤ دەکات
                headers = sheet.row_values(1)
                
                if "بڕیاری ئۆدیتۆر" not in headers:
                    sheet.update_cell(1, len(headers) + 1, "بڕیاری ئۆدیتۆر")
                    sheet.update_cell(1, len(headers) + 2, "تێبینی ئۆدیتۆر")
                    headers = sheet.row_values(1) # نوێکردنەوەی هێدەرەکان
                    
                col_status = headers.index("بڕیاری ئۆدیتۆر") + 1
                col_notes = headers.index("تێبینی ئۆدیتۆر") + 1

                # ناردنی بڕیارەکە و تێبینییەکە بۆ ناو گۆگڵ شیتەکە ڕێک لە تەنیشت ناوی کەسەکە
                sheet.update_cell(actual_row_in_sheet, col_status, status)
                sheet.update_cell(actual_row_in_sheet, col_notes, notes)

                st.success("کارەکە سەرکەوتوو بوو! بڕیارەکەت ڕاستەوخۆ لە گۆگڵ شیتەکە پاشکەوت کرا. ✅")

except Exception as e:
    st.error(f"کێشەیەک هەیە لە بەستنەوە: {e}")
