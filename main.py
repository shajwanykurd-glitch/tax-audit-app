import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# ڕێکخستنی پەیوەندی بە گۆگڵ شیت
def get_data():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # لێرەدا ناوی ئەو فایلە JSON ە دەنووسین کە ناوتم نا "key.json"
    creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
    client = gspread.authorize(creds)
    # ناوی گۆگڵ شیتەکەت لێرە بنووسە
    sheet = client.open("site CIT QA - Tranche 4").sheet1 
    return pd.DataFrame(sheet.get_all_records())

st.set_page_config(page_title="سیستمی ئۆدیتی حکومی", layout="wide")
st.title("🔎 سیستمی وردبینی و ڤالیدەیشن (فەرمی)")

try:
    df = get_data()
    st.success("پەیوەندی بە گۆگڵ شیت سەرکەوتوو بوو ✅")
    
    if st.button("دەستپێکردنی ئۆدیتی داتاکان"):
        results = []
        for index, row in df.iterrows():
            errors = []
            # 1. Registration Check
            if not row.get('ID') or not row.get('Name'):
                errors.append("زانیاری ناسنامە کەمە")
            
            # 2. Salary Tax Check (بۆ نموونە ٥٪)
            salary = float(row.get('Salary', 0))
            tax_paid = float(row.get('Tax_Paid', 0))
            if tax_paid != (salary * 0.05):
                errors.append(f"هەڵە لە باج: دەبێت {salary * 0.05} بێت")
            
            # 3. Annual Filing Check
            annual = float(row.get('Annual_Total', 0))
            if annual != (salary * 12):
                errors.append("کۆی ساڵانە لەگەڵ مانگانە یەکسان نییە")
            
            results.append("تەواوە ✅" if not errors else "❌ " + " | ".join(errors))
        
        df['ئەنجامی ئۆدیت'] = results
        st.table(df)

except Exception as e:
    st.error(f"کێشەیەک هەیە لە بەستنەوە: {e}")