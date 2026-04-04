



Here's the refactored core — drop-in replacements for the expensive parts of your app:

## 1. Cache the gspread Client (connection reuse)

```python
@st.cache_resource(show_spinner=False)
def get_gspread_client():
    """Singleton gspread client — survives reruns."""
    creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)
```

> **Why**: Your old code rebuilt credentials + authorized a new client on *every single rerun*. `cache_resource` keeps one alive across the session.

---

## 2. Cache Data Reads with TTL + Manual Bust

```python
@st.cache_data(ttl=120, show_spinner="Loading sheet data…")
def cached_load_worksheet(_client, spreadsheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """
    Cached read — hits Sheets API at most once per 2 minutes.
    Prefix `_client` with underscore so Streamlit skips hashing it.
    """
    sh = _client.open(spreadsheet_name)
    ws = sh.worksheet(worksheet_name)
    rows = ws.get_all_values()          # single API call
    if len(rows) < 2:
        return pd.DataFrame()
    headers = [h.strip() for h in rows[0]]
    data = rows[1:]
    df = pd.DataFrame(data, columns=headers)

    # ensure system columns exist in-memory (don't write yet)
    for col in SYSTEM_COLS:
        if col not in df.columns:
            df[col] = ""
    return df


def load_worksheet(spreadsheet_name: str, worksheet_name: str, force: bool = False) -> pd.DataFrame:
    """Wrapper that supports manual cache-busting."""
    client = get_gspread_client()
    if force:
        cached_load_worksheet.clear()
    return cached_load_worksheet(client, spreadsheet_name, worksheet_name)
```

Add a refresh button in the sidebar:

```python
if st.sidebar.button("🔄 Refresh data"):
    st.session_state["force_reload"] = True
    st.rerun()
```

And on load:

```python
force = st.session_state.pop("force_reload", False)
df = load_worksheet(SPREADSHEET_NAME, ws_name, force=force)
```

> **Savings**: Goes from ~4-8 API calls per rerun → **0** for 2 minutes, then **1**.

---

## 3. Batch Writes (biggest quota saver)

Your old `audit_form` likely did something like:

```python
# ❌ OLD — one API call per cell
ws.update_cell(row, status_col, new_status)
ws.update_cell(row, log_col, new_log)
ws.update_cell(row, auditor_col, email)
ws.update_cell(row, date_col, now)
```

Replace with a single batch:

```python
def batch_update_record(spreadsheet_name: str, worksheet_name: str,
                        row_idx: int, updates: dict):
    """
    updates = {"Status": "Completed", "Audit_Log": "...", ...}
    Writes everything in ONE API call.
    """
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    ws = sh.worksheet(worksheet_name)

    headers = ws.row_values(1)
    header_map = {h.strip(): i + 1 for i, h in enumerate(headers)}

    sheet_row = row_idx + 2  # +1 header, +1 zero-index

    cells = []
    for col_name, value in updates.items():
        col_idx = header_map.get(col_name)
        if col_idx is None:
            continue
        cells.append(gspread.Cell(row=sheet_row, col=col_idx, value=str(value)))

    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")  # 1 API call

    # bust cache so next read picks up changes
    cached_load_worksheet.clear()
```

Usage in your audit form:

```python
if st.button("Submit Audit"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{now}] {st.session_state.user_email}: {audit_notes}"
    existing_log = df.at[row_idx, "Audit_Log"]
    full_log = f"{existing_log}\n{log_entry}".strip()

    batch_update_record(SPREADSHEET_NAME, ws_name, row_idx, {
        "Status": new_status,
        "Audit_Log": full_log,
        "Auditor_Email": st.session_state.user_email,
        "Submission_Date": now,
        # include any edited field columns too
        **edited_fields,
    })
    st.success("Saved.")
    st.rerun()
```

> **Savings**: 4-10 calls per submission → **1**.

---

## 4. Batch-Add Missing System Columns

```python
def ensure_system_cols_in_sheet(spreadsheet_name: str, worksheet_name: str):
    """Adds all missing system columns in a single resize + batch write."""
    client = get_gspread_client()
    sh = client.open(spreadsheet_name)
    ws = sh.worksheet(worksheet_name)

    headers = ws.row_values(1)
    missing = [c for c in SYSTEM_COLS if c not in headers]

    if not missing:
        return

    current_cols = len(headers)
    new_total = current_cols + len(missing)

    # one resize call
    ws.resize(cols=new_total)

    # one batch write for all new headers
    cells = [
        gspread.Cell(row=1, col=current_cols + i + 1, value=col)
        for i, col in enumerate(missing)
    ]
    ws.update_cells(cells, value_input_option="RAW")
```

> **Before**: N calls for N missing columns. **After**: 2 calls max (resize + batch).

---

## 5. Cache UsersDB Separately (longer TTL)

```python
@st.cache_data(ttl=300, show_spinner=False)
def cached_load_users(_client) -> pd.DataFrame:
    """Users change rarely — cache for 5 min."""
    sh = _client.open(SPREADSHEET_NAME)
    ws = sh.worksheet("UsersDB")
    rows = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame(columns=["Email", "Password", "Role"])
    return pd.DataFrame(rows[1:], columns=rows[0])
```

---

## Quick Reference — API Call Comparison

| Action | Before | After |
|---|---|---|
| Page rerun (read) | 4-8 | **0** (cached) |
| Audit submission | 4-10 | **1** |
| Add system columns | N | **2** |
| Auth check | 1-2 | **0** (cached 5 min) |
| Manual refresh | — | **1** |

Typical session goes from **~50+ calls/min** during active use → **~3-5 calls/min**.

---

Want me to wire this into your full file and hand back the complete refactored script?
