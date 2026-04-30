import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import io
import os
import calendar
import certifi
from urllib.parse import quote_plus, urlparse, urlunparse
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import warnings
warnings.filterwarnings("ignore")

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Staffing Dashboard",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── MONGODB CONNECTION ──────────────────────────────────────────────────────
def parse_and_escape_uri(uri: str) -> str:
    if not uri or not uri.startswith("mongodb"):
        return uri
    try:
        parsed = urlparse(uri)
        if parsed.username and parsed.password:
            user = quote_plus(parsed.username)
            pwd = quote_plus(parsed.password)
            netloc = f"{user}:{pwd}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return urlunparse((
                parsed.scheme, netloc, parsed.path,
                parsed.params, parsed.query, parsed.fragment
            ))
    except Exception:
        pass
    return uri

@st.cache_resource(show_spinner=False)
def get_db():
    uri = ""
    try:
        uri = st.secrets.get("MONGO_URI", "")
    except Exception:
        pass
    if not uri:
        uri = os.environ.get("MONGO_URI", "")
    if not uri:
        return None, None

    uri = parse_and_escape_uri(uri)

    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,
            tls=True,
            tlsCAFile=certifi.where(),
            retryWrites=True,
            w="majority",
        )
        client.admin.command("ping")
        db = client["staffing_db"]

        db["employees"].create_index("ECN", unique=True)
        db["history"].create_index([("ECN", 1), ("field", 1), ("start_date", 1)])
        db["history"].create_index([("ECN", 1), ("field", 1), ("end_date", 1)])
        db["upload_log"].create_index("upload_date")
        return client, db
    except Exception as e:
        st.session_state["_mongo_err"] = str(e)
        return None, None

def get_employees_col():
    _, db = get_db()
    return db["employees"] if db is not None else None

def get_history_col():
    _, db = get_db()
    return db["history"] if db is not None else None

def get_upload_log_col():
    _, db = get_db()
    return db["upload_log"] if db is not None else None

def db_connected():
    _, db = get_db()
    return db is not None

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()

def row_to_doc(row: dict) -> dict:
    return {k.replace(".", "_"): safe_str(v) for k, v in row.items()}

def load_excel(uploaded_file) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(uploaded_file)
        sheet = "Consolidated Staffing" if "Consolidated Staffing" in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(uploaded_file, sheet_name=sheet, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.fillna("")
        return df
    except Exception as e:
        st.error(f"Error reading Excel: {e}")
        return pd.DataFrame()

def df_to_excel_bytes(df: pd.DataFrame, sheet_name="Staffing") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        wb = writer.book
        ws = writer.sheets[sheet_name]
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1f4e79",
            "font_color": "white", "border": 1
        })
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, header_fmt)
            ws.set_column(col_num, col_num, max(15, len(str(col_name)) + 2))
    return buf.getvalue()

# ─── CORE LOGIC ──────────────────────────────────────────────────────────────
BATCH_SIZE = 5000

def flush_batches(col, hist, emp_ops, hist_ops):
    if emp_ops:
        try:
            col.bulk_write(emp_ops, ordered=False)
        except BulkWriteError:
            pass
    if hist_ops:
        try:
            hist.bulk_write(hist_ops, ordered=False)
        except BulkWriteError:
            pass

def upsert_employees(df: pd.DataFrame, upload_date: str, progress_bar=None):
    col = get_employees_col()
    hist = get_history_col()
    log = get_upload_log_col()
    if col is None or hist is None:
        return 0, 0, "Database not connected"

    total_rows = len(df)

    # ── PRELOAD: all existing employees into dict (ECN -> doc) ──
    existing_docs = {}
    for doc in col.find({}, {"_id": 0}):
        ecn = doc.get("ECN")
        if ecn:
            existing_docs[ecn] = doc

    # ── PRELOAD: latest manual edit date per (ECN, field) ──
    manual_edits = {}
    for h in hist.find({"source": "manual_edit"}, {"_id": 0, "ECN": 1, "field": 1, "start_date": 1}):
        key = (h["ECN"], h["field"])
        # keep the most recent manual edit date
        if key not in manual_edits or h["start_date"] > manual_edits[key]:
            manual_edits[key] = h["start_date"]

    emp_ops = []
    hist_ops = []
    inserted = 0
    updated = 0
    skipped_manual = 0

    for idx, (_, row) in enumerate(df.iterrows()):
        ecn = str(row.get("ECN", "")).strip()
        if not ecn or ecn.lower() == "nan":
            continue

        doc = row_to_doc(row.to_dict())
        doc["ECN"] = ecn
        existing = existing_docs.get(ecn)

        if existing is None:
            # ── NEW EMPLOYEE ──
            doc["_created_at"] = upload_date
            doc["_updated_at"] = upload_date
            doc["_last_upload"] = upload_date
            emp_ops.append(UpdateOne({"ECN": ecn}, {"$set": doc}, upsert=True))
            inserted += 1

            for field, val in doc.items():
                if field.startswith("_") or val == "":
                    continue
                hist_ops.append(UpdateOne(
                    {"ECN": ecn, "field": field, "start_date": "2000-01-01"},
                    {"$set": {
                        "ECN": ecn,
                        "Employee": doc.get("Employee", ""),
                        "field": field,
                        "value": val,
                        "prev_value": "",
                        "start_date": "2000-01-01",
                        "end_date": "9999-12-31",
                        "source": "excel_upload",
                    }},
                    upsert=True
                ))
        else:
            # ── EXISTING EMPLOYEE ──
            changed = False
            last_upload = existing.get("_last_upload", "2000-01-01")

            for field, new_val in doc.items():
                if field.startswith("_"):
                    continue
                old_val = existing.get(field, "")

                if new_val != old_val:
                    # Check against preloaded manual edits
                    manual_date = manual_edits.get((ecn, field))
                    if manual_date and manual_date > last_upload:
                        skipped_manual += 1
                        continue

                    hist_ops.append(UpdateOne(
                        {"ECN": ecn, "field": field, "end_date": "9999-12-31"},
                        {"$set": {"end_date": upload_date}}
                    ))

                    hist_ops.append(UpdateOne(
                        {"ECN": ecn, "field": field, "start_date": upload_date},
                        {"$set": {
                            "ECN": ecn,
                            "Employee": doc.get("Employee", existing.get("Employee", "")),
                            "field": field,
                            "value": new_val,
                            "prev_value": old_val,
                            "start_date": upload_date,
                            "end_date": "9999-12-31",
                            "source": "excel_upload",
                        }},
                        upsert=True
                    ))
                    changed = True

            if changed:
                update_doc = {k: v for k, v in doc.items() if not k.startswith("_")}
                update_doc["_updated_at"] = upload_date
                update_doc["_last_upload"] = upload_date
                emp_ops.append(UpdateOne({"ECN": ecn}, {"$set": update_doc}))
                updated += 1
            else:
                emp_ops.append(UpdateOne({"ECN": ecn}, {"$set": {"_last_upload": upload_date}}))

        # Flush every BATCH_SIZE
        if len(emp_ops) >= BATCH_SIZE or len(hist_ops) >= BATCH_SIZE:
            flush_batches(col, hist, emp_ops, hist_ops)
            emp_ops = []
            hist_ops = []
            if progress_bar is not None:
                progress_bar.progress(min(0.99, (idx + 1) / total_rows),
                                      text=f"Processed {idx + 1:,}/{total_rows:,}...")

    # Final flush
    flush_batches(col, hist, emp_ops, hist_ops)
    if progress_bar is not None:
        progress_bar.progress(1.0, text="Done!")

    if log is not None:
        log.insert_one({
            "upload_date": upload_date,
            "rows_processed": total_rows,
            "inserted": inserted,
            "updated": updated,
            "skipped_manual": skipped_manual,
        })

    return inserted, updated, None


def get_employees_at_date(query_date: str) -> pd.DataFrame:
    col = get_employees_col()
    hist = get_history_col()
    if col is None:
        return pd.DataFrame()

    docs = list(col.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs)

    hist_docs = list(hist.find(
        {"start_date": {"$lte": query_date}, "end_date": {"$gt": query_date}},
        {"_id": 0, "ECN": 1, "field": 1, "value": 1}
    ))

    overrides = {}
    for h in hist_docs:
        key = (h["ECN"], h["field"])
        if key not in overrides:
            overrides[key] = h["value"]

    for (ecn, field), val in overrides.items():
        if field in df.columns:
            mask = df["ECN"] == ecn
            df.loc[mask, field] = val

    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal, errors="ignore")
    return df


def record_manual_edit(ecn: str, field: str, new_value: str, start_date: str):
    col = get_employees_col()
    hist = get_history_col()
    if col is None or hist is None:
        return False

    existing = col.find_one({"ECN": ecn})
    if not existing:
        return False

    old_value = existing.get(field, "")

    hist.update_many(
        {"ECN": ecn, "field": field, "end_date": "9999-12-31", "start_date": {"$lte": start_date}},
        {"$set": {"end_date": start_date}}
    )

    hist.update_one(
        {"ECN": ecn, "field": field, "start_date": start_date},
        {"$set": {
            "ECN": ecn,
            "Employee": existing.get("Employee", ""),
            "field": field,
            "value": new_value,
            "prev_value": old_value,
            "start_date": start_date,
            "end_date": "9999-12-31",
            "source": "manual_edit",
        }},
        upsert=True
    )

    col.update_one(
        {"ECN": ecn},
        {"$set": {field: new_value, "_updated_at": start_date}}
    )
    return True


def get_employee_history(ecn: str) -> pd.DataFrame:
    hist = get_history_col()
    if hist is None:
        return pd.DataFrame()
    docs = list(hist.find({"ECN": ecn}, {"_id": 0}).sort([("field", 1), ("start_date", -1)]))
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


def compact_history():
    hist = get_history_col()
    if hist is None:
        return 0
    result = hist.delete_many({"$expr": {"$eq": ["$value", "$prev_value"]}})
    return result.deleted_count


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/group.png", width=60)
    st.title("Staffing App")
    st.divider()

    if db_connected():
        st.success("✅ MongoDB Connected")
    else:
        err = st.session_state.get("_mongo_err", "")
        if err:
            st.error(f"❌ {err[:120]}")
        else:
            st.warning("⚠️ No MongoDB URI configured")

    st.divider()
    page = st.radio("Navigation", [
        "📤 Upload / Sync",
        "👤 Employee Editor",
        "📅 Date Snapshot",
        "📊 Export Data",
        "🛠️ DB Tools",
    ])

# ─── PAGE: UPLOAD ─────────────────────────────────────────────────────────────
if page == "📤 Upload / Sync":
    st.title("📤 Upload & Sync Staffing Data")
    st.markdown("Upload the **Consolidated Staffing** Excel file to populate or update the database.")

    if not db_connected():
        st.error("Please configure a valid MongoDB URI in Streamlit Secrets first.")
        st.stop()

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Choose Excel file (.xlsx)", type=["xlsx"])
    with col2:
        st.markdown("**Instructions**")
        st.markdown("""
        - **First upload**: populates the DB (baseline history = 2000-01-01)
        - **Subsequent uploads**: updates only changed records
        - **In-app edits are protected**: uploads will NOT overwrite fields manually edited after the last upload
        - **New columns** are added automatically; missing columns are left unchanged
        """)

    if uploaded:
        with st.spinner("Reading file..."):
            df = load_excel(uploaded)

        if df.empty:
            st.error("Could not read the Excel file.")
            st.stop()

        st.success(f"✅ File loaded — **{len(df):,} rows**, **{len(df.columns)} columns**")

        with st.expander("Preview (first 10 rows)"):
            st.dataframe(df.head(10), use_container_width=True)

        if st.button("🚀 Sync to Database", type="primary"):
            today_str = date.today().isoformat()
            progress = st.progress(0, text="Preparing...")

            with st.spinner("Writing to MongoDB..."):
                inserted, updated, err = upsert_employees(df, today_str, progress_bar=progress)

            if err:
                st.error(err)
            else:
                total = get_employees_col().count_documents({})
                st.success(f"""
                ✅ Sync complete!
                - **{inserted}** new employees added
                - **{updated}** existing employees updated
                - **{total:,}** total employees in DB
                """)

    st.divider()
    st.subheader("📊 Database Stats")
    col = get_employees_col()
    hist = get_history_col()
    if col is not None:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Employees", f"{col.count_documents({}):,}")
        c2.metric("Active", f"{col.count_documents({'Active/Inactive': 'Active'}):,}")
        c3.metric("Inactive", f"{col.count_documents({'Active/Inactive': 'Inactive'}):,}")
        c4.metric("History Records", f"{hist.count_documents({}):,}" if hist is not None else "—")

        try:
            db_stats = col.database.command("dbStats")
            used_mb = db_stats.get("dataSize", 0) / (1024 * 1024)
            st.caption(f"Approx. DB size: **{used_mb:.1f} MB** / 512 MB")
            st.progress(min(1.0, used_mb / 512))
        except Exception:
            pass
    else:
        st.info("Connect to MongoDB to see stats.")


# ─── PAGE: EMPLOYEE EDITOR ────────────────────────────────────────────────────
elif page == "👤 Employee Editor":
    st.title("👤 Employee Editor")
    col = get_employees_col()
    if col is None:
        st.error("Connect MongoDB first.")
        st.stop()

    search = st.text_input("🔍 Search by name, ECN, or email", placeholder="e.g. Santos, 12345")
    filter_status = st.selectbox("Filter by Status", ["All", "Active", "Inactive", "LOA", "Maternity", "Suspended"])

    query = {}
    if filter_status != "All":
        query["Active/Inactive"] = filter_status
    if search:
        query["$or"] = [
            {"Employee": {"$regex": search, "$options": "i"}},
            {"ECN": {"$regex": search, "$options": "i"}},
            {"Email": {"$regex": search, "$options": "i"}},
        ]

    docs = list(col.find(query, {"_id": 0}).limit(200))
    if not docs:
        st.warning("No employees found.")
        st.stop()

    df_list = pd.DataFrame(docs)
    display_cols = ["ECN", "Employee", "Client", "Sub-Process", "Role", "Billable/Buffer",
                    "Active/Inactive", "Location", "Overall Location"]
    display_cols = [c for c in display_cols if c in df_list.columns]

    st.markdown(f"**{len(docs)} employees found** (max 200 shown)")
    selected_rows = st.dataframe(
        df_list[display_cols],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    if selected_rows and selected_rows.selection.rows:
        idx = selected_rows.selection.rows[0]
        emp = docs[idx]
        ecn = emp["ECN"]

        st.divider()
        st.subheader(f"✏️ Edit: {emp.get('Employee', ecn)} (ECN: {ecn})")

        tab1, tab2 = st.tabs(["Edit Fields", "Field History"])

        with tab1:
            st.markdown("**Effective Date** — the date from which this change applies:")
            eff_date = st.date_input("Effective Start Date", value=date.today())
            eff_date_str = eff_date.isoformat()

            all_fields = [k for k in emp.keys() if not k.startswith("_")]
            preferred_first = ["Billable/Buffer", "Active/Inactive", "Client", "Sub-Process",
                               "Supervisor", "Role", "Manager", "Location", "Overall Location",
                               "Shift Timing", "Structure", "Tagging", "Role Tagging",
                               "Client Approved Billable"]
            ordered_fields = [f for f in preferred_first if f in all_fields]
            ordered_fields += [f for f in all_fields if f not in ordered_fields]

            st.markdown("**Fields** (all changes are tracked with effective dates)")
            edit_vals = {}
            cols = st.columns(3)
            for i, field in enumerate(ordered_fields):
                with cols[i % 3]:
                    edit_vals[field] = st.text_input(
                        field, value=emp.get(field, ""), key=f"edit_{field}"
                    )

            if st.button("💾 Save Changes", type="primary"):
                saved = 0
                for field, new_val in edit_vals.items():
                    if new_val != emp.get(field, ""):
                        ok = record_manual_edit(ecn, field, new_val, eff_date_str)
                        if ok:
                            saved += 1

                if saved:
                    st.success(f"✅ {saved} field(s) updated, effective {eff_date_str}")
                    st.rerun()
                else:
                    st.info("No changes detected.")

        with tab2:
            st.markdown(f"**Change history for ECN {ecn}**")
            hist_df = get_employee_history(ecn)
            if not hist_df.empty:
                display = hist_df[["field", "value", "prev_value", "start_date", "end_date", "source"]]
                display.columns = ["Field", "Value", "Previous", "Start", "End", "Source"]
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.info("No history found for this employee.")


# ─── PAGE: DATE SNAPSHOT ──────────────────────────────────────────────────────
elif page == "📅 Date Snapshot":
    st.title("📅 Date Snapshot")
    st.markdown("View staffing data **as it was on any specific date**, applying all historical changes.")

    if not db_connected():
        st.error("Connect MongoDB first.")
        st.stop()

    snap_date = st.date_input("Select snapshot date", value=date.today())
    snap_str = snap_date.isoformat()

    filter_client = st.text_input("Filter by Client (optional)")
    filter_bb = st.selectbox("Filter Billable/Buffer", ["All", "Billable", "Buffer", "Support", "Training", "Excluded"])
    filter_status = st.selectbox("Filter Status", ["All", "Active", "Inactive", "LOA", "Maternity", "Suspended"])

    if st.button("📸 Load Snapshot", type="primary"):
        with st.spinner(f"Building snapshot for {snap_str}..."):
            df = get_employees_at_date(snap_str)

        if df.empty:
            st.warning("No data found.")
        else:
            if filter_client and "Client" in df.columns:
                df = df[df["Client"].str.contains(filter_client, case=False, na=False)]
            if filter_bb != "All" and "Billable/Buffer" in df.columns:
                df = df[df["Billable/Buffer"] == filter_bb]
            if filter_status != "All" and "Active/Inactive" in df.columns:
                df = df[df["Active/Inactive"] == filter_status]

            st.success(f"✅ Snapshot for **{snap_str}** — **{len(df):,} employees**")

            m1, m2, m3, m4 = st.columns(4)
            if "Billable/Buffer" in df.columns:
                m1.metric("Billable", len(df[df["Billable/Buffer"] == "Billable"]))
                m2.metric("Buffer", len(df[df["Billable/Buffer"] == "Buffer"]))
                m3.metric("Support", len(df[df["Billable/Buffer"] == "Support"]))
            if "Active/Inactive" in df.columns:
                m4.metric("Active", len(df[df["Active/Inactive"] == "Active"]))

            display_cols = [c for c in [
                "ECN", "Employee", "Client", "Sub-Process", "Role",
                "Billable/Buffer", "Active/Inactive", "Location",
                "Overall Location", "Supervisor", "Manager"
            ] if c in df.columns]

            st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

            excel_bytes = df_to_excel_bytes(df, sheet_name=f"Snapshot_{snap_str}")
            st.download_button(
                label=f"⬇️ Download Snapshot ({snap_str})",
                data=excel_bytes,
                file_name=f"staffing_snapshot_{snap_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# ─── PAGE: EXPORT ─────────────────────────────────────────────────────────────
elif page == "📊 Export Data":
    st.title("📊 Export Data")
    st.markdown("Export staffing data for any time range.")

    if not db_connected():
        st.error("Connect MongoDB first.")
        st.stop()

    exp_type = st.radio("Export Type", ["Daily", "Weekly", "Monthly", "Yearly", "Custom Range"], horizontal=True)

    today = date.today()

    if exp_type == "Daily":
        exp_date = st.date_input("Select date", value=today)
        start_date = exp_date.isoformat()
        end_date = exp_date.isoformat()
        label = f"daily_{exp_date}"

    elif exp_type == "Weekly":
        week_start = today - timedelta(days=today.weekday())
        exp_week = st.date_input("Week starting (Monday)", value=week_start)
        start_date = exp_week.isoformat()
        end_date = (exp_week + timedelta(days=6)).isoformat()
        label = f"weekly_{start_date}_to_{end_date}"

    elif exp_type == "Monthly":
        col1, col2 = st.columns(2)
        with col1:
            month = st.selectbox("Month", list(range(1, 13)), index=today.month - 1,
                                  format_func=lambda m: datetime(2000, m, 1).strftime("%B"))
        with col2:
            year = st.number_input("Year", min_value=2020, max_value=2035, value=today.year)
        last_day = calendar.monthrange(year, month)[1]
        start_date = date(year, month, 1).isoformat()
        end_date = date(year, month, last_day).isoformat()
        label = f"monthly_{year}_{month:02d}"

    elif exp_type == "Yearly":
        year = st.number_input("Year", min_value=2020, max_value=2035, value=today.year)
        start_date = date(year, 1, 1).isoformat()
        end_date = date(year, 12, 31).isoformat()
        label = f"yearly_{year}"

    else:
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("Start date", value=today - timedelta(days=7))
        with col2:
            end = st.date_input("End date", value=today)
        start_date = start.isoformat()
        end_date = end.isoformat()
        label = f"custom_{start_date}_to_{end_date}"

    st.markdown(f"**Range:** `{start_date}` → `{end_date}`")

    st.divider()
    export_mode = st.radio("Export Mode", [
        "End-of-period snapshot",
        "Daily snapshots (one sheet per day)",
    ], help="End-of-period: single snapshot at the last day. Daily: one tab per day in Excel.")

    filter_active = st.checkbox("Active employees only", value=False)
    filter_client2 = st.text_input("Filter by Client (optional)", key="exp_client")

    if st.button("📥 Generate Export", type="primary"):
        if export_mode == "End-of-period snapshot":
            with st.spinner(f"Generating snapshot at {end_date}..."):
                df = get_employees_at_date(end_date)
                if filter_active and "Active/Inactive" in df.columns:
                    df = df[df["Active/Inactive"] == "Active"]
                if filter_client2 and "Client" in df.columns:
                    df = df[df["Client"].str.contains(filter_client2, case=False, na=False)]

            st.success(f"✅ {len(df):,} employees as of {end_date}")
            excel_bytes = df_to_excel_bytes(df, sheet_name="Export")
            st.download_button(
                f"⬇️ Download {exp_type} Export",
                data=excel_bytes,
                file_name=f"staffing_{label}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        else:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days = (end_dt - start_dt).days + 1

            if days > 31:
                st.warning("Daily snapshot mode is limited to 31 days. Please narrow your range.")
            else:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                    wb = writer.book
                    header_fmt = wb.add_format({"bold": True, "bg_color": "#1f4e79", "font_color": "white"})

                    progress_bar = st.progress(0, text="Building sheets...")
                    for i in range(days):
                        d = (start_dt + timedelta(days=i)).date().isoformat()
                        df_day = get_employees_at_date(d)
                        if filter_active and "Active/Inactive" in df_day.columns:
                            df_day = df_day[df_day["Active/Inactive"] == "Active"]
                        if filter_client2 and "Client" in df_day.columns:
                            df_day = df_day[df_day["Client"].str.contains(filter_client2, case=False, na=False)]

                        sheet_name = d.replace("-", "")[-6:]
                        df_day.to_excel(writer, index=False, sheet_name=sheet_name)
                        ws = writer.sheets[sheet_name]
                        for col_num, col_name in enumerate(df_day.columns):
                            ws.write(0, col_num, col_name, header_fmt)
                        progress_bar.progress((i + 1) / days, text=f"Building {d}...")

                st.success(f"✅ {days} daily sheets generated!")
                st.download_button(
                    "⬇️ Download Daily Export",
                    data=buf.getvalue(),
                    file_name=f"staffing_{label}_daily.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )


# ─── PAGE: DB TOOLS ───────────────────────────────────────────────────────────
elif page == "🛠️ DB Tools":
    st.title("🛠️ Database Tools")
    st.markdown("Maintenance utilities for your 512 MB free-tier MongoDB.")

    if not db_connected():
        st.error("Connect MongoDB first.")
        st.stop()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Storage")
        try:
            db_stats = get_employees_col().database.command("dbStats")
            used_mb = db_stats.get("dataSize", 0) / (1024 * 1024)
            st.metric("Used Storage", f"{used_mb:.1f} MB")
            st.progress(min(1.0, used_mb / 512))
            st.caption("Free tier limit: 512 MB")
        except Exception as e:
            st.error(f"Could not fetch stats: {e}")

    with col2:
        st.subheader("Cleanup")
        st.markdown("""
        Remove redundant history entries where `value == prev_value` 
        (no actual change) to save space.
        """)
        if st.button("🧹 Compact History", type="primary"):
            with st.spinner("Compacting..."):
                deleted = compact_history()
            st.success(f"Removed **{deleted}** redundant history entries.")

    st.divider()
    st.subheader("Recent Uploads")
    log = get_upload_log_col()
    if log is not None:
        logs = list(log.find({}, {"_id": 0}).sort("upload_date", -1).limit(10))
        if logs:
            st.dataframe(pd.DataFrame(logs), use_container_width=True, hide_index=True)
        else:
            st.info("No uploads logged yet.")
