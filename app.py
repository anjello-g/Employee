import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import io
import os
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
MONGO_URI = os.environ.get("MONGO_URI", "")

@st.cache_resource
def get_db():
    if not MONGO_URI:
        return None, None
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["staffing_db"]
    return client, db

def get_employees_col():
    _, db = get_db()
    if db is None:
        return None
    return db["employees"]

def get_history_col():
    _, db = get_db()
    if db is None:
        return None
    return db["field_history"]

# ─── HELPERS ─────────────────────────────────────────────────────────────────
CORE_COLS = [
    "ECN", "Employee", "Client", "Sub-Process", "Supervisor", "Role",
    "Manager", "DOJ Knack", "DOJ Project", "Date of Separation",
    "Shift Timing", "Email", "NT Login", "Structure", "Billable/Buffer",
    "Process Owner", "Department", "Location", "Allocated Seats", "Gender",
    "Seat Number", "Global ID (GPP)", "Active/Inactive", "CDP Email",
    "BufferAgent", "EWS Type", "Driver", "Expected Move Date",
    "Overall Location", "Client Approved Billable", "Tagging",
    "Role Tagging", "Specialty",
]

TRACKED_FIELDS = [
    "Billable/Buffer", "Active/Inactive", "Client", "Sub-Process",
    "Supervisor", "Role", "Manager", "Location", "Overall Location",
    "Shift Timing", "Structure", "Tagging", "Role Tagging",
    "Client Approved Billable",
]


def safe_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()


def row_to_doc(row: dict) -> dict:
    doc = {}
    for k, v in row.items():
        doc[k.replace(".", "_")] = safe_str(v)
    return doc


def load_excel(uploaded_file) -> pd.DataFrame:
    xl = pd.ExcelFile(uploaded_file)
    if "Consolidated Staffing" in xl.sheet_names:
        df = pd.read_excel(uploaded_file, sheet_name="Consolidated Staffing", dtype=str)
    else:
        df = pd.read_excel(uploaded_file, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna("")
    return df


def upsert_employees(df: pd.DataFrame, today_str: str):
    col = get_employees_col()
    hist = get_history_col()
    if col is None:
        return 0, 0

    ops = []
    hist_ops = []
    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        ecn = str(row.get("ECN", "")).strip()
        if not ecn or ecn == "nan":
            continue

        doc = row_to_doc(row.to_dict())
        doc["ECN"] = ecn

        existing = col.find_one({"ECN": ecn}, {"_id": 0})

        if existing is None:
            # New employee
            doc["_created_at"] = today_str
            doc["_updated_at"] = today_str
            ops.append(UpdateOne({"ECN": ecn}, {"$set": doc}, upsert=True))
            inserted += 1
        else:
            # Track field changes
            changed = {}
            for field in TRACKED_FIELDS:
                old_val = existing.get(field, "")
                new_val = doc.get(field, "")
                if old_val != new_val and new_val != "":
                    changed[field] = {"from": old_val, "to": new_val}

            if changed:
                for field, chg in changed.items():
                    # Close previous history entry
                    hist.update_many(
                        {"ECN": ecn, "field": field, "end_date": "9999-12-31"},
                        {"$set": {"end_date": today_str}}
                    )
                    # Open new history entry
                    hist_ops.append(UpdateOne(
                        {"ECN": ecn, "field": field, "start_date": today_str, "end_date": "9999-12-31"},
                        {"$set": {
                            "ECN": ecn,
                            "Employee": doc.get("Employee", existing.get("Employee", "")),
                            "field": field,
                            "value": chg["to"],
                            "prev_value": chg["from"],
                            "start_date": today_str,
                            "end_date": "9999-12-31",
                        }},
                        upsert=True
                    ))

                doc["_updated_at"] = today_str
                ops.append(UpdateOne({"ECN": ecn}, {"$set": doc}))
                updated += 1

    # Seed initial history if empty
    emp_count = col.count_documents({})
    if emp_count == 0 or hist.count_documents({}) == 0:
        # Seed history for all current employees
        for _, row in df.iterrows():
            ecn = str(row.get("ECN", "")).strip()
            if not ecn or ecn == "nan":
                continue
            for field in TRACKED_FIELDS:
                val = safe_str(row.get(field, ""))
                if val:
                    hist_ops.append(UpdateOne(
                        {"ECN": ecn, "field": field, "start_date": "2000-01-01", "end_date": "9999-12-31"},
                        {"$set": {
                            "ECN": ecn,
                            "Employee": safe_str(row.get("Employee", "")),
                            "field": field,
                            "value": val,
                            "prev_value": "",
                            "start_date": "2000-01-01",
                            "end_date": "9999-12-31",
                        }},
                        upsert=True
                    ))

    if ops:
        try:
            col.bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass
    if hist_ops:
        try:
            hist.bulk_write(hist_ops, ordered=False)
        except BulkWriteError:
            pass

    return inserted, updated


def get_employees_at_date(query_date: str) -> pd.DataFrame:
    """Return snapshot of all employees at a specific date, applying history overrides."""
    col = get_employees_col()
    hist = get_history_col()
    if col is None:
        return pd.DataFrame()

    docs = list(col.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs)

    # Apply field history: for each tracked field, find the applicable value at query_date
    hist_docs = list(hist.find(
        {"start_date": {"$lte": query_date}, "end_date": {"$gt": query_date}},
        {"_id": 0, "ECN": 1, "field": 1, "value": 1}
    ))

    for h in hist_docs:
        ecn = h["ECN"]
        field = h["field"]
        val = h["value"]
        mask = df["ECN"] == ecn
        if mask.any() and field in df.columns:
            df.loc[mask, field] = val

    # Clean up internal fields
    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal, errors="ignore")
    return df


def record_manual_edit(ecn: str, field: str, new_value: str, start_date: str):
    """Record a manual in-app edit with a specific effective start date."""
    col = get_employees_col()
    hist = get_history_col()
    if col is None:
        return False

    existing = col.find_one({"ECN": ecn})
    if not existing:
        return False

    old_value = existing.get(field, "")

    # Close any open history entry for this field
    hist.update_many(
        {"ECN": ecn, "field": field, "end_date": "9999-12-31", "start_date": {"$lte": start_date}},
        {"$set": {"end_date": start_date}}
    )

    # Insert new history record
    hist.update_one(
        {"ECN": ecn, "field": field, "start_date": start_date, "end_date": "9999-12-31"},
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

    # Update current record
    col.update_one({"ECN": ecn}, {"$set": {field: new_value, "_updated_at": start_date}})
    return True


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Staffing")
        wb = writer.book
        ws = writer.sheets["Staffing"]
        header_fmt = wb.add_format({"bold": True, "bg_color": "#1f4e79", "font_color": "white", "border": 1})
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, header_fmt)
            ws.set_column(col_num, col_num, max(15, len(col_name) + 2))
    return buf.getvalue()


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/group.png", width=60)
    st.title("Staffing App")
    st.divider()

    mongo_uri_input = st.text_input(
        "MongoDB URI",
        value=MONGO_URI,
        type="password",
        placeholder="mongodb+srv://user:pass@cluster.mongodb.net/",
        help="Paste your MongoDB Atlas connection string"
    )
    if mongo_uri_input and mongo_uri_input != MONGO_URI:
        os.environ["MONGO_URI"] = mongo_uri_input
        st.cache_resource.clear()
        st.rerun()

    # Test connection
    try:
        _, db = get_db()
        if db is not None:
            db.command("ping")
            st.success("✅ MongoDB Connected")
        else:
            st.warning("⚠️ No MongoDB URI")
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)[:60]}")

    st.divider()
    page = st.radio("Navigation", [
        "📤 Upload / Sync",
        "👤 Employee Editor",
        "📅 Date Snapshot",
        "📊 Export Data",
    ])

# ─── PAGE: UPLOAD ─────────────────────────────────────────────────────────────
if page == "📤 Upload / Sync":
    st.title("📤 Upload & Sync Staffing Data")
    st.markdown("Upload the **Consolidated Staffing** Excel file to populate or update the database.")

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Choose Excel file (.xlsx)", type=["xlsx"])
    with col2:
        st.markdown("**Instructions**")
        st.markdown("""
        - First upload: populates the DB
        - Subsequent uploads: updates changed records
        - In-app edits are **never overwritten** by uploads
        - New/removed columns are handled automatically
        """)

    if uploaded:
        with st.spinner("Reading file..."):
            df = load_excel(uploaded)

        st.success(f"✅ File loaded — **{len(df):,} rows**, **{len(df.columns)} columns**")

        with st.expander("Preview (first 10 rows)"):
            st.dataframe(df.head(10), use_container_width=True)

        if st.button("🚀 Sync to Database", type="primary"):
            if get_employees_col() is None:
                st.error("Please configure MongoDB URI in the sidebar first.")
            else:
                today_str = date.today().isoformat()
                progress = st.progress(0, text="Syncing...")

                with st.spinner("Writing to MongoDB..."):
                    inserted, updated = upsert_employees(df, today_str)
                    progress.progress(100, text="Done!")

                total = get_employees_col().count_documents({})
                st.success(f"""
                ✅ Sync complete!
                - **{inserted}** new employees added
                - **{updated}** existing employees updated
                - **{total:,}** total unique employees in DB
                """)

    # DB Stats
    st.divider()
    st.subheader("📊 Database Stats")
    col = get_employees_col()
    hist = get_history_col()
    if col is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Employees", f"{col.count_documents({}):,}")
        c2.metric("Active Employees", f"{col.count_documents({'Active/Inactive': 'Active'}):,}")
        c3.metric("History Records", f"{hist.count_documents({}):,}" if hist else "—")
    else:
        st.info("Connect to MongoDB to see stats.")


# ─── PAGE: EMPLOYEE EDITOR ────────────────────────────────────────────────────
elif page == "👤 Employee Editor":
    st.title("👤 Employee Editor")
    col = get_employees_col()
    if col is None:
        st.error("Connect MongoDB first.")
        st.stop()

    # Search
    search = st.text_input("🔍 Search by name, ECN, or email", placeholder="e.g. Santos, 12345")
    filter_status = st.selectbox("Filter by Status", ["All", "Active", "Inactive", "LOA", "Maternity"])

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

            editable = [f for f in TRACKED_FIELDS if f in emp]
            non_editable = [k for k in emp if k not in TRACKED_FIELDS and not k.startswith("_")]

            st.markdown("**Tracked Fields (with history)**")
            edit_vals = {}
            cols = st.columns(2)
            for i, field in enumerate(editable):
                with cols[i % 2]:
                    edit_vals[field] = st.text_input(field, value=emp.get(field, ""), key=f"edit_{field}")

            st.markdown("**Other Fields** (direct update, no history)")
            other_vals = {}
            cols2 = st.columns(2)
            other_fields = [f for f in ["Email", "NT Login", "Shift Timing", "Gender",
                                         "Seat Number", "Department", "DOJ Knack", "DOJ Project",
                                         "Date of Separation"] if f in emp]
            for i, field in enumerate(other_fields):
                with cols2[i % 2]:
                    other_vals[field] = st.text_input(field, value=emp.get(field, ""), key=f"other_{field}")

            if st.button("💾 Save Changes", type="primary"):
                saved = 0
                for field, new_val in edit_vals.items():
                    if new_val != emp.get(field, ""):
                        ok = record_manual_edit(ecn, field, new_val, eff_date_str)
                        if ok:
                            saved += 1

                # Direct update for non-tracked fields
                direct_update = {k: v for k, v in other_vals.items() if v != emp.get(k, "")}
                if direct_update:
                    get_employees_col().update_one({"ECN": ecn}, {"$set": direct_update})
                    saved += len(direct_update)

                if saved:
                    st.success(f"✅ {saved} field(s) updated, effective {eff_date_str}")
                    st.rerun()
                else:
                    st.info("No changes detected.")

        with tab2:
            st.markdown(f"**Change history for ECN {ecn}**")
            hist = get_history_col()
            history = list(hist.find({"ECN": ecn}, {"_id": 0}).sort("start_date", -1))
            if history:
                hist_df = pd.DataFrame(history)[["field", "value", "prev_value", "start_date", "end_date"]]
                hist_df.columns = ["Field", "Value", "Previous Value", "Start Date", "End Date"]
                st.dataframe(hist_df, use_container_width=True, hide_index=True)
            else:
                st.info("No history found for this employee.")


# ─── PAGE: DATE SNAPSHOT ──────────────────────────────────────────────────────
elif page == "📅 Date Snapshot":
    st.title("📅 Date Snapshot")
    st.markdown("View staffing data **as it was on any specific date**, applying all historical changes.")

    snap_date = st.date_input("Select snapshot date", value=date.today())
    snap_str = snap_date.isoformat()

    filter_client = st.text_input("Filter by Client (optional)")
    filter_bb = st.selectbox("Filter Billable/Buffer", ["All", "Billable", "Buffer", "Support", "Training", "Excluded"])

    if st.button("📸 Load Snapshot", type="primary"):
        if get_employees_col() is None:
            st.error("Connect MongoDB first.")
        else:
            with st.spinner(f"Building snapshot for {snap_str}..."):
                df = get_employees_at_date(snap_str)

            if df.empty:
                st.warning("No data found.")
            else:
                if filter_client:
                    df = df[df["Client"].str.contains(filter_client, case=False, na=False)]
                if filter_bb != "All" and "Billable/Buffer" in df.columns:
                    df = df[df["Billable/Buffer"] == filter_bb]

                st.success(f"✅ Snapshot for **{snap_str}** — **{len(df):,} employees**")

                # Summary metrics
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

                # Download button
                excel_bytes = df_to_excel_bytes(df)
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

    exp_type = st.radio("Export Type", ["Daily", "Weekly", "Monthly", "Yearly", "Custom Range"], horizontal=True)

    today = date.today()

    if exp_type == "Daily":
        exp_date = st.date_input("Select date", value=today)
        start_date = exp_date.isoformat()
        end_date = exp_date.isoformat()
        label = f"daily_{exp_date}"

    elif exp_type == "Weekly":
        # Show week picker
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
            year = st.number_input("Year", min_value=2020, max_value=2030, value=today.year)
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        start_date = date(year, month, 1).isoformat()
        end_date = date(year, month, last_day).isoformat()
        label = f"monthly_{year}_{month:02d}"

    elif exp_type == "Yearly":
        year = st.number_input("Year", min_value=2020, max_value=2030, value=today.year)
        start_date = date(year, 1, 1).isoformat()
        end_date = date(year, 12, 31).isoformat()
        label = f"yearly_{year}"

    else:  # Custom
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("Start date", value=today - timedelta(days=7))
        with col2:
            end = st.date_input("End date", value=today)
        start_date = start.isoformat()
        end_date = end.isoformat()
        label = f"custom_{start_date}_to_{end_date}"

    st.markdown(f"**Range:** `{start_date}` → `{end_date}`")

    # Export options
    st.divider()
    export_mode = st.radio("Export Mode", [
        "End-of-period snapshot",
        "Daily snapshots (one sheet per day)",
    ], help="End-of-period: single snapshot at the last day. Daily: one tab per day in Excel.")

    filter_active = st.checkbox("Active employees only", value=True)
    filter_client2 = st.text_input("Filter by Client (optional)", key="exp_client")

    if st.button("📥 Generate Export", type="primary"):
        if get_employees_col() is None:
            st.error("Connect MongoDB first.")
        else:
            if export_mode == "End-of-period snapshot":
                with st.spinner(f"Generating snapshot at {end_date}..."):
                    df = get_employees_at_date(end_date)
                    if filter_active and "Active/Inactive" in df.columns:
                        df = df[df["Active/Inactive"] == "Active"]
                    if filter_client2 and "Client" in df.columns:
                        df = df[df["Client"].str.contains(filter_client2, case=False, na=False)]

                st.success(f"✅ {len(df):,} employees as of {end_date}")
                excel_bytes = df_to_excel_bytes(df)
                st.download_button(
                    f"⬇️ Download {exp_type} Export",
                    data=excel_bytes,
                    file_name=f"staffing_{label}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            else:
                # Daily snapshots
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

                            sheet_name = d.replace("-", "")[-6:]  # MMDDYY
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

    # ── Summary Chart ──
    st.divider()
    st.subheader("Quick Summary")
    col = get_employees_col()
    if col is not None:
        total = col.count_documents({})
        active = col.count_documents({"Active/Inactive": "Active"})
        billable = col.count_documents({"Billable/Buffer": "Billable"})
        buffer_ = col.count_documents({"Billable/Buffer": "Buffer"})

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Employees", f"{total:,}")
        c2.metric("Active", f"{active:,}")
        c3.metric("Billable", f"{billable:,}")
        c4.metric("Buffer", f"{buffer_:,}")
