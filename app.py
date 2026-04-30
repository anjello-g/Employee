import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import io
import os
import calendar
import certifi
from urllib.parse import quote_plus, urlparse, urlunparse
import warnings
warnings.filterwarnings("ignore")

# Try TiDB (PyMySQL/SQLAlchemy), fallback to pymongo if needed
try:
    import pymysql
    from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Integer, JSON
    from sqlalchemy.orm import sessionmaker
    TIDB_AVAILABLE = True
except ImportError:
    TIDB_AVAILABLE = False

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Staffing Dashboard",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── DATABASE CONNECTION ──────────────────────────────────────────────────────
def parse_and_escape_uri(uri: str) -> str:
    if not uri or not uri.startswith(("mysql", "mongodb")):
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
        uri = st.secrets.get("TIDB_URI", "")
    except Exception:
        pass
    if not uri:
        uri = os.environ.get("TIDB_URI", "")
    if not uri:
        return None, None

    uri = parse_and_escape_uri(uri)

    if not TIDB_AVAILABLE:
        st.session_state["_db_err"] = "TiDB drivers not installed. Add 'PyMySQL>=1.1' and 'SQLAlchemy>=2.0' to requirements.txt"
        return None, None

    try:
        # TiDB connection with SSL
        engine = create_engine(
            uri,
            connect_args={
                "ssl": {"ca": certifi.where()},
                "connect_timeout": 10,
            },
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Create tables if not exist
        metadata = MetaData()

        # Employees table - flexible JSON for dynamic columns
        employees = Table(
            'employees', metadata,
            Column('ecn', String(50), primary_key=True),
            Column('data', JSON),
            Column('created_at', String(10)),
            Column('updated_at', String(10)),
            Column('last_upload', String(10)),
            mysql_engine='InnoDB'
        )

        # History table
        history = Table(
            'history', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('ecn', String(50), index=True),
            Column('employee_name', String(200)),
            Column('field', String(100), index=True),
            Column('value', String(500)),
            Column('prev_value', String(500)),
            Column('start_date', String(10), index=True),
            Column('end_date', String(10), index=True),
            Column('source', String(20)),
            mysql_engine='InnoDB'
        )

        # Upload log
        upload_log = Table(
            'upload_log', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('upload_date', String(10), index=True),
            Column('rows_processed', Integer),
            Column('inserted', Integer),
            Column('updated', Integer),
            Column('skipped_manual', Integer),
            mysql_engine='InnoDB'
        )

        metadata.create_all(engine)

        return engine, metadata
    except Exception as e:
        st.session_state["_db_err"] = str(e)
        return None, None

def get_engine():
    engine, _ = get_db()
    return engine

def get_table(name):
    _, metadata = get_db()
    if metadata is None:
        return None
    return Table(name, metadata, autoload_with=get_engine())

def db_connected():
    engine, _ = get_db()
    return engine is not None

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()

def parse_date(v):
    """Parse various date formats to YYYY-MM-DD string."""
    if not v or str(v).strip() == "":
        return None
    try:
        if isinstance(v, (datetime, pd.Timestamp)):
            return v.strftime("%Y-%m-%d")
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%Y/%m/%d"]:
            try:
                return datetime.strptime(str(v).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        try:
            return pd.to_datetime(float(v), unit='D', origin='1899-12-30').strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass
    except Exception:
        pass
    return None

def row_to_doc(row: dict) -> dict:
    return {k.replace(".", "_"): safe_str(v) for k, v in row.items()}

def load_excel(uploaded_file) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(uploaded_file)
        sheet = "Consolidated Staffing" if "Consolidated Staffing" in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(uploaded_file, sheet_name=sheet, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.fillna("")
        if "ECN" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["ECN"], keep="last")
            if len(df) < before:
                st.toast(f"⚠️ Removed {before - len(df)} duplicate ECN rows", icon="⚠️")
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

def get_effective_dates(row: dict, upload_date: str) -> tuple:
    eff_from = parse_date(row.get("Effective From", ""))
    eff_to = parse_date(row.get("Effective To", ""))

    if not eff_from:
        doj = parse_date(row.get("DOJ Knack", ""))
        if doj and doj <= upload_date:
            eff_from = doj
        else:
            eff_from = upload_date

    if not eff_to:
        eff_to = "9999-12-31"

    return eff_from, eff_to

def generate_template_bytes() -> bytes:
    template_data = {
        "ECN": ["EMP001", "EMP002"],
        "Employee": ["John Doe", "Jane Smith"],
        "DOJ Knack": ["2024-01-15", "2024-03-01"],
        "Date of Separation": ["", ""],
        "Effective From": ["", ""],
        "Effective To": ["", ""],
        "Client": ["ABC Corp", "XYZ Inc"],
        "Sub-Process": ["Support", "Billing"],
        "Supervisor": ["Manager A", "Manager B"],
        "Role": ["Agent", "Senior Agent"],
        "Manager": ["Director X", "Director Y"],
        "DOJ Project": ["2024-01-15", "2024-03-01"],
        "Shift Timing": ["9AM-6PM", "10AM-7PM"],
        "Email": ["john@company.com", "jane@company.com"],
        "NT Login": ["jdoe", "jsmith"],
        "Structure": ["Ops", "Ops"],
        "Billable/Buffer": ["Billable", "Buffer"],
        "Process Owner": ["Owner 1", "Owner 2"],
        "Department": ["Customer Service", "Finance"],
        "Location": ["Manila", "Cebu"],
        "Allocated Seats": ["A1", "B2"],
        "Gender": ["Male", "Female"],
        "Seat Number": ["101", "102"],
        "Global ID (GPP)": ["GPP001", "GPP002"],
        "Active/Inactive": ["Active", "Active"],
        "CDP Email": ["john.cdp@company.com", "jane.cdp@company.com"],
        "BufferAgent": ["", ""],
        "EWS Type": ["", ""],
        "Driver": ["", ""],
        "Expected Move Date": ["", ""],
        "Overall Location": ["PH", "PH"],
        "Client Approved Billable": ["Yes", "No"],
        "Tagging": ["", ""],
        "Role Tagging": ["", ""],
        "Specialty": ["", ""],
    }
    df = pd.DataFrame(template_data)
    return df_to_excel_bytes(df, sheet_name="Consolidated Staffing")

# ─── CORE LOGIC ──────────────────────────────────────────────────────────────
BATCH_SIZE = 1000

def get_employee(engine, ecn):
    """Get employee by ECN."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT data FROM employees WHERE ecn = :ecn"),
            {"ecn": ecn}
        ).fetchone()
        if result:
            import json
            data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            data["ECN"] = ecn
            return data
        return None

def get_all_employees(engine):
    """Get all employees as list of dicts."""
    with engine.connect() as conn:
        results = conn.execute(text("SELECT ecn, data FROM employees")).fetchall()
        employees = []
        for row in results:
            import json
            data = json.loads(row[1]) if isinstance(row[1], str) else row[1]
            data["ECN"] = row[0]
            employees.append(data)
        return employees

def upsert_employees(df: pd.DataFrame, upload_date: str, progress_bar=None):
    engine = get_engine()
    if engine is None:
        return 0, 0, "Database not connected"

    total_rows = len(df)

    # Preload all employees
    existing_docs = {}
    for emp in get_all_employees(engine):
        existing_docs[emp["ECN"]] = emp

    # Preload manual edits
    manual_edits = {}
    with engine.connect() as conn:
        results = conn.execute(
            text("SELECT ecn, field, MAX(start_date) as max_date FROM history WHERE source = 'manual_edit' GROUP BY ecn, field")
        ).fetchall()
        for row in results:
            manual_edits[(row[0], row[1])] = row[2]

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

        eff_from, eff_to = get_effective_dates(row.to_dict(), upload_date)

        if existing is None:
            # New employee
            import json
            with engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO employees (ecn, data, created_at, updated_at, last_upload) VALUES (:ecn, :data, :created, :updated, :upload)"),
                    {
                        "ecn": ecn,
                        "data": json.dumps(doc),
                        "created": upload_date,
                        "updated": upload_date,
                        "upload": upload_date
                    }
                )
                conn.commit()

            inserted += 1

            # Seed history for non-empty fields
            for field, val in doc.items():
                if field.startswith("_") or field in ("Effective From", "Effective To") or val == "":
                    continue
                with engine.connect() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source)
                            VALUES (:ecn, :emp, :field, :val, '', :start, :end, 'excel_upload')
                            ON DUPLICATE KEY UPDATE
                            value = VALUES(value), end_date = VALUES(end_date)
                        """),
                        {
                            "ecn": ecn, "emp": doc.get("Employee", ""),
                            "field": field, "val": val,
                            "start": eff_from, "end": eff_to
                        }
                    )
                    conn.commit()
        else:
            # Existing employee
            changed = False
            last_upload = existing.get("_last_upload", "2000-01-01")
            import json

            for field, new_val in doc.items():
                if field.startswith("_") or field in ("Effective From", "Effective To"):
                    continue
                old_val = existing.get(field, "")

                if new_val != old_val:
                    manual_date = manual_edits.get((ecn, field))
                    if manual_date and manual_date > last_upload:
                        skipped_manual += 1
                        continue

                    # Close previous history
                    with engine.connect() as conn:
                        conn.execute(
                            text("UPDATE history SET end_date = :upload WHERE ecn = :ecn AND field = :field AND end_date = '9999-12-31'"),
                            {"upload": upload_date, "ecn": ecn, "field": field}
                        )
                        conn.commit()

                    # Insert new history
                    with engine.connect() as conn:
                        conn.execute(
                            text("""
                                INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source)
                                VALUES (:ecn, :emp, :field, :val, :prev, :start, :end, 'excel_upload')
                                ON DUPLICATE KEY UPDATE
                                value = VALUES(value), prev_value = VALUES(prev_value), end_date = VALUES(end_date)
                            """),
                            {
                                "ecn": ecn, "emp": doc.get("Employee", existing.get("Employee", "")),
                                "field": field, "val": new_val, "prev": old_val,
                                "start": eff_from, "end": eff_to
                            }
                        )
                        conn.commit()

                    changed = True

            if changed:
                # Update employee data
                existing.update(doc)
                existing["_updated_at"] = upload_date
                existing["_last_upload"] = upload_date
                with engine.connect() as conn:
                    conn.execute(
                        text("UPDATE employees SET data = :data, updated_at = :updated, last_upload = :upload WHERE ecn = :ecn"),
                        {
                            "ecn": ecn,
                            "data": json.dumps(existing),
                            "updated": upload_date,
                            "upload": upload_date
                        }
                    )
                    conn.commit()
                updated += 1
            else:
                with engine.connect() as conn:
                    conn.execute(
                        text("UPDATE employees SET last_upload = :upload WHERE ecn = :ecn"),
                        {"ecn": ecn, "upload": upload_date}
                    )
                    conn.commit()

        if progress_bar is not None and idx % 50 == 0:
            progress_bar.progress(min(0.99, (idx + 1) / total_rows),
                                  text=f"Processed {idx + 1:,}/{total_rows:,}...")

    if progress_bar is not None:
        progress_bar.progress(1.0, text="Done!")

    # Log upload
    with engine.connect() as conn:
        conn.execute(
            text("INSERT INTO upload_log (upload_date, rows_processed, inserted, updated, skipped_manual) VALUES (:date, :rows, :ins, :upd, :skip)"),
            {"date": upload_date, "rows": total_rows, "ins": inserted, "upd": updated, "skip": skipped_manual}
        )
        conn.commit()

    return inserted, updated, None


def get_employees_at_date(query_date: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    employees = get_all_employees(engine)
    if not employees:
        return pd.DataFrame()

    df = pd.DataFrame(employees)

    # Filter by DOJ
    if "DOJ Knack" in df.columns:
        df["__doj_parsed"] = df["DOJ Knack"].apply(parse_date)
        df = df[df["__doj_parsed"].isna() | (df["__doj_parsed"] <= query_date)]
        df = df.drop(columns=["__doj_parsed"])

    # Filter by separation
    if "Date of Separation" in df.columns:
        df["__sep_parsed"] = df["Date of Separation"].apply(parse_date)
        df = df[df["__sep_parsed"].isna() | (df["__sep_parsed"] >= query_date)]
        df = df.drop(columns=["__sep_parsed"])

    # Apply history overrides
    with engine.connect() as conn:
        results = conn.execute(
            text("SELECT ecn, field, value FROM history WHERE start_date <= :date AND end_date > :date"),
            {"date": query_date}
        ).fetchall()

    overrides = {}
    for row in results:
        key = (row[0], row[1])
        if key not in overrides:
            overrides[key] = row[2]

    for (ecn, field), val in overrides.items():
        if field in df.columns:
            mask = df["ECN"] == ecn
            df.loc[mask, field] = val

    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal, errors="ignore")
    return df


def record_manual_edit(ecn: str, field: str, new_value: str, start_date: str, end_date: str = "9999-12-31"):
    engine = get_engine()
    if engine is None:
        return False, "Database not connected"

    try:
        existing = get_employee(engine, ecn)
        if not existing:
            return False, "Employee not found"

        old_value = existing.get(field, "")

        with engine.connect() as conn:
            # Close overlapping history
            conn.execute(
                text("UPDATE history SET end_date = :start WHERE ecn = :ecn AND field = :field AND end_date = '9999-12-31' AND start_date <= :start"),
                {"start": start_date, "ecn": ecn, "field": field}
            )
            conn.execute(
                text("UPDATE history SET end_date = :start WHERE ecn = :ecn AND field = :field AND start_date > :start AND start_date < :end"),
                {"start": start_date, "end": end_date, "ecn": ecn, "field": field}
            )

            # Insert new record
            conn.execute(
                text("""
                    INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source)
                    VALUES (:ecn, :emp, :field, :val, :prev, :start, :end, 'manual_edit')
                    ON DUPLICATE KEY UPDATE
                    value = VALUES(value), prev_value = VALUES(prev_value), end_date = VALUES(end_date)
                """),
                {
                    "ecn": ecn, "emp": existing.get("Employee", ""),
                    "field": field, "val": new_value, "prev": old_value,
                    "start": start_date, "end": end_date
                }
            )

            # Update current record if ongoing
            if end_date == "9999-12-31":
                import json
                existing[field] = new_value
                existing["_updated_at"] = start_date
                conn.execute(
                    text("UPDATE employees SET data = :data, updated_at = :updated WHERE ecn = :ecn"),
                    {"ecn": ecn, "data": json.dumps(existing), "updated": start_date}
                )

            conn.commit()
        return True, "Saved"

    except Exception as e:
        return False, f"Error: {str(e)[:100]}"


def get_employee_history(ecn: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    with engine.connect() as conn:
        results = conn.execute(
            text("SELECT ecn, employee_name, field, value, prev_value, start_date, end_date, source FROM history WHERE ecn = :ecn ORDER BY field, start_date DESC"),
            {"ecn": ecn}
        ).fetchall()

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results, columns=["ECN", "Employee", "field", "value", "prev_value", "start_date", "end_date", "source"])
    return df


def delete_history_record(ecn: str, field: str, start_date: str):
    engine = get_engine()
    if engine is None:
        return False, "Database not connected"

    try:
        with engine.connect() as conn:
            # Find record to delete
            record = conn.execute(
                text("SELECT * FROM history WHERE ecn = :ecn AND field = :field AND start_date = :start"),
                {"ecn": ecn, "field": field, "start": start_date}
            ).fetchone()

            if not record:
                return False, "Record not found"

            # Delete
            conn.execute(
                text("DELETE FROM history WHERE ecn = :ecn AND field = :field AND start_date = :start"),
                {"ecn": ecn, "field": field, "start": start_date}
            )

            # Find previous to restore
            prev = conn.execute(
                text("SELECT * FROM history WHERE ecn = :ecn AND field = :field AND end_date = :start ORDER BY start_date DESC LIMIT 1"),
                {"ecn": ecn, "field": field, "start": start_date}
            ).fetchone()

            if prev:
                conn.execute(
                    text("UPDATE history SET end_date = '9999-12-31' WHERE ecn = :ecn AND field = :field AND start_date = :prev_start"),
                    {"ecn": ecn, "field": field, "prev_start": prev[5]}
                )

                # Update current employee
                import json
                emp = get_employee(engine, ecn)
                if emp:
                    emp[field] = prev[3]
                    conn.execute(
                        text("UPDATE employees SET data = :data WHERE ecn = :ecn"),
                        {"ecn": ecn, "data": json.dumps(emp)}
                    )
            else:
                import json
                emp = get_employee(engine, ecn)
                if emp:
                    emp[field] = ""
                    conn.execute(
                        text("UPDATE employees SET data = :data WHERE ecn = :ecn"),
                        {"ecn": ecn, "data": json.dumps(emp)}
                    )

            conn.commit()
        return True, "Deleted and restored previous value"

    except Exception as e:
        return False, f"Error: {str(e)[:100]}"


def compact_history():
    engine = get_engine()
    if engine is None:
        return 0

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM history WHERE value = prev_value AND value != ''")
            )
            conn.commit()
            return result.rowcount
    except Exception:
        return 0


def get_db_stats():
    engine = get_engine()
    if engine is None:
        return None, None, None

    with engine.connect() as conn:
        emp_count = conn.execute(text("SELECT COUNT(*) FROM employees")).scalar()
        active_count = conn.execute(text("SELECT COUNT(*) FROM employees WHERE JSON_EXTRACT(data, '$.Active/Inactive') = 'Active'")).scalar()
        hist_count = conn.execute(text("SELECT COUNT(*) FROM history")).scalar()

    return emp_count, active_count, hist_count


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/group.png", width=60)
    st.title("Staffing App")
    st.divider()

    if db_connected():
        st.success("✅ TiDB Connected")
    else:
        err = st.session_state.get("_db_err", "")
        if err:
            st.error(f"❌ {err[:120]}")
        else:
            st.warning("⚠️ No TiDB URI configured")

    st.divider()
    page = st.radio("Navigation", [
        "📤 Upload / Sync",
        "👤 Employee Editor",
        "📅 Date Snapshot",
        "📊 Export Data",
        "📜 History Manager",
        "🛠️ DB Tools",
    ])

# ─── PAGE: UPLOAD ─────────────────────────────────────────────────────────────
if page == "📤 Upload / Sync":
    st.title("📤 Upload & Sync Staffing Data")
    st.markdown("Upload the **Consolidated Staffing** Excel file to populate or update the database.")

    if not db_connected():
        st.error("Please configure a valid TiDB URI in Streamlit Secrets first.")
        st.stop()

    # Template download
    st.subheader("📋 Template")
    st.markdown("Download the template to see the required format:")
    template_bytes = generate_template_bytes()
    st.download_button(
        label="⬇️ Download Excel Template",
        data=template_bytes,
        file_name="staffing_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="Download a template with all recommended columns and sample data"
    )

    st.divider()

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Choose Excel file (.xlsx)", type=["xlsx"])
    with col2:
        st.markdown("**Instructions**")
        st.markdown("""
        - **Required column:** `ECN` (unique employee ID)
        - **Optional date columns:** `Effective From`, `Effective To`
        - **Recommended:** `DOJ Knack`, `Date of Separation`
        - **Any other columns** are accepted — the app adapts automatically
        - **First upload:** populates the DB (baseline history = DOJ Knack or upload date)
        - **Subsequent uploads:** updates only changed records
        - **In-app edits are protected:** uploads will NOT overwrite fields manually edited after the last upload
        """)

    if uploaded:
        with st.spinner("Reading file..."):
            df = load_excel(uploaded)

        if df.empty:
            st.error("Could not read the Excel file.")
            st.stop()

        st.success(f"✅ File loaded — **{len(df):,} rows**, **{len(df.columns)} columns**")
        st.caption(f"Columns detected: {', '.join(df.columns[:12])}{'...' if len(df.columns) > 12 else ''}")

        with st.expander("Preview (first 10 rows)"):
            st.dataframe(df.head(10), use_container_width=True)

        if st.button("🚀 Sync to Database", type="primary"):
            today_str = date.today().isoformat()
            progress = st.progress(0, text="Preparing...")

            with st.spinner("Writing to TiDB..."):
                inserted, updated, err = upsert_employees(df, today_str, progress_bar=progress)

            if err:
                st.error(err)
            else:
                emp_count, _, _ = get_db_stats()
                st.success(f"""
                ✅ Sync complete!
                - **{inserted}** new employees added
                - **{updated}** existing employees updated
                - **{emp_count:,}** total employees in DB
                """)

    st.divider()
    st.subheader("📊 Database Stats")
    emp_count, active_count, hist_count = get_db_stats()
    if emp_count is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Employees", f"{emp_count:,}")
        c2.metric("Active", f"{active_count:,}")
        c3.metric("History Records", f"{hist_count:,}")
    else:
        st.info("Connect to TiDB to see stats.")


# ─── PAGE: EMPLOYEE EDITOR ────────────────────────────────────────────────────
elif page == "👤 Employee Editor":
    st.title("👤 Employee Editor")
    engine = get_engine()
    if engine is None:
        st.error("Connect TiDB first.")
        st.stop()

    st.info("""
    **How manual edits work:**
    - Click any row in the table below to open the edit dialog
    - Set **Effective From** and optionally **Effective To**
    - Leave **Effective To** blank for ongoing changes
    - The change is recorded in history for that date range
    - **Future Excel uploads will skip this field** if they occur after your manual edit date
    """)

    search = st.text_input("🔍 Search by name, ECN, or email", placeholder="e.g. Santos, 12345")
    filter_status = st.selectbox("Filter by Status", ["All", "Active", "Inactive", "LOA", "Maternity", "Suspended"])

    # Build query for TiDB
    employees = get_all_employees(engine)
    filtered = []
    for emp in employees:
        match = True
        if filter_status != "All":
            if emp.get("Active/Inactive") != filter_status:
                match = False
        if search:
            search_lower = search.lower()
            if not (search_lower in emp.get("Employee", "").lower() or
                    search_lower in emp.get("ECN", "").lower() or
                    search_lower in emp.get("Email", "").lower()):
                match = False
        if match:
            filtered.append(emp)

    if not filtered:
        st.warning("No employees found.")
        st.stop()

    df_list = pd.DataFrame(filtered)
    display_cols = ["ECN", "Employee", "Client", "Sub-Process", "Role", "Billable/Buffer",
                    "Active/Inactive", "Location", "Overall Location"]
    display_cols = [c for c in display_cols if c in df_list.columns]

    st.markdown(f"**{len(filtered)} employees found** (max 200 shown)")
    st.caption("👆 Click any row to edit that employee")

    selected_rows = st.dataframe(
        df_list[display_cols],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    @st.dialog("✏️ Edit Employee", width="large")
    def edit_employee_modal(emp):
        ecn = emp["ECN"]
        st.subheader(f"{emp.get('Employee', ecn)} (ECN: {ecn})")

        tab1, tab2 = st.tabs(["Edit Fields", "Field History"])

        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                eff_from = st.date_input("Effective From", value=date.today(), key=f"eff_from_{ecn}")
            with col2:
                eff_to = st.date_input("Effective To (leave blank for ongoing)", value=None, key=f"eff_to_{ecn}")

            eff_from_str = eff_from.isoformat()
            eff_to_str = eff_to.isoformat() if eff_to else "9999-12-31"

            if eff_to and eff_to <= eff_from:
                st.error("Effective To must be after Effective From")
                return

            # Core fields (always visible)
            st.markdown("**Core Fields**")
            core_fields = ["Billable/Buffer", "Active/Inactive", "Client", "Sub-Process",
                           "Supervisor", "Role", "Manager", "Location", "Overall Location"]
            core_fields = [f for f in core_fields if f in emp]

            edit_vals = {}
            cols = st.columns(3)
            for i, field in enumerate(core_fields):
                with cols[i % 3]:
                    edit_vals[field] = st.text_input(
                        field, value=emp.get(field, ""), key=f"edit_core_{ecn}_{field}"
                    )

            # Show more toggle
            show_more = st.toggle("🔽 Show More Fields", key=f"show_more_{ecn}")

            if show_more:
                st.markdown("**Additional Fields**")
                other_fields = [k for k in emp.keys() if not k.startswith("_") and k not in core_fields and k not in ("Effective From", "Effective To")]
                cols2 = st.columns(3)
                for i, field in enumerate(other_fields):
                    with cols2[i % 3]:
                        edit_vals[field] = st.text_input(
                            field, value=emp.get(field, ""), key=f"edit_extra_{ecn}_{field}"
                        )

            # Confirm dialog before saving
            if st.button("💾 Save Changes", type="primary", key=f"save_{ecn}"):
                # Check if any changes were made
                changes_made = []
                for field, new_val in edit_vals.items():
                    if new_val != emp.get(field, ""):
                        changes_made.append(f"{field}: {emp.get(field, '')} → {new_val}")

                if not changes_made:
                    st.info("No changes detected.")
                    return

                # Show confirmation
                st.warning("**Please confirm the following changes:**")
                for change in changes_made:
                    st.markdown(f"- {change}")
                st.markdown(f"**Effective:** {eff_from_str} to {eff_to_str if eff_to_str != '9999-12-31' else 'ongoing'}")

                if st.button("✅ Confirm Save", type="primary", key=f"confirm_{ecn}"):
                    saved = 0
                    errors = []
                    for field, new_val in edit_vals.items():
                        if new_val != emp.get(field, ""):
                            ok, msg = record_manual_edit(ecn, field, new_val, eff_from_str, eff_to_str)
                            if ok:
                                saved += 1
                            else:
                                errors.append(f"{field}: {msg}")

                    if errors:
                        for e in errors:
                            st.error(e)
                    if saved:
                        st.success(f"✅ {saved} field(s) updated!")
                        st.rerun()

        with tab2:
            st.markdown(f"**Change history for ECN {ecn}**")
            hist_df = get_employee_history(ecn)
            if not hist_df.empty:
                display = hist_df[["field", "value", "prev_value", "start_date", "end_date", "source"]]
                display.columns = ["Field", "Value", "Previous", "Start", "End", "Source"]
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.info("No history found for this employee.")

    if selected_rows and selected_rows.selection.rows:
        idx = selected_rows.selection.rows[0]
        emp = filtered[idx]
        edit_employee_modal(emp)


# ─── PAGE: DATE SNAPSHOT ──────────────────────────────────────────────────────
elif page == "📅 Date Snapshot":
    st.title("📅 Date Snapshot")
    st.markdown("View staffing data **as it was on any specific date**, applying all historical changes.")

    if not db_connected():
        st.error("Connect TiDB first.")
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
    st.markdown("Export staffing data for any time range. All dates in one sheet with `Date Exported` column.")

    if not db_connected():
        st.error("Connect TiDB first.")
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
        "Single sheet (all dates appended with Date Exported column)",
        "One sheet per day",
    ], help="Single sheet: all dates in one tab, filterable by Date Exported. One sheet per day: separate tabs.")

    filter_active = st.checkbox("Active employees only", value=False)
    filter_client2 = st.text_input("Filter by Client (optional)", key="exp_client")

    if st.button("📥 Generate Export", type="primary"):
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end_dt - start_dt).days + 1

        if days > 366:
            st.warning("Export limited to 366 days. Please narrow your range.")
            st.stop()

        if export_mode == "Single sheet (all dates appended with Date Exported column)":
            all_dfs = []
            status_text = st.empty()

            for i in range(days):
                d = (start_dt + timedelta(days=i)).date().isoformat()
                df_day = get_employees_at_date(d)
                if df_day.empty:
                    continue
                if filter_active and "Active/Inactive" in df_day.columns:
                    df_day = df_day[df_day["Active/Inactive"] == "Active"]
                if filter_client2 and "Client" in df_day.columns:
                    df_day = df_day[df_day["Client"].str.contains(filter_client2, case=False, na=False)]

                if not df_day.empty:
                    df_day["Date Exported"] = d
                    all_dfs.append(df_day)

                if i % 10 == 0 or i == days - 1:
                    status_text.text(f"Processing... {i + 1}/{days} days")

            if not all_dfs:
                st.warning("No data found for the selected range.")
                st.stop()

            combined_df = pd.concat(all_dfs, ignore_index=True)
            cols = ["Date Exported"] + [c for c in combined_df.columns if c != "Date Exported"]
            combined_df = combined_df[cols]

            st.success(f"✅ {len(combined_df):,} rows across {len(all_dfs)} days")
            st.dataframe(combined_df.head(20), use_container_width=True)

            excel_bytes = df_to_excel_bytes(combined_df, sheet_name="Staffing Export")
            st.download_button(
                f"⬇️ Download {exp_type} Export (Single Sheet)",
                data=excel_bytes,
                file_name=f"staffing_{label}_single_sheet.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        else:
            if days > 31:
                st.warning("One sheet per day mode is limited to 31 days.")
                st.stop()

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                wb = writer.book
                header_fmt = wb.add_format({"bold": True, "bg_color": "#1f4e79", "font_color": "white"})

                status_text = st.empty()
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
                    
                    if i % 5 == 0 or i == days - 1:
                        status_text.text(f"Building sheets... {i + 1}/{days}")

            st.success(f"✅ {days} daily sheets generated!")
            st.download_button(
                "⬇️ Download Daily Export",
                data=buf.getvalue(),
                file_name=f"staffing_{label}_daily.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# ─── PAGE: HISTORY MANAGER ───────────────────────────────────────────────────
elif page == "📜 History Manager":
    st.title("📜 History Manager")
    st.markdown("View, edit, and delete history records. Use this to correct errors in historical data.")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    engine = get_engine()

    # Search for employee
    search = st.text_input("🔍 Search by ECN or Employee name", placeholder="e.g. EMP001 or John Doe")

    # Get all history records
    with engine.connect() as conn:
        if search:
            results = conn.execute(
                text("""
                    SELECT * FROM history 
                    WHERE ecn LIKE :search OR employee_name LIKE :search 
                    ORDER BY ecn, field, start_date DESC 
                    LIMIT 500
                """),
                {"search": f"%{search}%"}
            ).fetchall()
        else:
            results = conn.execute(
                text("SELECT * FROM history ORDER BY ecn, field, start_date DESC LIMIT 500")
            ).fetchall()

    if not results:
        st.info("No history records found.")
        st.stop()

    # Convert to DataFrame
    hist_df = pd.DataFrame(results, columns=["id", "ECN", "Employee", "field", "value", "prev_value", "start_date", "end_date", "source"])

    st.markdown(f"**{len(hist_df)} history records found** (max 500 shown)")

    display_cols = ["ECN", "Employee", "field", "value", "prev_value", "start_date", "end_date", "source"]
    display_cols = [c for c in display_cols if c in hist_df.columns]

    selected = st.dataframe(
        hist_df[display_cols],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    if selected and selected.selection.rows:
        idx = selected.selection.rows[0]
        record = results[idx]

        st.divider()
        st.subheader("🛠️ Manage Record")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**ECN:** {record[1]}")
            st.markdown(f"**Employee:** {record[2] or 'N/A'}")
            st.markdown(f"**Field:** {record[3]}")
        with col2:
            st.markdown(f"**Value:** {record[4]}")
            st.markdown(f"**Previous:** {record[5] or 'N/A'}")
            st.markdown(f"**Period:** {record[6]} → {record[7]}")

        st.divider()

        # Edit option
        with st.expander("✏️ Edit this record"):
            new_value = st.text_input("New Value", value=record[4], key=f"edit_hist_{idx}")
            new_start = st.text_input("Start Date", value=record[6], key=f"start_hist_{idx}")
            new_end = st.text_input("End Date (9999-12-31 for ongoing)", value=record[7], key=f"end_hist_{idx}")

            if st.button("💾 Update Record", type="primary", key=f"update_hist_{idx}"):
                try:
                    with engine.connect() as conn:
                        conn.execute(
                            text("""
                                UPDATE history 
                                SET value = :val, start_date = :start, end_date = :end 
                                WHERE id = :id
                            """),
                            {"val": new_value, "start": new_start, "end": new_end, "id": record[0]}
                        )
                        
                        # Update current employee record if this is the active one
                        if new_end == "9999-12-31":
                            emp = get_employee(engine, record[1])
                            if emp:
                                import json
                                emp[record[3]] = new_value
                                conn.execute(
                                    text("UPDATE employees SET data = :data WHERE ecn = :ecn"),
                                    {"data": json.dumps(emp), "ecn": record[1]}
                                )
                        
                        conn.commit()
                    st.success("✅ Record updated!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)[:100]}")

        # Delete option
        with st.expander("🗑️ Delete this record"):
            st.warning("This will delete the history record and restore the previous value if available.")
            if st.button("🗑️ Confirm Delete", type="primary", key=f"del_hist_{idx}"):
                ok, msg = delete_history_record(record[1], record[3], record[6])
                if ok:
                    st.success(f"✅ {msg}")
                    st.rerun()
                else:
                    st.error(msg)

    # Bulk operations
    st.divider()
    st.subheader("Bulk Operations")

    if st.button("🧹 Remove Redundant Records (value == prev_value)"):
        with st.spinner("Cleaning..."):
            deleted = compact_history()
        st.success(f"Removed **{deleted}** redundant records")

    if st.button("📊 Show History Statistics"):
        with engine.connect() as conn:
            stats = conn.execute(
                text("SELECT field, COUNT(*) as count FROM history GROUP BY field ORDER BY count DESC")
            ).fetchall()
        
        if stats:
            stats_df = pd.DataFrame(stats, columns=["Field", "Change Count"])
            st.dataframe(stats_df, use_container_width=True, hide_index=True)


# ─── PAGE: DB TOOLS ───────────────────────────────────────────────────────────
elif page == "🛠️ DB Tools":
    st.title("🛠️ Database Tools")
    st.markdown("Maintenance utilities for your TiDB database.")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Storage")
        try:
            emp_count, active_count, hist_count = get_db_stats()
            st.metric("Total Employees", f"{emp_count:,}")
            st.metric("History Records", f"{hist_count:,}")
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
    engine = get_engine()
    if engine is not None:
        with engine.connect() as conn:
            logs = conn.execute(
                text("SELECT * FROM upload_log ORDER BY upload_date DESC LIMIT 10")
            ).fetchall()
        
        if logs:
            logs_df = pd.DataFrame(logs, columns=["id", "upload_date", "rows_processed", "inserted", "updated", "skipped_manual"])
            st.dataframe(logs_df, use_container_width=True, hide_index=True)
        else:
            st.info("No uploads logged yet.")
