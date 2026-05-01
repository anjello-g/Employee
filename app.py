import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import io
import os
import calendar
import certifi
import json
import warnings
from urllib.parse import quote_plus, urlparse, urlunparse
from collections import defaultdict

warnings.filterwarnings("ignore")

# ─── DATABASE DRIVERS ─────────────────────────────────────────────────────────
try:
    import pymysql
    from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Integer, JSON
    TIDB_AVAILABLE = True
except ImportError:
    TIDB_AVAILABLE = False

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Staffing Dashboard",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── DATABASE CONNECTION ──────────────────────────────────────────────────────
def parse_and_escape_uri(uri: str) -> str:
    if not uri or not uri.startswith("mysql"):
        return uri
    try:
        parsed = urlparse(uri)
        if parsed.username and parsed.password:
            user, pwd = quote_plus(parsed.username), quote_plus(parsed.password)
            netloc = f"{user}:{pwd}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    except Exception:
        pass
    return uri

@st.cache_resource(show_spinner=False)
def get_db():
    uri = st.secrets.get("TIDB_URI", "") or os.environ.get("TIDB_URI", "")
    if not uri:
        return None, None
    if not TIDB_AVAILABLE:
        st.session_state["_db_err"] = "TiDB drivers not installed. Add 'PyMySQL>=1.1' and 'SQLAlchemy>=2.0' to requirements.txt"
        return None, None

    uri = parse_and_escape_uri(uri)
    try:
        engine = create_engine(
            uri,
            connect_args={"ssl": {"ca": certifi.where()}, "connect_timeout": 10},
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10,
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        metadata = MetaData()

        Table('employees', metadata,
            Column('ecn', String(50), primary_key=True),
            Column('data', JSON),
            Column('created_at', String(10)),
            Column('updated_at', String(10)),
            Column('last_upload', String(10)),
            mysql_engine='InnoDB'
        )

        Table('history', metadata,
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

        Table('upload_log', metadata,
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

def db_connected():
    return get_engine() is not None

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()

def parse_date(v):
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
        header_fmt = wb.add_format({"bold": True, "bg_color": "#1f4e79", "font_color": "white", "border": 1})
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, header_fmt)
            ws.set_column(col_num, col_num, max(15, len(str(col_name)) + 2))
    return buf.getvalue()

def get_effective_dates(row: dict, upload_date: str) -> tuple:
    eff_from = parse_date(row.get("Effective From", ""))
    eff_to = parse_date(row.get("Effective To", ""))
    if not eff_from:
        doj = parse_date(row.get("DOJ Knack", ""))
        eff_from = doj if doj and doj <= upload_date else upload_date
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
    return df_to_excel_bytes(pd.DataFrame(template_data), sheet_name="Consolidated Staffing")

# ─── FAST BULK DATABASE OPERATIONS ────────────────────────────────────────────
BATCH_SIZE = 1000

def get_all_employees_df(engine) -> pd.DataFrame:
    """Load all employees as DataFrame — fast bulk read."""
    if engine is None:
        return pd.DataFrame()
    query = text("SELECT ecn, data, created_at, updated_at, last_upload FROM employees")
    df = pd.read_sql(query, engine)
    if df.empty:
        return df
    records = []
    for _, row in df.iterrows():
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        data["ECN"] = row["ecn"]
        data["_created_at"] = row["created_at"]
        data["_updated_at"] = row["updated_at"]
        data["_last_upload"] = row["last_upload"]
        records.append(data)
    return pd.DataFrame(records)

def get_employee(engine, ecn: str) -> dict:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT data FROM employees WHERE ecn = :ecn"), {"ecn": ecn}).fetchone()
        if not row:
            return None
        data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        data["ECN"] = ecn
        return data

def upsert_employees(df: pd.DataFrame, upload_date: str, progress_bar=None):
    engine = get_engine()
    if engine is None:
        return 0, 0, "Database not connected"

    total_rows = len(df)
    existing_df = get_all_employees_df(engine)
    existing_map = {}
    if not existing_df.empty and "ECN" in existing_df.columns:
        existing_map = {row["ECN"]: row.to_dict() for _, row in existing_df.iterrows()}

    # Preload manual edits
    manual_edits = {}
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT ecn, field, MAX(start_date) as max_date 
            FROM history 
            WHERE source = 'manual_edit' 
            GROUP BY ecn, field
        """)).fetchall()
        manual_edits = {(r[0], r[1]): r[2] for r in result}

    inserted, updated, skipped_manual = 0, 0, 0
    new_employees = []
    history_records = []
    updated_employees = []
    unchanged_ecns = []

    for idx, (_, row) in enumerate(df.iterrows()):
        ecn = str(row.get("ECN", "")).strip()
        if not ecn or ecn.lower() == "nan":
            continue

        doc = row_to_doc(row.to_dict())
        doc["ECN"] = ecn
        existing = existing_map.get(ecn)
        eff_from, eff_to = get_effective_dates(row.to_dict(), upload_date)

        if existing is None:
            doc["_created_at"] = upload_date
            doc["_updated_at"] = upload_date
            doc["_last_upload"] = upload_date
            new_employees.append({
                "ecn": ecn,
                "data": json.dumps(doc),
                "created_at": upload_date,
                "updated_at": upload_date,
                "last_upload": upload_date
            })
            inserted += 1

            emp_name = doc.get("Employee", "")
            for field, val in doc.items():
                if field.startswith("_") or field in ("Effective From", "Effective To") or val == "":
                    continue
                history_records.append({
                    "ecn": ecn, "employee_name": emp_name, "field": field,
                    "value": val, "prev_value": "", "start_date": eff_from,
                    "end_date": eff_to, "source": "excel_upload"
                })
        else:
            changed = False
            last_upload = existing.get("_last_upload", "2000-01-01")
            emp_name = doc.get("Employee", existing.get("Employee", ""))

            for field, new_val in doc.items():
                if field.startswith("_") or field in ("Effective From", "Effective To"):
                    continue
                old_val = existing.get(field, "")
                if new_val != old_val:
                    manual_date = manual_edits.get((ecn, field))
                    if manual_date and manual_date > last_upload:
                        skipped_manual += 1
                        continue

                    history_records.append({
                        "ecn": ecn, "employee_name": emp_name, "field": field,
                        "value": new_val, "prev_value": old_val, "start_date": eff_from,
                        "end_date": eff_to, "source": "excel_upload",
                        "_close_prev": True, "_ecn": ecn, "_field": field
                    })
                    existing[field] = new_val
                    changed = True

            if changed:
                existing["_updated_at"] = upload_date
                existing["_last_upload"] = upload_date
                updated_employees.append({
                    "ecn": ecn,
                    "data": json.dumps(existing),
                    "updated_at": upload_date,
                    "last_upload": upload_date
                })
                updated += 1
            else:
                unchanged_ecns.append(ecn)

        if progress_bar is not None and idx % 50 == 0:
            progress_bar.progress(min(0.95, (idx + 1) / total_rows),
                                  text=f"Staging {idx + 1:,}/{total_rows:,}...")

    # ─── BULK WRITE TO DATABASE ─────────────────────────────────────────────
    if progress_bar is not None:
        progress_bar.progress(0.96, text="Writing to TiDB in batches...")

    with engine.connect() as conn:
        # 1. Insert new employees
        for i in range(0, len(new_employees), BATCH_SIZE):
            batch = new_employees[i:i + BATCH_SIZE]
            conn.execute(text("""
                INSERT INTO employees (ecn, data, created_at, updated_at, last_upload)
                VALUES (:ecn, :data, :created_at, :updated_at, :last_upload)
                ON DUPLICATE KEY UPDATE
                data = VALUES(data), updated_at = VALUES(updated_at), last_upload = VALUES(last_upload)
            """), batch)

        # 2. Close previous history records
        close_map = defaultdict(list)
        for h in history_records:
            if h.get("_close_prev"):
                close_map[(h["_ecn"], h["_field"])].append(h["start_date"])

        for (ecn, field), start_dates in close_map.items():
            min_start = min(start_dates)
            conn.execute(text("""
                UPDATE history SET end_date = :upload 
                WHERE ecn = :ecn AND field = :field AND end_date = '9999-12-31' AND start_date < :min_start
            """), {"upload": upload_date, "ecn": ecn, "field": field, "min_start": min_start})

        # 3. Insert history records
        hist_to_insert = [{k: v for k, v in h.items() if not k.startswith("_")} for h in history_records]
        for i in range(0, len(hist_to_insert), BATCH_SIZE):
            batch = hist_to_insert[i:i + BATCH_SIZE]
            conn.execute(text("""
                INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source)
                VALUES (:ecn, :employee_name, :field, :value, :prev_value, :start_date, :end_date, :source)
                ON DUPLICATE KEY UPDATE
                value = VALUES(value), prev_value = VALUES(prev_value), end_date = VALUES(end_date)
            """), batch)

        # 4. Update changed employees
        for i in range(0, len(updated_employees), BATCH_SIZE):
            batch = updated_employees[i:i + BATCH_SIZE]
            conn.execute(text("""
                UPDATE employees SET data = :data, updated_at = :updated_at, last_upload = :last_upload
                WHERE ecn = :ecn
            """), batch)

        # 5. Bulk update last_upload for unchanged employees
        if unchanged_ecns:
            for i in range(0, len(unchanged_ecns), BATCH_SIZE):
                batch = unchanged_ecns[i:i + BATCH_SIZE]
                placeholders = ",".join([f":ecn_{j}" for j in range(len(batch))])
                params = {f"ecn_{j}": ecn for j, ecn in enumerate(batch)}
                conn.execute(text(f"""
                    UPDATE employees SET last_upload = :upload 
                    WHERE ecn IN ({placeholders})
                """), {"upload": upload_date, **params})

        # 6. Log upload
        conn.execute(text("""
            INSERT INTO upload_log (upload_date, rows_processed, inserted, updated, skipped_manual)
            VALUES (:date, :rows, :ins, :upd, :skip)
        """), {
            "date": upload_date, "rows": total_rows,
            "ins": inserted, "upd": updated, "skip": skipped_manual
        })

        conn.commit()

    if progress_bar is not None:
        progress_bar.progress(1.0, text="Done!")

    return inserted, updated, None


def get_employees_at_date(query_date: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    df = get_all_employees_df(engine)
    if df.empty:
        return df

    if "DOJ Knack" in df.columns:
        df["__doj"] = df["DOJ Knack"].apply(parse_date)
        df = df[df["__doj"].isna() | (df["__doj"] <= query_date)]
        df = df.drop(columns=["__doj"])

    if "Date of Separation" in df.columns:
        df["__sep"] = df["Date of Separation"].apply(parse_date)
        df = df[df["__sep"].isna() | (df["__sep"] >= query_date)]
        df = df.drop(columns=["__sep"])

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT ecn, field, value FROM history 
            WHERE start_date <= :date AND end_date > :date
        """), {"date": query_date}).fetchall()

    if result:
        overrides = {}
        for ecn, field, value in result:
            key = (ecn, field)
            if key not in overrides:
                overrides[key] = value

        for (ecn, field), val in overrides.items():
            if field in df.columns:
                df.loc[df["ECN"] == ecn, field] = val

    internal = [c for c in df.columns if c.startswith("_")]
    return df.drop(columns=internal, errors="ignore")


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
            conn.execute(text("""
                UPDATE history SET end_date = :start 
                WHERE ecn = :ecn AND field = :field AND end_date = '9999-12-31' AND start_date <= :start
            """), {"start": start_date, "ecn": ecn, "field": field})

            conn.execute(text("""
                UPDATE history SET end_date = :start 
                WHERE ecn = :ecn AND field = :field AND start_date > :start AND start_date < :end
            """), {"start": start_date, "end": end_date, "ecn": ecn, "field": field})

            conn.execute(text("""
                INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source)
                VALUES (:ecn, :emp, :field, :val, :prev, :start, :end, 'manual_edit')
                ON DUPLICATE KEY UPDATE
                value = VALUES(value), prev_value = VALUES(prev_value), end_date = VALUES(end_date)
            """), {
                "ecn": ecn, "emp": existing.get("Employee", ""),
                "field": field, "val": new_value, "prev": old_value,
                "start": start_date, "end": end_date
            })

            if end_date == "9999-12-31":
                existing[field] = new_value
                existing["_updated_at"] = start_date
                conn.execute(text("""
                    UPDATE employees SET data = :data, updated_at = :updated WHERE ecn = :ecn
                """), {"ecn": ecn, "data": json.dumps(existing), "updated": start_date})

            conn.commit()
        return True, "Saved"
    except Exception as e:
        return False, f"Error: {str(e)[:100]}"


def get_employee_history(ecn: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    query = text("""
        SELECT ecn, employee_name, field, value, prev_value, start_date, end_date, source 
        FROM history 
        WHERE ecn = :ecn 
        ORDER BY field, start_date DESC
    """)
    df = pd.read_sql(query, engine, params={"ecn": ecn})
    if df.empty:
        return df
    df.columns = ["ECN", "Employee", "Field", "Value", "Previous", "Start", "End", "Source"]
    return df


def delete_history_record(record_id: int):
    engine = get_engine()
    if engine is None:
        return False, "Database not connected"

    try:
        with engine.connect() as conn:
            record = conn.execute(
                text("SELECT * FROM history WHERE id = :id"), {"id": record_id}
            ).mappings().fetchone()

            if not record:
                return False, "Record not found"

            ecn = record["ecn"]
            field = record["field"]
            start_date = record["start_date"]

            conn.execute(text("DELETE FROM history WHERE id = :id"), {"id": record_id})

            prev = conn.execute(text("""
                SELECT * FROM history 
                WHERE ecn = :ecn AND field = :field AND end_date = :start 
                ORDER BY start_date DESC LIMIT 1
            """), {"ecn": ecn, "field": field, "start": start_date}).mappings().fetchone()

            emp = get_employee(engine, ecn)

            if prev:
                conn.execute(text("""
                    UPDATE history SET end_date = '9999-12-31' 
                    WHERE ecn = :ecn AND field = :field AND start_date = :prev_start
                """), {"ecn": ecn, "field": field, "prev_start": prev["start_date"]})

                if emp:
                    emp[field] = prev["value"]
                    conn.execute(text("""
                        UPDATE employees SET data = :data WHERE ecn = :ecn
                    """), {"ecn": ecn, "data": json.dumps(emp)})
            else:
                if emp:
                    emp[field] = ""
                    conn.execute(text("""
                        UPDATE employees SET data = :data WHERE ecn = :ecn
                    """), {"ecn": ecn, "data": json.dumps(emp)})

            conn.commit()
            return True, "Deleted and restored previous value"
    except Exception as e:
        return False, f"Error: {str(e)[:100]}"


def update_history_record(record_id: int, new_value: str, new_start: str, new_end: str):
    engine = get_engine()
    if engine is None:
        return False, "Database not connected"
    try:
        with engine.connect() as conn:
            record = conn.execute(
                text("SELECT ecn, field, end_date FROM history WHERE id = :id"), {"id": record_id}
            ).mappings().fetchone()

            if not record:
                return False, "Record not found"

            conn.execute(text("""
                UPDATE history 
                SET value = :val, start_date = :start, end_date = :end 
                WHERE id = :id
            """), {"val": new_value, "start": new_start, "end": new_end, "id": record_id})

            if record["end_date"] == "9999-12-31" or new_end == "9999-12-31":
                emp = get_employee(engine, record["ecn"])
                if emp:
                    emp[record["field"]] = new_value
                    conn.execute(text("""
                        UPDATE employees SET data = :data WHERE ecn = :ecn
                    """), {"ecn": record["ecn"], "data": json.dumps(emp)})

            conn.commit()
            return True, "Updated"
    except Exception as e:
        return False, f"Error: {str(e)[:100]}"


def compact_history():
    engine = get_engine()
    if engine is None:
        return 0
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                DELETE FROM history WHERE value = prev_value AND value != ''
            """))
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
        active_count = conn.execute(text("""
            SELECT COUNT(*) FROM employees WHERE JSON_UNQUOTE(JSON_EXTRACT(data, '$."Active/Inactive"')) = 'Active'
        """)).scalar()
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
        st.error(f"❌ {err[:120]}") if err else st.warning("⚠️ No TiDB URI configured")

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

    if not db_connected():
        st.error("Please configure a valid TiDB URI in Streamlit Secrets first.")
        st.stop()

    st.subheader("📋 Template")
    st.download_button(
        label="⬇️ Download Excel Template",
        data=generate_template_bytes(),
        file_name="staffing_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Choose Excel file (.xlsx)", type=["xlsx"])
    with col2:
        st.markdown("""
        **Instructions**
        - **Required:** `ECN` (unique employee ID)
        - **Optional:** `Effective From`, `Effective To`
        - **Recommended:** `DOJ Knack`, `Date of Separation`
        - Any other columns accepted automatically
        - In-app edits are protected from Excel overwrites
        """)

    if uploaded:
        with st.spinner("Reading file..."):
            df = load_excel(uploaded)

        if df.empty:
            st.error("Could not read the Excel file.")
            st.stop()

        st.success(f"✅ **{len(df):,} rows**, **{len(df.columns)} columns**")
        st.caption(f"Columns: {', '.join(df.columns[:12])}{'...' if len(df.columns) > 12 else ''}")

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


# ─── PAGE: EMPLOYEE EDITOR ────────────────────────────────────────────────────
elif page == "👤 Employee Editor":
    st.title("👤 Employee Editor")
    engine = get_engine()
    if engine is None:
        st.error("Connect TiDB first.")
        st.stop()

    st.info("""
    **How manual edits work:**
    - Click any row to edit that employee
    - Set **Effective From** and optionally **Effective To**
    - Leave **Effective To** blank for ongoing changes
    - Future Excel uploads will skip manually-edited fields
    """)

    search = st.text_input("🔍 Search by name, ECN, or email", placeholder="e.g. Santos, 12345")
    filter_status = st.selectbox("Filter by Status", ["All", "Active", "Inactive", "LOA", "Maternity", "Suspended"])

    employees_df = get_all_employees_df(engine)
    if employees_df.empty:
        st.warning("No employees found.")
        st.stop()

    if "Active/Inactive" in employees_df.columns and filter_status != "All":
        employees_df = employees_df[employees_df["Active/Inactive"] == filter_status]

    if search:
        s = search.lower()
        mask = (
            employees_df.get("Employee", "").str.lower().str.contains(s, na=False) |
            employees_df.get("ECN", "").str.lower().str.contains(s, na=False) |
            employees_df.get("Email", "").str.lower().str.contains(s, na=False)
        )
        employees_df = employees_df[mask]

    if employees_df.empty:
        st.warning("No employees match your filters.")
        st.stop()

    display_cols = ["ECN", "Employee", "Client", "Sub-Process", "Role", "Billable/Buffer",
                    "Active/Inactive", "Location", "Overall Location"]
    display_cols = [c for c in display_cols if c in employees_df.columns]

    st.markdown(f"**{len(employees_df)} employees found**")
    st.caption("👆 Click any row to edit")

    selected = st.dataframe(
        employees_df[display_cols],
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
            c1, c2 = st.columns(2)
            with c1:
                eff_from = st.date_input("Effective From", value=date.today(), key=f"eff_from_{ecn}")
            with c2:
                eff_to = st.date_input("Effective To (blank = ongoing)", value=None, key=f"eff_to_{ecn}")

            eff_from_str = eff_from.isoformat()
            eff_to_str = eff_to.isoformat() if eff_to else "9999-12-31"

            if eff_to and eff_to <= eff_from:
                st.error("Effective To must be after Effective From")
                return

            core_fields = ["Billable/Buffer", "Active/Inactive", "Client", "Sub-Process",
                           "Supervisor", "Role", "Manager", "Location", "Overall Location"]
            core_fields = [f for f in core_fields if f in emp]

            edit_vals = {}
            cols = st.columns(3)
            for i, field in enumerate(core_fields):
                with cols[i % 3]:
                    edit_vals[field] = st.text_input(field, value=emp.get(field, ""), key=f"ec_{ecn}_{field}")

            if st.toggle("🔽 Show More Fields", key=f"sm_{ecn}"):
                other = [k for k in emp.keys() if not k.startswith("_") and k not in core_fields
                         and k not in ("Effective From", "Effective To")]
                cols2 = st.columns(3)
                for i, field in enumerate(other):
                    with cols2[i % 3]:
                        edit_vals[field] = st.text_input(field, value=emp.get(field, ""), key=f"eo_{ecn}_{field}")

            if st.button("💾 Review Changes", type="primary", key=f"rev_{ecn}"):
                changes = [f"{f}: {emp.get(f, '')} → {v}" for f, v in edit_vals.items() if v != emp.get(f, "")]
                if not changes:
                    st.info("No changes detected.")
                    return
                st.session_state[f"pending_changes_{ecn}"] = {
                    "changes": {f: v for f, v in edit_vals.items() if v != emp.get(f, "")},
                    "eff_from": eff_from_str, "eff_to": eff_to_str
                }
                st.rerun()

            if f"pending_changes_{ecn}" in st.session_state:
                pending = st.session_state[f"pending_changes_{ecn}"]
                st.divider()
                st.warning("**Confirm these changes?**")
                for f, v in pending["changes"].items():
                    st.markdown(f"- **{f}:** `{emp.get(f, '')}` → `{v}`")
                st.caption(f"Effective: {pending['eff_from']} to {pending['eff_to'] if pending['eff_to'] != '9999-12-31' else 'ongoing'}")

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("✅ Confirm Save", type="primary", key=f"cs_{ecn}"):
                        saved, errors = 0, []
                        for f, v in pending["changes"].items():
                            ok, msg = record_manual_edit(ecn, f, v, pending["eff_from"], pending["eff_to"])
                            if ok:
                                saved += 1
                            else:
                                errors.append(f"{f}: {msg}")
                        del st.session_state[f"pending_changes_{ecn}"]
                        for e in errors:
                            st.error(e)
                        if saved:
                            st.success(f"✅ {saved} field(s) updated!")
                            st.rerun()
                with c2:
                    if st.button("❌ Cancel", key=f"cx_{ecn}"):
                        del st.session_state[f"pending_changes_{ecn}"]
                        st.rerun()

        with tab2:
            hist = get_employee_history(ecn)
            if hist.empty:
                st.info("No history found.")
            else:
                st.dataframe(hist, use_container_width=True, hide_index=True)

    if selected and selected.selection.rows:
        idx = selected.selection.rows[0]
        emp = employees_df.iloc[idx].to_dict()
        edit_employee_modal(emp)


# ─── PAGE: DATE SNAPSHOT ──────────────────────────────────────────────────────
elif page == "📅 Date Snapshot":
    st.title("📅 Date Snapshot")
    st.markdown("View staffing data **as it was on any specific date**.")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    snap_date = st.date_input("Select snapshot date", value=date.today())
    snap_str = snap_date.isoformat()

    c1, c2, c3 = st.columns(3)
    with c1:
        filter_client = st.text_input("Filter by Client")
    with c2:
        filter_bb = st.selectbox("Billable/Buffer", ["All", "Billable", "Buffer", "Support", "Training", "Excluded"])
    with c3:
        filter_status = st.selectbox("Status", ["All", "Active", "Inactive", "LOA", "Maternity", "Suspended"])

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

            st.success(f"✅ **{snap_str}** — **{len(df):,} employees**")

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

            st.download_button(
                label=f"⬇️ Download Snapshot ({snap_str})",
                data=df_to_excel_bytes(df, sheet_name=f"Snapshot_{snap_str}"),
                file_name=f"staffing_snapshot_{snap_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# ─── PAGE: EXPORT ─────────────────────────────────────────────────────────────
elif page == "📊 Export Data":
    st.title("📊 Export Data")
    st.markdown("Export staffing data for any time range. Lightning fast — loads DB once.")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    exp_type = st.radio("Export Type", ["Daily", "Weekly", "Monthly", "Yearly", "Custom Range"], horizontal=True)
    today = date.today()

    if exp_type == "Daily":
        exp_date = st.date_input("Select date", value=today)
        dates = [exp_date.isoformat()]
        label = f"daily_{exp_date}"
    elif exp_type == "Weekly":
        week_start = today - timedelta(days=today.weekday())
        exp_week = st.date_input("Week starting (Monday)", value=week_start)
        dates = [(exp_week + timedelta(days=i)).isoformat() for i in range(7)]
        label = f"weekly_{dates[0]}_to_{dates[-1]}"
    elif exp_type == "Monthly":
        c1, c2 = st.columns(2)
        with c1:
            month = st.selectbox("Month", list(range(1, 13)), index=today.month - 1,
                                 format_func=lambda m: datetime(2000, m, 1).strftime("%B"))
        with c2:
            year = st.number_input("Year", min_value=2020, max_value=2035, value=today.year)
        last_day = calendar.monthrange(year, month)[1]
        dates = [date(year, month, d).isoformat() for d in range(1, last_day + 1)]
        label = f"monthly_{year}_{month:02d}"
    elif exp_type == "Yearly":
        year = st.number_input("Year", min_value=2020, max_value=2035, value=today.year)
        dates = [date(year, m, d).isoformat() for m in range(1, 13) for d in range(1, calendar.monthrange(year, m)[1] + 1)]
        label = f"yearly_{year}"
    else:
        c1, c2 = st.columns(2)
        with c1:
            start = st.date_input("Start date", value=today - timedelta(days=7))
        with c2:
            end = st.date_input("End date", value=today)
        start_dt, end_dt = start, end
        dates = [(start_dt + timedelta(days=i)).isoformat() for i in range((end_dt - start_dt).days + 1)]
        label = f"custom_{dates[0]}_to_{dates[-1]}"

    if len(dates) > 366:
        st.warning("⚠️ Export limited to 366 days. Please narrow your range.")
        st.stop()

    st.markdown(f"**Range:** `{dates[0]}` → `{dates[-1]}`  (**{len(dates)} days**)")

    export_mode = st.radio("Export Mode", [
        "Single sheet (all dates appended with Date Exported column)",
        "One sheet per day",
    ])
    filter_active = st.checkbox("Active employees only", value=False)
    filter_client2 = st.text_input("Filter by Client (optional)", key="exp_client")

    if st.button("📥 Generate Export", type="primary"):
        status = st.empty()
        status.info("⏳ Loading base data from TiDB (one time)...")

        # ─── LOAD EVERYTHING ONCE ─────────────────────────────────────────
        engine = get_engine()
        base_df = get_all_employees_df(engine)
        if base_df.empty:
            st.warning("No employee data found.")
            st.stop()

        # Pre-compute DOJ/Sep dates for filtering
        if "DOJ Knack" in base_df.columns:
            base_df["__doj_dt"] = pd.to_datetime(base_df["DOJ Knack"].apply(parse_date), errors="coerce")
        else:
            base_df["__doj_dt"] = pd.NaT

        if "Date of Separation" in base_df.columns:
            base_df["__sep_dt"] = pd.to_datetime(base_df["Date of Separation"].apply(parse_date), errors="coerce")
        else:
            base_df["__sep_dt"] = pd.NaT

        # Load ALL history into memory once
        with engine.connect() as conn:
            hist_all = pd.read_sql(text("""
                SELECT ecn, field, value, start_date, end_date 
                FROM history
            """), conn)

        if not hist_all.empty:
            hist_all["start_dt"] = pd.to_datetime(hist_all["start_date"], errors="coerce")
            hist_all["end_dt"] = pd.to_datetime(hist_all["end_date"], errors="coerce")

        status.info(f"⏳ Processing {len(dates)} days in-memory...")

        all_dfs = []
        date_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]

        for i, d_str in enumerate(dates):
            d = date_objs[i]
            d_ts = pd.Timestamp(d)

            # Vectorized filter: keep if DOJ <= date AND (Sep is null OR Sep >= date)
            mask = pd.Series(True, index=base_df.index)
            if "__doj_dt" in base_df.columns:
                mask &= base_df["__doj_dt"].isna() | (base_df["__doj_dt"] <= d_ts)
            if "__sep_dt" in base_df.columns:
                mask &= base_df["__sep_dt"].isna() | (base_df["__sep_dt"] >= d_ts)

            df_day = base_df[mask].copy()

            # Apply history overrides for this date (vectorized)
            if not hist_all.empty:
                day_hist = hist_all[(hist_all["start_dt"] <= d_ts) & (hist_all["end_dt"] > d_ts)]
                if not day_hist.empty:
                    # Keep latest start date per ecn+field
                    day_hist = day_hist.sort_values("start_dt", ascending=False).drop_duplicates(subset=["ecn", "field"])
                    for _, hrow in day_hist.iterrows():
                        if hrow["field"] in df_day.columns:
                            df_day.loc[df_day["ECN"] == hrow["ecn"], hrow["field"]] = hrow["value"]

            # Drop internal cols
            df_day = df_day.drop(columns=[c for c in df_day.columns if c.startswith("__")], errors="ignore")
            df_day = df_day.drop(columns=[c for c in df_day.columns if c.startswith("_")], errors="ignore")

            # Apply filters
            if filter_active and "Active/Inactive" in df_day.columns:
                df_day = df_day[df_day["Active/Inactive"] == "Active"]
            if filter_client2 and "Client" in df_day.columns:
                df_day = df_day[df_day["Client"].str.contains(filter_client2, case=False, na=False)]

            if not df_day.empty:
                df_day["Date Exported"] = d_str
                all_dfs.append(df_day)

            if i % 30 == 0 or i == len(dates) - 1:
                status.info(f"⏳ Processed {i + 1}/{len(dates)} days...")

        if not all_dfs:
            st.warning("No data found for the selected range.")
            st.stop()

        if export_mode.startswith("Single"):
            combined = pd.concat(all_dfs, ignore_index=True)
            cols = ["Date Exported"] + [c for c in combined.columns if c != "Date Exported"]
            combined = combined[cols]

            status.success(f"✅ {len(combined):,} rows across {len(all_dfs)} days")
            st.dataframe(combined.head(20), use_container_width=True)

            st.download_button(
                f"⬇️ Download {exp_type} Export",
                data=df_to_excel_bytes(combined, sheet_name="Staffing Export"),
                file_name=f"staffing_{label}_single_sheet.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            if len(dates) > 31:
                st.warning("One sheet per day mode is limited to 31 days.")
                st.stop()

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                wb = writer.book
                header_fmt = wb.add_format({"bold": True, "bg_color": "#1f4e79", "font_color": "white"})

                for i, df_day in enumerate(all_dfs):
                    d_str = dates[i]
                    sheet = d_str.replace("-", "")[-6:]
                    df_day.to_excel(writer, index=False, sheet_name=sheet)
                    ws = writer.sheets[sheet]
                    for col_num, col_name in enumerate(df_day.columns):
                        ws.write(0, col_num, col_name, header_fmt)

            status.success(f"✅ {len(all_dfs)} daily sheets generated!")
            st.download_button(
                "⬇️ Download Daily Export",
                data=buf.getvalue(),
                file_name=f"staffing_{label}_daily.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# ─── PAGE: HISTORY MANAGER ───────────────────────────────────────────────────
elif page == "📜 History Manager":
    st.title("📜 History Manager")
    st.markdown("One row per employee. Click to view, edit, or delete their history records.")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    engine = get_engine()
    search = st.text_input("🔍 Search by ECN or Employee name", placeholder="e.g. EMP001 or John Doe")

    query = text("""
        SELECT 
            ecn,
            MAX(employee_name) as employee_name,
            COUNT(*) as record_count,
            MAX(start_date) as last_updated,
            GROUP_CONCAT(DISTINCT field ORDER BY field SEPARATOR ', ') as fields_changed
        FROM history
        WHERE (:search = '' OR ecn LIKE :search OR employee_name LIKE :search)
        GROUP BY ecn
        ORDER BY last_updated DESC
        LIMIT 500
    """)

    with engine.connect() as conn:
        agg_results = conn.execute(query, {"search": f"%{search}%" if search else ""}).fetchall()

    if not agg_results:
        st.info("No history records found.")
        st.stop()

    agg_df = pd.DataFrame(agg_results, columns=["ECN", "Employee", "Records", "Last Updated", "Fields Changed"])
    st.markdown(f"**{len(agg_df)} employees with history**")

    selected = st.dataframe(
        agg_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    if selected and selected.selection.rows:
        idx = selected.selection.rows[0]
        ecn = agg_df.iloc[idx]["ECN"]
        emp_name = agg_df.iloc[idx]["Employee"]

        st.divider()
        st.subheader(f"📋 History for {emp_name or ecn} ({ecn})")

        hist_df = get_employee_history(ecn)
        if hist_df.empty:
            st.info("No detailed history found.")
            st.stop()

        for _, row in hist_df.iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1, 1])
                with c1:
                    st.markdown(f"**{row['Field']}**  \n`{row['Previous'] or '(blank)'}` → `{row['Value']}`")
                    st.caption(f"📅 {row['Start']} → {row['End']}  |  🏷️ {row['Source']}")
                with c2:
                    if st.button("✏️ Edit", key=f"edit_{row['ECN']}_{row['Field']}_{row['Start']}"):
                        st.session_state[f"edit_mode_{row['ECN']}_{row['Field']}_{row['Start']}"] = True
                        st.rerun()
                with c3:
                    if st.button("🗑️ Delete", key=f"del_{row['ECN']}_{row['Field']}_{row['Start']}"):
                        st.session_state[f"del_mode_{row['ECN']}_{row['Field']}_{row['Start']}"] = True
                        st.rerun()

                edit_key = f"edit_mode_{row['ECN']}_{row['Field']}_{row['Start']}"
                if st.session_state.get(edit_key):
                    with st.form(key=f"form_{edit_key}"):
                        st.markdown("**Edit Record**")
                        nc1, nc2, nc3 = st.columns(3)
                        with nc1:
                            new_val = st.text_input("Value", value=row["Value"], key=f"v_{edit_key}")
                        with nc2:
                            new_start = st.text_input("Start Date", value=row["Start"], key=f"s_{edit_key}")
                        with nc3:
                            new_end = st.text_input("End Date", value=row["End"], key=f"e_{edit_key}")

                        with engine.connect() as conn:
                            rec = conn.execute(text("""
                                SELECT id FROM history 
                                WHERE ecn = :ecn AND field = :field AND start_date = :start
                            """), {"ecn": row["ECN"], "field": row["Field"], "start": row["Start"]}).fetchone()

                        c1, c2 = st.columns(2)
                        with c1:
                            if st.form_submit_button("💾 Save", type="primary"):
                                if rec:
                                    ok, msg = update_history_record(rec[0], new_val, new_start, new_end)
                                    if ok:
                                        st.success(msg)
                                        del st.session_state[edit_key]
                                        st.rerun()
                                    else:
                                        st.error(msg)
                        with c2:
                            if st.form_submit_button("Cancel"):
                                del st.session_state[edit_key]
                                st.rerun()

                del_key = f"del_mode_{row['ECN']}_{row['Field']}_{row['Start']}"
                if st.session_state.get(del_key):
                    st.warning("Are you sure you want to delete this record?")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("✅ Yes, Delete", type="primary", key=f"yd_{del_key}"):
                            with engine.connect() as conn:
                                rec = conn.execute(text("""
                                    SELECT id FROM history 
                                    WHERE ecn = :ecn AND field = :field AND start_date = :start
                                """), {"ecn": row["ECN"], "field": row["Field"], "start": row["Start"]}).fetchone()
                            if rec:
                                ok, msg = delete_history_record(rec[0])
                                if ok:
                                    st.success(msg)
                                    del st.session_state[del_key]
                                    st.rerun()
                                else:
                                    st.error(msg)
                    with c2:
                        if st.button("Cancel", key=f"cd_{del_key}"):
                            del st.session_state[del_key]
                            st.rerun()

        st.divider()
        if st.button("🧹 Remove Redundant Records (value == prev_value)"):
            with st.spinner("Cleaning..."):
                deleted = compact_history()
            st.success(f"Removed **{deleted}** redundant records")


# ─── PAGE: DB TOOLS ───────────────────────────────────────────────────────────
elif page == "🛠️ DB Tools":
    st.title("🛠️ Database Tools")

    if not db_connected():
        st.error("Connect TiDB first.")
        st.stop()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Storage")
        try:
            emp_count, active_count, hist_count = get_db_stats()
            st.metric("Total Employees", f"{emp_count:,}")
            st.metric("Active", f"{active_count:,}")
            st.metric("History Records", f"{hist_count:,}")
        except Exception as e:
            st.error(f"Could not fetch stats: {e}")

    with c2:
        st.subheader("Cleanup")
        st.markdown("Remove history entries where `value == prev_value` (no actual change).")
        if st.button("🧹 Compact History", type="primary"):
            with st.spinner("Compacting..."):
                deleted = compact_history()
            st.success(f"Removed **{deleted}** redundant entries.")

    st.divider()
    st.subheader("Recent Uploads")
    engine = get_engine()
    if engine:
        with engine.connect() as conn:
            logs = conn.execute(text("""
                SELECT * FROM upload_log ORDER BY upload_date DESC LIMIT 10
            """)).fetchall()
        if logs:
            logs_df = pd.DataFrame(logs, columns=["id", "upload_date", "rows_processed", "inserted", "updated", "skipped_manual"])
            st.dataframe(logs_df, use_container_width=True, hide_index=True)
        else:
            st.info("No uploads logged yet.")
