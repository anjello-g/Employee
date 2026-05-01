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

warnings.filterwarnings('ignore')

try:
    import pymysql
    from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Integer, JSON
    TIDB_AVAILABLE = True
except ImportError:
    TIDB_AVAILABLE = False

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title='Knack RCM | Employee Dashboard',
    page_icon='https://kimi-web-img.moonshot.cn/img/knackrcm.com/26ef9a05ac06e2c7d058a5cafefa681569032b17.png',
    layout='wide',
    initial_sidebar_state='expanded'
)

# ─── BRAND PALETTE ──────────────────────────────────────────────────────────
BRAND_DARK_BLUE  = '#2b3c78'
BRAND_LIGHT_BLUE = '#5fb7de'
BRAND_MED_BLUE   = '#0968b1'
BRAND_MAROON     = '#751026'
BRAND_ORANGE     = '#f47e20'
BRAND_LIGHT_GRAY = '#d5d9db'
BRAND_OFF_WHITE  = '#ecf3f4'

# ─── COLUMNS ────────────────────────────────────────────────────────────────
CORE_COLS = [
    'Date Exported','ECN','Employee','Client','Sub-Process','Supervisor',
    'Manager','Role','Process Owner','Billable/Buffer','DOJ Knack',
    'Date of Separation','Active/Inactive','Email','NT Login','Structure',
    'Department','Location','Gender','Global ID (GPP)','Attrition Type',
    'Reason for Attrition','CDP Email','Overall Location','Aging Bucket'
]
DISPLAY_ORDER = CORE_COLS[:]

# ─── THEME CSS ──────────────────────────────────────────────────────────────
def inject_theme():
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {{
        --bg-base:      #080e1c;
        --bg-surface:   #0f1829;
        --bg-card:      #141f35;
        --bg-elevated:  #1a2844;
        --border:       rgba(95,183,222,0.12);
        --border-hover: rgba(95,183,222,0.35);
        --brand-blue:   #2b3c78;
        --brand-light:  #5fb7de;
        --brand-med:    #0968b1;
        --brand-orange: #f47e20;
        --brand-maroon: #751026;
        --text-primary: #e8f4f8;
        --text-muted:   #7a9bb5;
        --text-faint:   #3d5a72;
        --accent-glow:  rgba(95,183,222,0.15);
        --orange-glow:  rgba(244,126,32,0.18);
        --radius-sm:    6px;
        --radius-md:    10px;
        --radius-lg:    16px;
        --shadow-card:  0 4px 24px rgba(0,0,0,0.35);
        --shadow-glow:  0 0 32px rgba(95,183,222,0.08);
    }}

    html, body, [class*="css"] {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        color: var(--text-primary) !important;
    }}

    /* ── BASE ── */
    .stApp {{
        background: var(--bg-base) !important;
        background-image:
            radial-gradient(ellipse 80% 50% at 50% -20%, rgba(43,60,120,0.25) 0%, transparent 70%),
            radial-gradient(ellipse 40% 30% at 90% 10%, rgba(9,104,177,0.12) 0%, transparent 60%) !important;
    }}

    /* ── SIDEBAR ── */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #0d1628 0%, #0a1220 100%) !important;
        border-right: 1px solid var(--border) !important;
        box-shadow: 4px 0 32px rgba(0,0,0,0.4) !important;
    }}
    section[data-testid="stSidebar"] > div {{
        padding-top: 1.5rem !important;
    }}

    /* Sidebar nav buttons */
    section[data-testid="stSidebar"] .stButton > button {{
        width: 100% !important;
        border-radius: var(--radius-sm) !important;
        border: 1px solid transparent !important;
        background: transparent !important;
        color: var(--text-muted) !important;
        text-align: left !important;
        padding: 0.6rem 1rem !important;
        margin-bottom: 2px !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.01em !important;
        transition: all 0.18s ease !important;
        box-shadow: none !important;
    }}
    section[data-testid="stSidebar"] .stButton > button:hover {{
        background: var(--accent-glow) !important;
        border-color: var(--border-hover) !important;
        color: var(--brand-light) !important;
        transform: translateX(3px) !important;
    }}
    section[data-testid="stSidebar"] .stButton > button[data-testid="baseButton-primary"] {{
        background: linear-gradient(135deg, rgba(43,60,120,0.8) 0%, rgba(9,104,177,0.6) 100%) !important;
        border-color: var(--brand-light) !important;
        color: var(--brand-light) !important;
        font-weight: 600 !important;
        box-shadow: 0 0 16px rgba(95,183,222,0.15), inset 0 1px 0 rgba(255,255,255,0.06) !important;
    }}

    /* ── TYPOGRAPHY ── */
    h1 {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 800 !important;
        font-size: 1.75rem !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.03em !important;
        padding-bottom: 0.75rem !important;
        margin-bottom: 1.5rem !important;
        border-bottom: 1px solid var(--border) !important;
        background: linear-gradient(135deg, var(--text-primary) 0%, var(--brand-light) 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
    }}
    h2, h3 {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 700 !important;
        color: var(--brand-light) !important;
        letter-spacing: -0.02em !important;
    }}
    h4, h5, h6 {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
    }}
    p, li, label, .stMarkdown {{
        color: var(--text-primary) !important;
    }}

    /* ── MAIN ACTION BUTTONS ── */
    .stButton > button[data-testid="baseButton-primary"] {{
        background: linear-gradient(135deg, var(--brand-orange) 0%, #d96a12 100%) !important;
        color: #fff !important;
        border: none !important;
        border-radius: var(--radius-sm) !important;
        font-weight: 700 !important;
        font-size: 0.88rem !important;
        letter-spacing: 0.02em !important;
        padding: 0.55rem 1.4rem !important;
        box-shadow: 0 2px 16px var(--orange-glow), inset 0 1px 0 rgba(255,255,255,0.12) !important;
        transition: all 0.18s ease !important;
    }}
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 24px rgba(244,126,32,0.4), inset 0 1px 0 rgba(255,255,255,0.15) !important;
    }}
    .stButton > button[data-testid="baseButton-secondary"] {{
        background: var(--bg-elevated) !important;
        color: var(--brand-light) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: all 0.18s ease !important;
    }}
    .stButton > button[data-testid="baseButton-secondary"]:hover {{
        border-color: var(--brand-light) !important;
        background: var(--accent-glow) !important;
    }}
    .stButton > button {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}

    /* ── INPUTS ── */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {{
        background: var(--bg-elevated) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
        transition: border-color 0.15s, box-shadow 0.15s !important;
    }}
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stDateInput > div > div > input:focus {{
        border-color: var(--brand-light) !important;
        box-shadow: 0 0 0 3px rgba(95,183,222,0.12) !important;
        outline: none !important;
    }}
    .stSelectbox > div > div {{
        background: var(--bg-elevated) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
        color: var(--text-primary) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}
    .stSelectbox > div > div:focus-within {{
        border-color: var(--brand-light) !important;
        box-shadow: 0 0 0 3px rgba(95,183,222,0.12) !important;
    }}
    /* Selectbox dropdown options */
    [data-baseweb="select"] [data-baseweb="menu"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
    }}

    /* ── METRICS ── */
    [data-testid="stMetric"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
        padding: 1.1rem 1.25rem !important;
        box-shadow: var(--shadow-card) !important;
        position: relative !important;
        overflow: hidden !important;
        transition: border-color 0.2s, box-shadow 0.2s !important;
    }}
    [data-testid="stMetric"]:hover {{
        border-color: var(--border-hover) !important;
        box-shadow: var(--shadow-card), var(--shadow-glow) !important;
    }}
    [data-testid="stMetric"]::before {{
        content: '' !important;
        position: absolute !important;
        top: 0 !important; left: 0 !important; right: 0 !important;
        height: 2px !important;
        background: linear-gradient(90deg, var(--brand-blue), var(--brand-light)) !important;
    }}
    [data-testid="stMetricLabel"] {{
        color: var(--text-muted) !important;
        font-size: 0.78rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.06em !important;
        text-transform: uppercase !important;
    }}
    [data-testid="stMetricValue"] {{
        color: var(--text-primary) !important;
        font-weight: 800 !important;
        font-size: 1.9rem !important;
        letter-spacing: -0.03em !important;
        line-height: 1.1 !important;
    }}
    [data-testid="stMetricDelta"] {{
        color: var(--brand-orange) !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
    }}

    /* ── DATAFRAMES ── */
    .stDataFrame {{
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
        overflow: hidden !important;
        box-shadow: var(--shadow-card) !important;
    }}
    .stDataFrame [data-testid="stDataFrameResizable"] {{
        background: var(--bg-card) !important;
    }}
    .stDataFrame th {{
        background: var(--bg-elevated) !important;
        color: var(--text-muted) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.05em !important;
        text-transform: uppercase !important;
        border-bottom: 1px solid var(--border) !important;
    }}
    .stDataFrame td {{
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
        font-size: 0.85rem !important;
        border-bottom: 1px solid rgba(95,183,222,0.06) !important;
    }}
    .stDataFrame tr:hover td {{
        background: var(--bg-elevated) !important;
    }}

    /* ── EXPANDERS ── */
    .streamlit-expanderHeader {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
        color: var(--text-primary) !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: border-color 0.15s !important;
    }}
    .streamlit-expanderHeader:hover {{
        border-color: var(--border-hover) !important;
    }}
    .streamlit-expanderContent {{
        background: var(--bg-surface) !important;
        border: 1px solid var(--border) !important;
        border-top: none !important;
        border-radius: 0 0 var(--radius-sm) var(--radius-sm) !important;
    }}

    /* ── ALERTS ── */
    .stSuccess > div, [data-testid="stNotificationContentSuccess"] {{
        background: rgba(34,197,94,0.08) !important;
        border: 1px solid rgba(34,197,94,0.25) !important;
        border-radius: var(--radius-sm) !important;
        color: #86efac !important;
    }}
    .stInfo > div, [data-testid="stNotificationContentInfo"] {{
        background: rgba(95,183,222,0.08) !important;
        border: 1px solid rgba(95,183,222,0.25) !important;
        border-radius: var(--radius-sm) !important;
        color: var(--brand-light) !important;
    }}
    .stWarning > div, [data-testid="stNotificationContentWarning"] {{
        background: rgba(244,126,32,0.08) !important;
        border: 1px solid rgba(244,126,32,0.25) !important;
        border-radius: var(--radius-sm) !important;
        color: #fdba74 !important;
    }}
    .stError > div, [data-testid="stNotificationContentError"] {{
        background: rgba(239,68,68,0.08) !important;
        border: 1px solid rgba(239,68,68,0.25) !important;
        border-radius: var(--radius-sm) !important;
        color: #fca5a5 !important;
    }}

    /* ── FILE UPLOADER ── */
    .stFileUploader > div > div {{
        background: var(--bg-card) !important;
        border: 2px dashed var(--border) !important;
        border-radius: var(--radius-md) !important;
        transition: border-color 0.2s !important;
    }}
    .stFileUploader > div > div:hover {{
        border-color: var(--brand-light) !important;
        background: var(--accent-glow) !important;
    }}

    /* ── PROGRESS ── */
    .stProgress > div > div {{
        background: var(--bg-elevated) !important;
        border-radius: 100px !important;
    }}
    .stProgress > div > div > div {{
        background: linear-gradient(90deg, var(--brand-blue), var(--brand-light)) !important;
        border-radius: 100px !important;
    }}

    /* ── RADIO ── */
    .stRadio > div > div > label {{
        color: var(--text-primary) !important;
        font-size: 0.88rem !important;
    }}
    .stRadio [data-testid="stWidgetLabel"] {{
        color: var(--text-muted) !important;
        font-weight: 600 !important;
    }}

    /* ── CHECKBOX ── */
    .stCheckbox > label {{
        color: var(--text-primary) !important;
        font-size: 0.88rem !important;
    }}

    /* ── TOGGLE ── */
    .stToggle > label {{
        color: var(--text-primary) !important;
    }}

    /* ── DIVIDER ── */
    hr {{
        border-color: var(--border) !important;
        margin: 1.5rem 0 !important;
    }}

    /* ── DIALOGS ── */
    [data-testid="stDialog"] > div > div {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-lg) !important;
        box-shadow: 0 24px 64px rgba(0,0,0,0.6) !important;
    }}

    /* ── CONTAINER BORDERS ── */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
        box-shadow: var(--shadow-card) !important;
    }}

    /* ── SCROLLBAR ── */
    ::-webkit-scrollbar {{ width: 5px; height: 5px; }}
    ::-webkit-scrollbar-track {{ background: var(--bg-base); }}
    ::-webkit-scrollbar-thumb {{ background: var(--bg-elevated); border-radius: 100px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: var(--brand-blue); }}

    /* ── SPINNER ── */
    .stSpinner > div {{
        border-top-color: var(--brand-light) !important;
    }}

    /* ── CAPTION / SMALL TEXT ── */
    .stCaption, small, [data-testid="stCaptionContainer"] {{
        color: var(--text-muted) !important;
        font-size: 0.78rem !important;
    }}

    /* ── WIDGET LABELS ── */
    [data-testid="stWidgetLabel"] p {{
        color: var(--text-muted) !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.03em !important;
        text-transform: uppercase !important;
    }}

    /* ── DOWNLOAD BUTTON ── */
    .stDownloadButton > button {{
        background: var(--bg-elevated) !important;
        color: var(--brand-light) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        transition: all 0.18s ease !important;
    }}
    .stDownloadButton > button:hover {{
        border-color: var(--brand-light) !important;
        background: var(--accent-glow) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 16px rgba(95,183,222,0.15) !important;
    }}

    /* ── TABS (if used) ── */
    .stTabs [data-baseweb="tab-list"] {{
        background: var(--bg-card) !important;
        border-radius: var(--radius-sm) !important;
        border: 1px solid var(--border) !important;
        gap: 2px !important;
        padding: 3px !important;
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 4px !important;
        color: var(--text-muted) !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }}
    .stTabs [aria-selected="true"] {{
        background: var(--brand-blue) !important;
        color: var(--brand-light) !important;
    }}

    /* ── POPOVER ── */
    [data-testid="stPopover"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
    }}

    /* ── TOAST ── */
    [data-testid="toastContainer"] {{
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}
    </style>
    """, unsafe_allow_html=True)

inject_theme()

# ─── SIDEBAR BRAND HEADER ────────────────────────────────────────────────────
def render_sidebar_brand():
    st.markdown(f"""
    <div style="
        display:flex; align-items:center; gap:12px;
        padding: 0.5rem 0.75rem 1rem 0.75rem;
        border-bottom: 1px solid rgba(95,183,222,0.12);
        margin-bottom: 1rem;
    ">
        <img src="https://kimi-web-img.moonshot.cn/img/knackrcm.com/26ef9a05ac06e2c7d058a5cafefa681569032b17.png"
             style="width:40px; height:40px; object-fit:contain; border-radius:8px;" />
        <div>
            <div style="
                font-family:'Plus Jakarta Sans',sans-serif;
                font-size:1.05rem; font-weight:800;
                color:#e8f4f8; letter-spacing:-0.02em; line-height:1.1;
            ">Knack RCM</div>
            <div style="
                font-size:0.7rem; font-weight:600; letter-spacing:0.08em;
                color:#5fb7de; text-transform:uppercase; margin-top:1px;
            ">Employee Dashboard</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ─── STAT CARD HTML ──────────────────────────────────────────────────────────
def stat_card(label, value, icon='', color='var(--brand-light)', delta=None):
    delta_html = f'<div style="font-size:0.75rem;color:var(--brand-orange);font-weight:600;margin-top:4px;">{delta}</div>' if delta else ''
    return f"""
    <div style="
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius-md, 10px);
        padding: 1.1rem 1.25rem;
        position: relative; overflow: hidden;
        transition: border-color 0.2s, box-shadow 0.2s;
        box-shadow: 0 4px 24px rgba(0,0,0,0.3);
    ">
        <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,{color},transparent);"></div>
        <div style="font-size:0.72rem;font-weight:700;letter-spacing:0.07em;text-transform:uppercase;color:#7a9bb5;margin-bottom:6px;">{icon} {label}</div>
        <div style="font-size:1.85rem;font-weight:800;color:#e8f4f8;letter-spacing:-0.03em;line-height:1;">{value}</div>
        {delta_html}
    </div>
    """

# ─── PAGE HEADER HTML ────────────────────────────────────────────────────────
def page_header(title, subtitle=''):
    sub = f'<div style="font-size:0.85rem;color:#7a9bb5;font-weight:500;margin-top:4px;">{subtitle}</div>' if subtitle else ''
    st.markdown(f"""
    <div style="margin-bottom:1.75rem; padding-bottom:1rem; border-bottom:1px solid rgba(95,183,222,0.12);">
        <h1 style="
            font-family:'Plus Jakarta Sans',sans-serif;
            font-size:1.6rem; font-weight:800; margin:0; padding:0; border:none !important;
            background: linear-gradient(135deg, #e8f4f8 0%, #5fb7de 100%);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text; letter-spacing:-0.03em; line-height:1.1;
        ">{title}</h1>
        {sub}
    </div>
    """, unsafe_allow_html=True)

# ─── SECTION LABEL ───────────────────────────────────────────────────────────
def section_label(text):
    st.markdown(f"""
    <div style="
        font-size:0.72rem; font-weight:700; letter-spacing:0.08em;
        text-transform:uppercase; color:#5fb7de;
        margin: 1.25rem 0 0.6rem 0; display:flex; align-items:center; gap:8px;
    "><span style="flex:1;height:1px;background:rgba(95,183,222,0.15);"></span>
    <span>{text}</span>
    <span style="flex:1;height:1px;background:rgba(95,183,222,0.15);"></span></div>
    """, unsafe_allow_html=True)

# ─── DATABASE ────────────────────────────────────────────────────────────────
def parse_and_escape_uri(uri: str) -> str:
    if not uri or not uri.startswith('mysql'):
        return uri
    try:
        parsed = urlparse(uri)
        if parsed.username and parsed.password:
            user = quote_plus(parsed.username)
            pwd  = quote_plus(parsed.password)
            netloc = f'{user}:{pwd}@{parsed.hostname}'
            if parsed.port:
                netloc += f':{parsed.port}'
            return urlunparse((parsed.scheme, netloc, parsed.path,
                               parsed.params, parsed.query, parsed.fragment))
    except Exception:
        pass
    return uri

@st.cache_resource(show_spinner=False)
def get_db():
    uri = (st.secrets.get('TIDB_URI', '') or os.environ.get('TIDB_URI', '')).strip()
    if not uri:
        return None, None
    if not TIDB_AVAILABLE:
        st.session_state['_db_err'] = 'Missing drivers: add pymysql and sqlalchemy to requirements.txt'
        return None, None
    uri = parse_and_escape_uri(uri)
    try:
        engine = create_engine(
            uri,
            connect_args={'ssl': {'ca': certifi.where()}, 'connect_timeout': 15},
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        metadata = MetaData()
        Table('employees', metadata,
              Column('ecn', String(50), primary_key=True),
              Column('data', JSON),
              Column('created_at', String(10)),
              Column('updated_at', String(10)),
              Column('last_upload', String(10)),
              mysql_engine='InnoDB')
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
              mysql_engine='InnoDB')
        Table('upload_log', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('upload_date', String(10), index=True),
              Column('rows_processed', Integer),
              Column('inserted', Integer),
              Column('updated', Integer),
              Column('skipped_manual', Integer),
              mysql_engine='InnoDB')
        metadata.create_all(engine)
        st.session_state.pop('_db_err', None)
        return engine, metadata
    except Exception as e:
        st.session_state['_db_err'] = str(e)
        return None, None

def get_engine():
    engine, _ = get_db()
    return engine

def db_connected():
    return get_engine() is not None

# ─── CUSTOM COLUMNS ──────────────────────────────────────────────────────────
def get_custom_columns():
    engine = get_engine()
    if engine is None:
        return st.session_state.get('_custom_cols', [])
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT ecn, data FROM employees WHERE ecn LIKE '_COL_%'")
            ).fetchall()
        cols = []
        for row in rows:
            data = json.loads(row[1]) if isinstance(row[1], str) else (row[1] or {})
            if data.get('approved') and data.get('col_name'):
                cols.append(data['col_name'])
        return cols
    except Exception:
        return st.session_state.get('_custom_cols', [])

def add_custom_column(col_name: str):
    engine = get_engine()
    if engine:
        try:
            with engine.connect() as conn:
                conn.execute(text(
                    'INSERT IGNORE INTO employees (ecn, data, created_at, updated_at, last_upload) '
                    'VALUES (:ecn, :data, :d, :d, :d)'
                ), {'ecn': '_COL_' + col_name,
                    'data': json.dumps({'col_name': col_name, 'approved': True}),
                    'd': '2000-01-01'})
                conn.commit()
        except Exception:
            pass
    custom = st.session_state.get('_custom_cols', [])
    if col_name not in custom:
        custom.append(col_name)
        st.session_state['_custom_cols'] = custom
    st.cache_data.clear()

def remove_custom_column(col_name: str):
    engine = get_engine()
    if engine:
        try:
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM employees WHERE ecn = :ecn"),
                             {'ecn': '_COL_' + col_name})
                conn.commit()
        except Exception:
            pass
    custom = st.session_state.get('_custom_cols', [])
    if col_name in custom:
        custom.remove(col_name)
        st.session_state['_custom_cols'] = custom
    st.cache_data.clear()

def get_all_accepted_columns():
    return list(dict.fromkeys(CORE_COLS + get_custom_columns()))

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_str(v):
    if v is None:
        return ''
    if isinstance(v, float) and np.isnan(v):
        return ''
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime('%m/%d/%Y')
    return str(v).strip()

def parse_date(v):
    if not v or str(v).strip() in ('', 'nan', 'None', 'NaT'):
        return None
    try:
        if isinstance(v, (datetime, pd.Timestamp)):
            return v.strftime('%m/%d/%Y')
        s = str(v).strip()
        # Strip time component if present (e.g. "2026-03-15 00:00:00" or "2026-03-15T00:00:00")
        if ' ' in s:
            s = s.split(' ')[0]
        if 'T' in s:
            s = s.split('T')[0]
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(s, fmt).strftime('%m/%d/%Y')
            except ValueError:
                continue
        try:
            return pd.to_datetime(float(s), unit='D', origin='1899-12-30').strftime('%m/%d/%Y')
        except (ValueError, OverflowError):
            pass
    except Exception:
        pass
    return None

def row_to_doc(row: dict) -> dict:
    accepted = set(get_all_accepted_columns())
    return {
        k.replace('.', '_'): safe_str(v)
        for k, v in row.items()
        if k.replace('.', '_') in accepted or k.startswith('_')
    }

def filter_accepted_columns(df: pd.DataFrame) -> pd.DataFrame:
    accepted = set(get_all_accepted_columns())
    keep = [c for c in df.columns if c in accepted or c.startswith('_') or c.startswith('__')]
    return df[[c for c in keep if c in df.columns]]

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    ordered = [c for c in DISPLAY_ORDER if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]

def _parse_to_date(dt_str):
    """Parse a date string (MM/DD/YYYY or YYYY-MM-DD) to a date object."""
    if not dt_str:
        return None
    s = str(dt_str).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def calculate_aging_bucket(doj_str, sep_str, fallback_date_str=None) -> str:
    start_dt = _parse_to_date(doj_str)
    if not start_dt:
        return ''
    end_dt = _parse_to_date(sep_str)
    if not end_dt:
        end_dt = _parse_to_date(fallback_date_str)
    if not end_dt:
        end_dt = date.today()
    delta = (end_dt - start_dt).days
    if delta < 0:   return ''
    if delta <= 30:  return '0-30'
    if delta <= 60:  return '31-60'
    if delta <= 90:  return '61-90'
    if delta <= 180: return '91-180'
    if delta <= 365: return '181-1yr'
    if delta <= 730: return '1-2yrs'
    return '2yrs+'

def apply_aging_bucket(df: pd.DataFrame, ref_date=None, use_date_exported=False) -> pd.DataFrame:
    if 'DOJ Knack' not in df.columns:
        return df
    def _row(row):
        sep = row.get('Date of Separation', '')
        fallback = row.get('Date Exported') if use_date_exported else None
        return calculate_aging_bucket(row.get('DOJ Knack', ''), sep, fallback)
    df = df.copy()
    df['Aging Bucket'] = df.apply(_row, axis=1)
    return df

def apply_active_nulls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'Active/Inactive' in df.columns:
        mask = df['Active/Inactive'] == 'Active'
        for col in ['Date of Separation', 'Attrition Type', 'Reason for Attrition']:
            if col in df.columns:
                df.loc[mask, col] = ''
    return df

def load_excel(uploaded_file) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(uploaded_file)
        sheet = 'Consolidated Staffing' if 'Consolidated Staffing' in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(uploaded_file, sheet_name=sheet, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.fillna('')

        # Date Exported is auto-generated — exclude from upload
        if 'Date Exported' in df.columns:
            df = df.drop(columns=['Date Exported'])
            st.toast('Date Exported is auto-generated and was excluded from upload', icon='ℹ️')

        accepted = set(get_all_accepted_columns())
        keep = [c for c in df.columns if c in accepted]
        ignored = [c for c in df.columns if c not in accepted]
        if ignored:
            st.toast(f'Skipped {len(ignored)} unrecognised column(s)', icon='⚠️')
        df = df[keep]

        # Ensure Active/Inactive exists and default blanks to Inactive
        if 'Active/Inactive' not in df.columns:
            df['Active/Inactive'] = 'Inactive'
            st.toast('Active/Inactive column was missing — defaulted to Inactive', icon='⚠️')
        else:
            df.loc[df['Active/Inactive'].astype(str).str.strip() == '', 'Active/Inactive'] = 'Inactive'

        if 'ECN' in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=['ECN'], keep='last')
            removed = before - len(df)
            if removed:
                st.toast(f'Removed {removed} duplicate ECN row(s)', icon='⚠️')
        return df
    except Exception as e:
        st.error(f'Error reading Excel: {e}')
        return pd.DataFrame()

def clean_export_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe for export: strip time from dates, replace NaN/None with blank."""
    df = df.copy()
    # Replace all NaN/None/NaT with empty string
    df = df.replace({pd.NaT: '', np.nan: '', None: ''})
    # Strip time from known date columns
    date_cols = ['DOJ Knack', 'Date of Separation', 'Date Exported',
                 'Effective From', 'Effective To', 'Start', 'End',
                 'Last Updated', 'created_at', 'updated_at', 'last_upload']
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({
                'nan': '', 'None': '', 'NaT': '', 'nat': '', 'null': ''
            })
            # Strip time component (e.g. "2026-03-15 00:00:00" -> "2026-03-15")
            df[col] = df[col].apply(lambda x: x.split(' ')[0] if ' ' in str(x) and str(x) != '' else x)
    return df

def df_to_excel_bytes(df: pd.DataFrame, sheet_name='Staffing') -> bytes:
    df = clean_export_df(df)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        wb = writer.book
        ws = writer.sheets[sheet_name]
        hdr = wb.add_format({'bold': True, 'bg_color': '#2b3c78', 'font_color': 'white', 'border': 1})
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, hdr)
            ws.set_column(i, i, max(15, len(str(col)) + 2))
    return buf.getvalue()

def generate_template_bytes() -> bytes:
    data = {
        'ECN': ['EMP001', 'EMP002'], 'Employee': ['John Doe', 'Jane Smith'],
        'Client': ['ABC Corp', 'XYZ Inc'], 'Sub-Process': ['Support', 'Billing'],
        'Supervisor': ['Manager A', 'Manager B'], 'Manager': ['Director X', 'Director Y'],
        'Role': ['Agent', 'Senior Agent'], 'Process Owner': ['Owner 1', 'Owner 2'],
        'Billable/Buffer': ['Billable', 'Buffer'], 'DOJ Knack': ['2024-01-15', '2024-03-01'],
        'Date of Separation': ['', ''], 'Active/Inactive': ['Active', 'Active'],
        'Email': ['john@company.com', 'jane@company.com'], 'NT Login': ['jdoe', 'jsmith'],
        'Structure': ['Ops', 'Ops'], 'Department': ['Customer Service', 'Finance'],
        'Location': ['Manila', 'Cebu'], 'Gender': ['Male', 'Female'],
        'Global ID (GPP)': ['GPP001', 'GPP002'], 'Attrition Type': ['', ''],
        'Reason for Attrition': ['', ''], 'CDP Email': ['john.cdp@co.com', 'jane.cdp@co.com'],
        'Overall Location': ['PH', 'PH'],
    }
    return df_to_excel_bytes(pd.DataFrame(data), sheet_name='Consolidated Staffing')

def get_effective_dates(row: dict, upload_date: str) -> tuple:
    eff_from = parse_date(row.get('Effective From', ''))
    eff_to   = parse_date(row.get('Effective To', ''))
    if not eff_from:
        doj = parse_date(row.get('DOJ Knack', ''))
        eff_from = doj if (doj and doj <= upload_date) else upload_date
    if not eff_to:
        eff_to = '12/31/9999'
    return eff_from, eff_to

BATCH_SIZE = 500

# ─── DB OPS ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def get_all_employees_df() -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(
            text('SELECT ecn, data, created_at, updated_at, last_upload FROM employees'),
            engine
        )
        if df.empty:
            return df
        records = []
        for _, row in df.iterrows():
            data = row['data']
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    data = {}
            if not isinstance(data, dict):
                data = {}
            data['ECN']          = row['ecn']
            data['_created_at']  = row['created_at']
            data['_updated_at']  = row['updated_at']
            data['_last_upload'] = row['last_upload']
            records.append(data)
        out = pd.DataFrame(records)
        out = out[~out['ECN'].astype(str).str.startswith('_')]
        out = filter_accepted_columns(out)
        out = apply_aging_bucket(out)
        out = apply_active_nulls(out)
        out = reorder_columns(out)
        return out
    except Exception as e:
        st.error(f'Error loading employees: {e}')
        return pd.DataFrame()

def get_employee(engine, ecn: str) -> dict:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text('SELECT data FROM employees WHERE ecn = :ecn'), {'ecn': ecn}
            ).fetchone()
        if not row:
            return None
        data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
        data['ECN'] = ecn
        return data
    except Exception:
        return None

def upsert_employees(df: pd.DataFrame, upload_date: str, progress_bar=None):
    engine = get_engine()
    if engine is None:
        return 0, 0, 'Database not connected'
    
    try:
        total = len(df)
        df['ECN'] = df['ECN'].astype(str).str.strip()
        ecns_to_check = df['ECN'].unique().tolist()
        
        if not ecns_to_check:
            return 0, 0, None

        # --- OPTIMIZATION 1: Selective Loading ---
        # Instead of loading ALL employees, we only load the ones in this file
        existing_map = {}
        batch_query_size = 1000
        for i in range(0, len(ecns_to_check), batch_query_size):
            batch_ecns = ecns_to_check[i:i + batch_query_size]
            ph = ','.join([f':e{j}' for j in range(len(batch_ecns))])
            params = {f'e{j}': e for j, e in enumerate(batch_ecns)}
            
            with engine.connect() as conn:
                res = conn.execute(text(f"SELECT ecn, data, last_upload FROM employees WHERE ecn IN ({ph})"), params).fetchall()
                for r in res:
                    # Pre-parse JSON once
                    data = json.loads(r[1]) if isinstance(r[1], str) else (r[1] or {})
                    data['_last_upload'] = r[2]
                    existing_map[r[0]] = data

        # --- OPTIMIZATION 2: Selective Manual Edits ---
        manual_edits = {}
        for i in range(0, len(ecns_to_check), batch_query_size):
            batch_ecns = ecns_to_check[i:i + batch_query_size]
            ph = ','.join([f':e{j}' for j in range(len(batch_ecns))])
            params = {f'e{j}': e for j, e in enumerate(batch_ecns)}
            with engine.connect() as conn:
                rows = conn.execute(text(
                    f"SELECT ecn, field, MAX(start_date) FROM history WHERE source='manual_edit' AND ecn IN ({ph}) GROUP BY ecn, field"
                ), params).fetchall()
                for r in rows: manual_edits[(r[0], r[1])] = r[2]

        inserted, updated, skipped_manual = 0, 0, 0
        new_employees, updated_employees, history_records, unchanged_ecns = [], [], [], []

        # --- OPTIMIZATION 3: Faster Processing Loop ---
        # Cache accepted columns outside the loop
        accepted_cols = set(get_all_accepted_columns())
        
        for idx, row_tuple in enumerate(df.iterrows()):
            _, row = row_tuple
            ecn = row['ECN']
            if not ecn or ecn.lower() == 'nan': continue
            
            # Fast row processing
            row_dict = row.to_dict()
            doc = {k.replace('.', '_'): safe_str(v) for k, v in row_dict.items() if k.replace('.', '_') in accepted_cols}
            doc['ECN'] = ecn
            
            existing = existing_map.get(ecn)
            eff_from, eff_to = get_effective_dates(row_dict, upload_date)

            if existing is None:
                doc['_created_at'] = doc['_updated_at'] = doc['_last_upload'] = upload_date
                new_employees.append({
                    'ecn': ecn, 'data': json.dumps(doc),
                    'created_at': upload_date, 'updated_at': upload_date, 'last_upload': upload_date
                })
                inserted += 1
                emp_name = doc.get('Employee', '')
                for field, val in doc.items():
                    if field.startswith('_') or field in ('Effective From', 'Effective To') or val == '': continue
                    history_records.append({
                        'ecn': ecn, 'employee_name': emp_name, 'field': field, 'value': val, 
                        'prev_value': '', 'start_date': eff_from, 'end_date': eff_to, 'source': 'excel_upload'
                    })
            else:
                changed = False
                last_upload = existing.get('_last_upload', '2000-01-01')
                emp_name = doc.get('Employee', existing.get('Employee', ''))
                for field, new_val in doc.items():
                    if field.startswith('_') or field in ('Effective From', 'Effective To'): continue
                    old_val = existing.get(field, '')
                    if new_val != old_val:
                        if manual_edits.get((ecn, field), '') > last_upload:
                            skipped_manual += 1
                            continue
                        history_records.append({
                            'ecn': ecn, 'employee_name': emp_name, 'field': field, 'value': new_val, 
                            'prev_value': old_val, 'start_date': eff_from, 'end_date': eff_to, 
                            'source': 'excel_upload', '_close_prev': True, '_ecn': ecn, '_field': field
                        })
                        existing[field] = new_val
                        changed = True
                
                if changed:
                    existing['_updated_at'] = existing['_last_upload'] = upload_date
                    # Remove internal helper key before saving
                    existing.pop('_last_upload', None) 
                    updated_employees.append({
                        'ecn': ecn, 'data': json.dumps(existing),
                        'updated_at': upload_date, 'last_upload': upload_date
                    })
                    updated += 1
                else:
                    unchanged_ecns.append(ecn)

            if progress_bar and idx % 200 == 0:
                progress_bar.progress(min(0.9, (idx + 1) / total), text=f'Processing {idx+1:,}/{total:,}…')

        # --- OPTIMIZATION 4: High-Speed Batch Writes ---
        with engine.begin() as conn: # Use engine.begin() for a single transaction
            # Upsert New/Existing (using INSERT IGNORE or ON DUPLICATE KEY)
            if new_employees:
                for i in range(0, len(new_employees), BATCH_SIZE):
                    conn.execute(text(
                        'INSERT INTO employees (ecn, data, created_at, updated_at, last_upload) '
                        'VALUES (:ecn, :data, :created_at, :updated_at, :last_upload) '
                        'ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=VALUES(updated_at), last_upload=VALUES(last_upload)'
                    ), new_employees[i:i+BATCH_SIZE])

            # History updates (Closing previous and inserting new)
            if history_records:
                # Optimized Close logic: Group updates by upload date
                close_targets = [h for h in history_records if h.get('_close_prev')]
                for h in close_targets:
                    conn.execute(text(
                        "UPDATE history SET end_date=:upload WHERE ecn=:ecn AND field=:field AND end_date='12/31/9999' AND start_date<:start"
                    ), {'upload': upload_date, 'ecn': h['_ecn'], 'field': h['_field'], 'start': h['start_date']})

                hist_clean = [{k: v for k, v in h.items() if not k.startswith('_')} for h in history_records]
                for i in range(0, len(hist_clean), BATCH_SIZE):
                    conn.execute(text(
                        'INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source) '
                        'VALUES (:ecn, :employee_name, :field, :value, :prev_value, :start_date, :end_date, :source)'
                    ), hist_clean[i:i+BATCH_SIZE])

            if updated_employees:
                for i in range(0, len(updated_employees), BATCH_SIZE):
                    conn.execute(text(
                        'UPDATE employees SET data=:data, updated_at=:updated_at, last_upload=:last_upload WHERE ecn=:ecn'
                    ), updated_employees[i:i+BATCH_SIZE])

            if unchanged_ecns:
                for i in range(0, len(unchanged_ecns), BATCH_SIZE):
                    batch = unchanged_ecns[i:i+BATCH_SIZE]
                    ph = ','.join([f':e{j}' for j in range(len(batch))])
                    params = {f'e{j}': e for j, e in enumerate(batch)}
                    params['upload'] = upload_date
                    conn.execute(text(f"UPDATE employees SET last_upload=:upload WHERE ecn IN ({ph})"), params)

            conn.execute(text(
                'INSERT INTO upload_log (upload_date, rows_processed, inserted, updated, skipped_manual) '
                'VALUES (:d, :r, :i, :u, :s)'
            ), {'d': upload_date, 'r': total, 'i': inserted, 'u': updated, 's': skipped_manual})

        if progress_bar: progress_bar.progress(1.0, text='Done!')
        st.cache_data.clear()
        return inserted, updated, None

    except Exception as e:
        import traceback
        return 0, 0, f"{str(e)}\n{traceback.format_exc()}"
        

def record_manual_edit(ecn: str, field: str, new_value: str, start_date: str, end_date: str = '12/31/9999'):
    engine = get_engine()
    if engine is None:
        return False, 'Database not connected'
    try:
        existing = get_employee(engine, ecn)
        if not existing:
            return False, 'Employee not found'
        old_value = existing.get(field, '')
        # Normalize dates to MM/DD/YYYY for storage consistency
        norm_start = parse_date(start_date) or start_date
        norm_end   = parse_date(end_date)   or end_date
        with engine.connect() as conn:
            conn.execute(text(
                "UPDATE history SET end_date=:start WHERE ecn=:ecn AND field=:field "
                "AND end_date='12/31/9999' AND start_date<=:start"
            ), {'start': norm_start, 'ecn': ecn, 'field': field})
            conn.execute(text(
                'UPDATE history SET end_date=:start WHERE ecn=:ecn AND field=:field '
                'AND start_date>:start AND start_date<:end'
            ), {'start': norm_start, 'end': norm_end, 'ecn': ecn, 'field': field})
            conn.execute(text(
                "INSERT INTO history (ecn, employee_name, field, value, prev_value, start_date, end_date, source) "
                "VALUES (:ecn, :emp, :field, :val, :prev, :start, :end, 'manual_edit') "
                "ON DUPLICATE KEY UPDATE value=VALUES(value), prev_value=VALUES(prev_value), end_date=VALUES(end_date)"
            ), {'ecn': ecn, 'emp': existing.get('Employee', ''), 'field': field,
                'val': new_value, 'prev': old_value, 'start': norm_start, 'end': norm_end})
            if norm_end == '12/31/9999':
                existing[field] = new_value
                existing['_updated_at'] = norm_start
                conn.execute(text('UPDATE employees SET data=:data, updated_at=:upd WHERE ecn=:ecn'),
                             {'ecn': ecn, 'data': json.dumps(existing), 'upd': norm_start})
            conn.commit()
        st.cache_data.clear()
        return True, 'Saved'
    except Exception as e:
        return False, f'Error: {str(e)[:120]}'

def get_employee_history(ecn: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(
            text('SELECT id, ecn, employee_name, field, value, prev_value, start_date, end_date, source '
                 'FROM history WHERE ecn=:ecn ORDER BY field, start_date DESC'),
            engine, params={'ecn': ecn}
        )
        if df.empty:
            return df
        df.columns = ['ID', 'ECN', 'Employee', 'Field', 'Value', 'Previous', 'Start', 'End', 'Source']
        return df
    except Exception:
        return pd.DataFrame()

def delete_history_record(record_id: int):
    engine = get_engine()
    if engine is None:
        return False, 'Database not connected'
    try:
        with engine.connect() as conn:
            record = conn.execute(
                text('SELECT * FROM history WHERE id=:id'), {'id': record_id}
            ).mappings().fetchone()
            if not record:
                return False, 'Record not found'
            ecn, field, start_date = record['ecn'], record['field'], record['start_date']
            # Normalize start_date so it matches the end_date stored by record_manual_edit
            norm_start = parse_date(start_date) or start_date
            conn.execute(text('DELETE FROM history WHERE id=:id'), {'id': record_id})
            prev = conn.execute(text(
                'SELECT * FROM history WHERE ecn=:ecn AND field=:field AND end_date=:start '
                'ORDER BY start_date DESC LIMIT 1'
            ), {'ecn': ecn, 'field': field, 'start': norm_start}).mappings().fetchone()
            emp = get_employee(engine, ecn)
            if prev:
                conn.execute(text(
                    "UPDATE history SET end_date='12/31/9999' WHERE ecn=:ecn AND field=:field AND start_date=:ps"
                ), {'ecn': ecn, 'field': field, 'ps': prev['start_date']})
                if emp:
                    emp[field] = prev['value']
                    conn.execute(text('UPDATE employees SET data=:data WHERE ecn=:ecn'),
                                 {'ecn': ecn, 'data': json.dumps(emp)})
            else:
                if emp:
                    emp[field] = ''
                    conn.execute(text('UPDATE employees SET data=:data WHERE ecn=:ecn'),
                                 {'ecn': ecn, 'data': json.dumps(emp)})
            conn.commit()
        st.cache_data.clear()
        return True, 'Deleted and restored previous value'
    except Exception as e:
        return False, f'Error: {str(e)[:120]}'

def update_history_record(record_id: int, new_value: str, new_start: str, new_end: str):
    engine = get_engine()
    if engine is None:
        return False, 'Database not connected'
    try:
        # Normalize dates to ISO format so lookups stay consistent
        norm_start = parse_date(new_start) or new_start
        norm_end   = parse_date(new_end)   or new_end
        with engine.connect() as conn:
            record = conn.execute(
                text('SELECT ecn, field, end_date FROM history WHERE id=:id'), {'id': record_id}
            ).mappings().fetchone()
            if not record:
                return False, 'Record not found'
            conn.execute(text(
                'UPDATE history SET value=:val, start_date=:start, end_date=:end WHERE id=:id'
            ), {'val': new_value, 'start': norm_start, 'end': norm_end, 'id': record_id})
            if record['end_date'] == '12/31/9999' or norm_end == '12/31/9999':
                emp = get_employee(engine, record['ecn'])
                if emp:
                    emp[record['field']] = new_value
                    conn.execute(text('UPDATE employees SET data=:data WHERE ecn=:ecn'),
                                 {'ecn': record['ecn'], 'data': json.dumps(emp)})
            conn.commit()
        st.cache_data.clear()
        return True, 'Updated'
    except Exception as e:
        return False, f'Error: {str(e)[:120]}'

def compact_history() -> int:
    engine = get_engine()
    if engine is None:
        return 0
    try:
        with engine.connect() as conn:
            r = conn.execute(text("DELETE FROM history WHERE value=prev_value AND value!=''"))
            conn.commit()
            st.cache_data.clear()
            return r.rowcount
    except Exception:
        return 0

@st.cache_data(ttl=60, show_spinner=False)
def get_db_stats():
    engine = get_engine()
    if engine is None:
        return None, None, None
    try:
        with engine.connect() as conn:
            emp = conn.execute(text("SELECT COUNT(*) FROM employees WHERE LEFT(ecn,1)!='_'")).scalar()
            active = conn.execute(text(
                "SELECT COUNT(*) FROM employees WHERE LEFT(ecn,1)!='_' AND data->>'$.\"Active/Inactive\"'='Active'"
            )).scalar()
            hist = conn.execute(text('SELECT COUNT(*) FROM history')).scalar()
        return emp, active, hist
    except Exception:
        return None, None, None

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
NAV = {
    'upload':    ('📤', 'Upload & Sync'),
    'employees': ('👤', 'Employees'),
    'export':    ('📊', 'Export'),
    'history':   ('📜', 'History'),
    'dbtools':   ('🛠️', 'DB Tools'),
}

if 'nav_page' not in st.session_state:
    st.session_state.nav_page = 'upload'

with st.sidebar:
    render_sidebar_brand()

    for key, (icon, label) in NAV.items():
        is_active = st.session_state.nav_page == key
        btn_type = 'primary' if is_active else 'secondary'
        if st.button(f'{icon}  {label}', key=f'nav_{key}',
                     use_container_width=True, type=btn_type):
            st.session_state.nav_page = key
            st.rerun()

    st.markdown('<div style="height:1rem;"></div>', unsafe_allow_html=True)

    # Connection status
    if db_connected():
        st.markdown("""
        <div style="
            background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.2);
            border-radius:6px; padding:0.5rem 0.75rem;
            font-size:0.78rem; font-weight:600; color:#86efac;
            display:flex; align-items:center; gap:6px;
        ">
            <span style="width:6px;height:6px;background:#22c55e;border-radius:50%;
            box-shadow:0 0 6px #22c55e;display:inline-block;flex-shrink:0;"></span>
            TiDB Connected
        </div>
        """, unsafe_allow_html=True)
    else:
        err = st.session_state.get('_db_err', '')
        if err:
            st.markdown(f"""
            <div style="
                background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.2);
                border-radius:6px; padding:0.5rem 0.75rem;
                font-size:0.75rem; font-weight:500; color:#fca5a5;
            ">⚠️ {err[:100]}</div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="
                background:rgba(244,126,32,0.08); border:1px solid rgba(244,126,32,0.2);
                border-radius:6px; padding:0.5rem 0.75rem;
                font-size:0.78rem; font-weight:600; color:#fdba74;
            ">⚙️ No TiDB URI configured</div>
            """, unsafe_allow_html=True)

    st.markdown('<div style="height:0.75rem;"></div>', unsafe_allow_html=True)
    if st.button('🔄  Refresh Cache', use_container_width=True, type='secondary'):
        st.cache_data.clear()
        st.toast('Cache cleared!', icon='✅')
        st.rerun()

page = st.session_state.nav_page

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: UPLOAD & SYNC
# ═══════════════════════════════════════════════════════════════════════════════
if page == 'upload':
    page_header('Upload & Sync', 'Import your staffing Excel file and sync it to TiDB')
    if not db_connected():
        st.error('Configure a valid `TIDB_URI` in Streamlit Secrets to continue.')
        st.stop()

    # Stats row
    emp_count, active_count, hist_count = get_db_stats()
    if emp_count is not None:
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(stat_card('Total Employees', f'{emp_count:,}', '👥', 'var(--brand-light)'), unsafe_allow_html=True)
        with c2: st.markdown(stat_card('Active', f'{active_count:,}', '✅', '#22c55e'), unsafe_allow_html=True)
        with c3: st.markdown(stat_card('History Records', f'{hist_count:,}', '📋', 'var(--brand-orange)'), unsafe_allow_html=True)
        st.markdown('<div style="height:1rem;"></div>', unsafe_allow_html=True)

    section_label('Template')
    st.download_button(
        '⬇️  Download Excel Template',
        data=generate_template_bytes(),
        file_name='staffing_template.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )

    section_label('File Upload')
    col_up, col_tip = st.columns([2, 1])
    with col_up:
        uploaded = st.file_uploader('Choose Excel file (.xlsx)', type=['xlsx'], label_visibility='collapsed')
    with col_tip:
        with st.expander('📌 Upload Tips'):
            st.markdown("""
- **Required column:** `ECN` (unique ID)
- Dates: `YYYY-MM-DD` or `MM/DD/YYYY`
- Optional: `Effective From`, `Effective To`
- Duplicate ECNs: last row wins
- Unrecognised columns are skipped
            """)

    if uploaded:
        with st.spinner('Reading file…'):
            df = load_excel(uploaded)
        if df.empty:
            st.error('Could not read the file — check the format.')
            st.stop()

        st.success(f'**{len(df):,} rows** · **{len(df.columns)} columns** recognised')

        preview_df = apply_aging_bucket(apply_active_nulls(reorder_columns(df.copy())))
        with st.expander('Preview — first 10 rows'):
            st.dataframe(preview_df.head(10), use_container_width=True, hide_index=True)

        st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)
        if st.button('🚀  Sync to Database', type='primary'):
            today_str = date.today().isoformat()
            prog = st.progress(0, text='Preparing…')
            ins, upd, err = upsert_employees(df, today_str, progress_bar=prog)
            if err:
                st.error(f'Sync failed: {err}')
            else:
                ec, _, _ = get_db_stats()
                st.success(
                    f'✅ Sync complete — **{ins}** inserted · **{upd}** updated · **{ec:,}** total in DB'
                )

    # Column management
    section_label('Accepted Columns')
    with st.expander('📋 Core columns (read-only)'):
        st.write(CORE_COLS)

    custom_cols = get_custom_columns()
    if custom_cols:
        st.markdown('**Custom columns:**')
        grid = st.columns(min(4, len(custom_cols)))
        for i, col in enumerate(custom_cols):
            with grid[i % len(grid)]:
                ca, cb = st.columns([5, 1])
                with ca:
                    st.markdown(
                        f'<div style="background:var(--bg-elevated);padding:5px 10px;'
                        f'border-radius:4px;border-left:3px solid var(--brand-light);'
                        f'font-size:0.85rem;color:var(--text-primary);">{col}</div>',
                        unsafe_allow_html=True
                    )
                with cb:
                    if st.button('✕', key=f'rem_{col}', help=f'Remove {col}'):
                        remove_custom_column(col)
                        st.rerun()

    na1, na2 = st.columns([3, 1])
    with na1:
        new_col = st.text_input('Add new column', placeholder='e.g. Shift Timing',
                                label_visibility='collapsed')
    with na2:
        if st.button('➕  Add', type='primary'):
            if new_col.strip():
                clean = new_col.strip()
                if clean in CORE_COLS:
                    st.warning('Already a core column.')
                elif clean in custom_cols:
                    st.warning('Already added.')
                else:
                    add_custom_column(clean)
                    st.success(f'Added `{clean}`')
                    st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: EMPLOYEES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == 'employees':
    page_header('Employees', 'Browse, filter, and edit employee records')
    engine = get_engine()
    if engine is None:
        st.error('Connect TiDB first.')
        st.stop()

    st.info('Select employees with checkboxes then click **Bulk Edit** · single selection opens individual edit')

    employees_df = get_all_employees_df()
    if employees_df.empty:
        st.warning('No employees found. Upload data first.')
        st.stop()

    # Filters — row 1
    fc = st.columns(4)
    search      = fc[0].text_input('🔍 Search', placeholder='Name / ECN / Email', label_visibility='collapsed')
    fs_opts     = ['All'] + sorted(employees_df['Active/Inactive'].dropna().unique().tolist()) if 'Active/Inactive' in employees_df.columns else ['All']
    filter_status = fc[1].selectbox('Status', fs_opts, key='ee_status')
    bb_opts     = ['All'] + sorted(employees_df['Billable/Buffer'].dropna().unique().tolist()) if 'Billable/Buffer' in employees_df.columns else ['All']
    filter_bb   = fc[2].selectbox('Billable/Buffer', bb_opts, key='ee_bb')
    loc_opts    = ['All'] + sorted(employees_df['Location'].dropna().unique().tolist()) if 'Location' in employees_df.columns else ['All']
    filter_loc  = fc[3].selectbox('Location', loc_opts, key='ee_loc')

    # Filters — row 2
    fc2 = st.columns(4)
    cl_opts  = ['All'] + sorted(employees_df['Client'].dropna().unique().tolist()) if 'Client' in employees_df.columns else ['All']
    filter_cl = fc2[0].selectbox('Client', cl_opts, key='ee_cl')
    sp_opts  = ['All'] + sorted(employees_df['Sub-Process'].dropna().unique().tolist()) if 'Sub-Process' in employees_df.columns else ['All']
    filter_sp = fc2[1].selectbox('Sub-Process', sp_opts, key='ee_sp')
    ro_opts  = ['All'] + sorted(employees_df['Role'].dropna().unique().tolist()) if 'Role' in employees_df.columns else ['All']
    filter_ro = fc2[2].selectbox('Role', ro_opts, key='ee_ro')
    su_opts  = ['All'] + sorted(employees_df['Supervisor'].dropna().unique().tolist()) if 'Supervisor' in employees_df.columns else ['All']
    filter_su = fc2[3].selectbox('Supervisor', su_opts, key='ee_su')

    def _apply(df, col, val):
        return df[df[col] == val] if val != 'All' and col in df.columns else df

    employees_df = _apply(employees_df, 'Active/Inactive', filter_status)
    employees_df = _apply(employees_df, 'Billable/Buffer', filter_bb)
    employees_df = _apply(employees_df, 'Location', filter_loc)
    employees_df = _apply(employees_df, 'Client', filter_cl)
    employees_df = _apply(employees_df, 'Sub-Process', filter_sp)
    employees_df = _apply(employees_df, 'Role', filter_ro)
    employees_df = _apply(employees_df, 'Supervisor', filter_su)

    if search:
        s = search.lower()
        mask = (
            employees_df.get('Employee', pd.Series()).str.lower().str.contains(s, na=False) |
            employees_df.get('ECN', pd.Series()).str.lower().str.contains(s, na=False) |
            employees_df.get('Email', pd.Series()).str.lower().str.contains(s, na=False)
        )
        employees_df = employees_df[mask]

    if employees_df.empty:
        st.warning('No employees match your filters.')
        st.stop()

    employees_df = reorder_columns(employees_df)
    display_cols = [c for c in DISPLAY_ORDER if c in employees_df.columns][:10]

    sa1, sa2, sa3 = st.columns([1, 1, 4])
    with sa1:
        if st.button('☑️  Select All', key='ee_all'):
            st.session_state['_sel_all'] = True; st.rerun()
    with sa2:
        if st.button('⬜  Clear', key='ee_clear'):
            st.session_state['_sel_all'] = False; st.rerun()
    with sa3:
        st.caption(f'{len(employees_df):,} employees shown')

    sel_default = st.session_state.get('_sel_all', False)
    edit_df = employees_df[display_cols].copy()
    edit_df.insert(0, 'Select', sel_default)
    edited = st.data_editor(
        edit_df, use_container_width=True, hide_index=True,
        column_config={'Select': st.column_config.CheckboxColumn('Select', default=False)},
        disabled=display_cols,
    )
    sel_idx = edited[edited['Select'] == True].index.tolist()

    if len(sel_idx) == 1:
        emp = employees_df.loc[sel_idx[0]].to_dict()
        ecn = emp['ECN']

        @st.dialog(f'✏️ Edit — {emp.get("Employee", ecn)}', width='large')
        def _single_edit():
            c1, c2 = st.columns(2)
            eff_from = c1.date_input('Effective From', value=date.today(), key=f'sef_{ecn}')
            eff_to   = c2.date_input('Effective To (blank = ongoing)', value=None, key=f'set_{ecn}')
            eff_from_s = eff_from.strftime('%m/%d/%Y')
            eff_to_s   = eff_to.strftime('%m/%d/%Y') if eff_to else '12/31/9999'
            if eff_to and eff_to <= eff_from:
                st.error('Effective To must be after Effective From'); return

            core = [f for f in ['Billable/Buffer','Active/Inactive','Client','Sub-Process',
                                 'Supervisor','Role','Manager','Location','Overall Location'] if f in emp]
            vals = {}
            cols = st.columns(3)
            for i, f in enumerate(core):
                with cols[i % 3]:
                    vals[f] = st.text_input(f, value=emp.get(f, ''), key=f'sec_{ecn}_{f}')

            if st.toggle('Show all fields', key=f'sem_{ecn}'):
                others = [k for k in emp if not k.startswith('_') and k not in core
                          and k not in ('ECN', 'Effective From', 'Effective To')]
                cols2 = st.columns(3)
                for i, f in enumerate(others):
                    with cols2[i % 3]:
                        vals[f] = st.text_input(f, value=emp.get(f, ''), key=f'seo_{ecn}_{f}')

            changes = {f: v for f, v in vals.items() if v != emp.get(f, '')}
            if not changes:
                st.info('No changes yet.'); return
            st.divider()
            st.markdown('**Changes to apply:**')
            for f, v in changes.items():
                st.markdown(f'- **{f}:** `{emp.get(f, "(blank)")}` → `{v}`')
            if st.button('💾  Save Changes', type='primary', key=f'ses_{ecn}'):
                saved = sum(1 for f, v in changes.items() if record_manual_edit(ecn, f, v, eff_from_s, eff_to_s)[0])
                if saved:
                    st.success(f'{saved} field(s) updated!'); st.rerun()

        _single_edit()

    elif len(sel_idx) > 1:
        if st.button('🔧  Bulk Edit Selected', type='primary'):
            st.session_state['_bulk_edit'] = True; st.rerun()

        if st.session_state.get('_bulk_edit'):
            @st.dialog(f'🔧 Bulk Edit — {len(sel_idx)} Employees', width='large')
            def _bulk_edit():
                emps = [employees_df.loc[i].to_dict() for i in sel_idx]
                ecns  = [e['ECN'] for e in emps]
                names = [e.get('Employee', e['ECN']) for e in emps]
                st.caption(', '.join(names[:5]) + ('…' if len(names) > 5 else ''))
                c1, c2 = st.columns(2)
                eff_from = c1.date_input('Effective From', value=date.today(), key='bef')
                eff_to   = c2.date_input('Effective To (blank = ongoing)', value=None, key='bet')
                eff_from_s = eff_from.strftime('%m/%d/%Y')
                eff_to_s   = eff_to.strftime('%m/%d/%Y') if eff_to else '12/31/9999'
                if eff_to and eff_to <= eff_from:
                    st.error('Effective To must be after Effective From'); return

                common = [f for f in ['Billable/Buffer','Active/Inactive','Client','Sub-Process',
                                      'Supervisor','Role','Manager','Location','Overall Location']
                          if f in employees_df.columns]
                st.caption('Pre-filled fields share the same value across all selected employees. Leave blank to skip.')
                vals = {}
                cols = st.columns(3)
                for i, f in enumerate(common):
                    fvals = [e.get(f, '') for e in emps]
                    shared = fvals[0] if len(set(fvals)) == 1 else ''
                    with cols[i % 3]:
                        vals[f] = st.text_input(f, value=shared,
                                                placeholder='(leave blank to skip)',
                                                key=f'be_{f}')

                if st.toggle('Show more fields', key='besm'):
                    others = [k for k in employees_df.columns
                              if not k.startswith('_') and k not in common
                              and k not in ('ECN', 'Select', 'Effective From', 'Effective To')]
                    cols2 = st.columns(3)
                    for i, f in enumerate(others):
                        fvals = [e.get(f, '') for e in emps]
                        shared = fvals[0] if len(set(fvals)) == 1 else ''
                        with cols2[i % 3]:
                            vals[f] = st.text_input(f, value=shared,
                                                    placeholder='(leave blank to skip)',
                                                    key=f'beo_{f}')

                changes = {f: v for f, v in vals.items() if v.strip()}
                if not changes:
                    st.info('Fill in at least one field to apply changes.'); return
                st.divider()
                st.warning(f'Will update **{len(ecns)} employees**:')
                for f, v in changes.items():
                    st.markdown(f'- **{f}** → `{v}`')
                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button('✅  Confirm', type='primary', key='be_confirm'):
                        saved = sum(
                            1 for ecn in ecns for f, v in changes.items()
                            if record_manual_edit(ecn, f, v, eff_from_s, eff_to_s)[0]
                        )
                        st.success(f'{saved} field updates saved!')
                        st.session_state['_bulk_edit'] = False; st.rerun()
                with bc2:
                    if st.button('Cancel', key='be_cancel'):
                        st.session_state['_bulk_edit'] = False; st.rerun()

            _bulk_edit()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: EXPORT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == 'export':
    page_header('Export', 'Generate staffing reports for any date range')
    if not db_connected():
        st.error('Connect TiDB first.'); st.stop()

    today = date.today()
    exp_type = st.radio('Export Type', ['Daily','Weekly','Monthly','Yearly','Custom Range'], horizontal=True)

    if exp_type == 'Daily':
        exp_date = st.date_input('Date', value=today)
        dates = [exp_date.isoformat()]; label = f'daily_{exp_date.strftime("%m-%d-%Y")}'
    elif exp_type == 'Weekly':
        ws = today - timedelta(days=today.weekday())
        exp_week = st.date_input('Week start (Monday)', value=ws)
        dates = [(exp_week + timedelta(days=i)).isoformat() for i in range(7)]
        label = f'weekly_{exp_week.strftime("%m-%d-%Y")}_to_{(exp_week + timedelta(days=6)).strftime("%m-%d-%Y")}'
    elif exp_type == 'Monthly':
        ec1, ec2 = st.columns(2)
        month = ec1.selectbox('Month', range(1,13), index=today.month-1,
                              format_func=lambda m: datetime(2000,m,1).strftime('%B'))
        year  = ec2.number_input('Year', 2020, 2035, today.year)
        last  = calendar.monthrange(year, month)[1]
        dates = [date(year, month, d).isoformat() for d in range(1, last+1)]
        label = f'monthly_{month:02d}-{year}'
    elif exp_type == 'Yearly':
        year  = st.number_input('Year', 2020, 2035, today.year)
        dates = [date(year, m, d).isoformat()
                 for m in range(1,13) for d in range(1, calendar.monthrange(year,m)[1]+1)]
        label = f'yearly_{year}'
    else:
        ec1, ec2 = st.columns(2)
        start = ec1.date_input('Start', value=today-timedelta(days=7))
        end   = ec2.date_input('End',   value=today)
        dates = [(start + timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
        label = f'custom_{start.strftime("%m-%d-%Y")}_to_{end.strftime("%m-%d-%Y")}'

    if len(dates) > 366:
        st.warning('Export limited to 366 days.'); st.stop()

    # Format dates for display as MM/DD/YYYY
    disp_start = datetime.strptime(dates[0], '%Y-%m-%d').strftime('%m/%d/%Y')
    disp_end   = datetime.strptime(dates[-1], '%Y-%m-%d').strftime('%m/%d/%Y')
    st.markdown(
        f'<div style="background:var(--bg-card);border:1px solid var(--border);'
        f'border-radius:6px;padding:0.6rem 1rem;font-size:0.85rem;color:var(--text-muted);">'
        f'Range: <b style="color:var(--text-primary)">{disp_start}</b> → '
        f'<b style="color:var(--text-primary)">{disp_end}</b> · '
        f'<b style="color:var(--brand-light)">{len(dates)}</b> days</div>',
        unsafe_allow_html=True
    )
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

    ec1, ec2, ec3 = st.columns(3)
    export_mode   = ec1.radio('Sheet mode', ['Single sheet', 'One sheet per day'])
    filter_active = ec2.checkbox('Active employees only', value=False)
    exclude_sep   = ec3.checkbox('Exclude separated employees', value=False)
    filter_cl3    = st.text_input('Filter by Client (optional)', key='exp_cl')

    if st.button('📥  Generate Export', type='primary'):
        if len(dates) > 31 and export_mode == 'One sheet per day':
            st.warning('One sheet per day is limited to 31 days.'); st.stop()

        status = st.empty()
        status.info('Loading base data…')
        engine = get_engine()
        base_df = get_all_employees_df()
        if base_df.empty:
            st.warning('No employee data.'); st.stop()

        # Pre-compute date columns — pd.to_datetime handles almost any format natively
        base_df['__doj_dt'] = pd.to_datetime(base_df['DOJ Knack'], errors='coerce') if 'DOJ Knack' in base_df.columns else pd.Series(dtype='datetime64[ns]')
        base_df['__sep_dt'] = pd.to_datetime(base_df['Date of Separation'], errors='coerce') if 'Date of Separation' in base_df.columns else pd.Series(dtype='datetime64[ns]')

        # Load ALL history once
        with engine.connect() as conn:
            hist_all = pd.read_sql(
                text('SELECT ecn, field, value, start_date, end_date FROM history'), conn
            )

        hist_lookup = defaultdict(list)
        if not hist_all.empty:
            hist_all['start_dt'] = pd.to_datetime(hist_all['start_date'], errors='coerce')
            hist_all['end_dt']   = pd.to_datetime(hist_all['end_date'],   errors='coerce')
            hist_all = hist_all.sort_values('start_dt', ascending=False)
            for _, h in hist_all.iterrows():
                hist_lookup[(h['ecn'], h['field'])].append(
                    {'start': h['start_dt'], 'end': h['end_dt'], 'value': h['value']}
                )

        base_cols  = [c for c in base_df.columns if not c.startswith('_') and not c.startswith('__')]
        base_clean = base_df[base_cols + ['__doj_dt', '__sep_dt']].copy()
        all_dfs    = []
        ts_list    = [pd.Timestamp(datetime.strptime(d, '%Y-%m-%d').date()) for d in dates]

        total_loaded = len(base_clean)
        excluded_doj = 0
        excluded_sep = 0

        for i, (d_str, d_ts) in enumerate(zip(dates, ts_list)):
            mask = pd.Series(True, index=base_clean.index)
            # Exclude future hires (not yet employed on this date)
            doj_mask = base_clean['__doj_dt'].isna() | (base_clean['__doj_dt'] <= d_ts)
            excluded_doj += (~doj_mask).sum()
            mask &= doj_mask
            # Optionally exclude already-separated employees
            if exclude_sep:
                sep_mask = base_clean['__sep_dt'].isna() | (base_clean['__sep_dt'] >= d_ts)
                excluded_sep += (~sep_mask).sum()
                mask &= sep_mask
            df_day = base_clean[mask].copy()
            if df_day.empty:
                continue
            for field in base_cols:
                if field == 'ECN':
                    continue
                ovr = {}
                for ecn in df_day['ECN'].unique():
                    for p in hist_lookup.get((ecn, field), []):
                        if p['start'] <= d_ts <= p['end']:
                            ovr[ecn] = p['value']; break
                if ovr:
                    df_day[field] = df_day['ECN'].map(ovr).fillna(df_day[field])
            df_day = df_day.drop(columns=[c for c in df_day.columns if c.startswith('__')], errors='ignore')
            if filter_active and 'Active/Inactive' in df_day.columns:
                df_day = df_day[df_day['Active/Inactive'] == 'Active']
            if filter_cl3 and 'Client' in df_day.columns:
                df_day = df_day[df_day['Client'].str.contains(filter_cl3, case=False, na=False)]
            df_day = filter_accepted_columns(df_day)
            df_day['Date Exported'] = d_ts.strftime('%m/%d/%Y')
            df_day = apply_aging_bucket(df_day, use_date_exported=True)
            df_day = apply_active_nulls(df_day)
            df_day = reorder_columns(df_day)
            if not df_day.empty:
                all_dfs.append(df_day)
            if i % 30 == 0 or i == len(dates)-1:
                status.info(f'Processed {i+1}/{len(dates)} days…')

        if not all_dfs:
            st.warning('No data for the selected range.'); st.stop()

        # Show breakdown metrics
        total_out = sum(len(d) for d in all_dfs)
        st.markdown(
            f'<div style="background:var(--bg-card);border:1px solid var(--border);'
            f'border-radius:6px;padding:0.6rem 1rem;font-size:0.85rem;color:var(--text-muted);margin-bottom:0.75rem;">'
            f'Loaded <b>{total_loaded:,}</b> from DB · '
            f'Excluded <b>{excluded_doj:,}</b> future hire(s) · '
            f'Excluded <b>{excluded_sep:,}</b> separated (filter on) · '
            f'Output <b style="color:var(--brand-light)">{total_out:,}</b> row(s)</div>',
            unsafe_allow_html=True
        )

        if export_mode == 'Single sheet':
            combined = pd.concat(all_dfs, ignore_index=True)
            cols = ['Date Exported'] + [c for c in combined.columns if c != 'Date Exported']
            combined = combined[cols]
            status.success(f'✅ {len(combined):,} rows across {len(all_dfs)} days')
            st.dataframe(combined.head(20), use_container_width=True, hide_index=True)
            st.download_button(
                f'⬇️  Download {exp_type} Export',
                data=df_to_excel_bytes(combined, 'Staffing Export'),
                file_name=f'staffing_{label}_single.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )
        else:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                wb = writer.book
                hdr = wb.add_format({'bold': True, 'bg_color': '#2b3c78', 'font_color': 'white'})
                for i, df_day in enumerate(all_dfs):
                    sheet = dates[i].replace('-', '')[-6:]
                    df_day_clean = clean_export_df(df_day)
                    df_day_clean.to_excel(writer, index=False, sheet_name=sheet)
                    ws = writer.sheets[sheet]
                    for j, col in enumerate(df_day_clean.columns):
                        ws.write(0, j, col, hdr)
            status.success(f'✅ {len(all_dfs)} sheets generated!')
            st.download_button(
                '⬇️  Download Daily Export',
                data=buf.getvalue(),
                file_name=f'staffing_{label}_daily.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: HISTORY
# ═══════════════════════════════════════════════════════════════════════════════
elif page == 'history':
    page_header('Change History', 'Audit trail of all employee data changes')
    if not db_connected():
        st.error('Connect TiDB first.'); st.stop()

    engine = get_engine()

    # ── Filters ──────────────────────────────────────────────────────────────
    section_label('Filters')
    f1, f2, f3, f4, f5 = st.columns([2, 2, 2, 2, 1])
    with f1:
        search = st.text_input('🔍 Search ECN / Name', placeholder='e.g. EMP001', key='hist_search')
    with f2:
        # Load distinct fields for filter
        try:
            with engine.connect() as conn:
                field_rows = conn.execute(text(
                    "SELECT DISTINCT field FROM history ORDER BY field LIMIT 200"
                )).fetchall()
            field_opts = ['All'] + [r[0] for r in field_rows if r[0]]
        except Exception:
            field_opts = ['All']
        filter_field = st.selectbox('Field', field_opts, key='hist_field')
    with f3:
        filter_source = st.selectbox('Source', ['All', 'excel_upload', 'manual_edit'], key='hist_source')
    with f4:
        date_mode = st.selectbox('Date filter', ['All dates', 'Start date range'], key='hist_date_mode')
    with f5:
        if st.button('🔄 Reset', key='hist_reset', use_container_width=True):
            for k in ['hist_search', 'hist_field', 'hist_source', 'hist_date_mode',
                      'hist_date_from', 'hist_date_to']:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

    date_from, date_to = None, None
    if date_mode == 'Start date range':
        d1, d2 = st.columns(2)
        with d1:
            date_from = st.date_input('From', value=date(2024, 1, 1), key='hist_date_from')
        with d2:
            date_to = st.date_input('To', value=date.today(), key='hist_date_to')

    # ── Build query ──────────────────────────────────────────────────────────
    where_clauses = []
    params = {}

    if search:
        where_clauses.append("(ecn LIKE :s OR COALESCE(employee_name, '') LIKE :s)")
        params['s'] = f'%{search}%'
    if filter_field != 'All':
        where_clauses.append("field = :f")
        params['f'] = filter_field
    if filter_source != 'All':
        where_clauses.append("source = :src")
        params['src'] = filter_source
    if date_mode == 'Start date range' and date_from and date_to:
        where_clauses.append("start_date BETWEEN :df AND :dt")
        params['df'] = date_from.isoformat()
        params['dt'] = date_to.isoformat()

    where_sql = ('WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

    try:
        with engine.connect() as conn:
            agg = conn.execute(text(
                f"SELECT ecn, MAX(employee_name) as emp, COUNT(*) as cnt, "
                f"MAX(start_date) as last_upd, "
                f"GROUP_CONCAT(DISTINCT field ORDER BY field SEPARATOR ', ') as fields "
                f"FROM history {where_sql} "
                f"GROUP BY ecn ORDER BY last_upd DESC LIMIT 500"
            ), params).fetchall()
    except Exception as e:
        st.error(f'Query error: {e}'); st.stop()

    if not agg:
        st.info('No history records match your filters.')
        st.stop()

    agg_df = pd.DataFrame(agg, columns=['ECN','Employee','Records','Last Updated','Fields Changed'])
    st.caption(f'{len(agg_df)} employees with history')
    sel = st.dataframe(agg_df, use_container_width=True, hide_index=True,
                       on_select='rerun', selection_mode='single-row')

    if sel and sel.selection.rows:
        idx     = sel.selection.rows[0]
        ecn     = agg_df.iloc[idx]['ECN']
        emp_name = agg_df.iloc[idx]['Employee']

        @st.dialog(f'📋 History — {emp_name or ecn}', width='large')
        def _history_modal():
            hist_df = get_employee_history(ecn)
            if hist_df.empty:
                st.info('No detailed records.'); return

            st.caption(f'{len(hist_df)} record(s) — select a row to edit or delete')

            # Fast dataframe display instead of slow per-row containers
            sel_hist = st.dataframe(
                hist_df,
                use_container_width=True,
                hide_index=True,
                on_select='rerun',
                selection_mode='single-row'
            )

            if sel_hist and sel_hist.selection.rows:
                h_idx = sel_hist.selection.rows[0]
                row = hist_df.iloc[h_idx]
                rec_id = int(row['ID'])

                st.divider()
                st.markdown(
                    f"**Editing:** `{row['Field']}` &nbsp;|&nbsp; "
                    f"`{row['Start']}` → `{row['End']}`"
                )

                with st.form(key=f'hist_edit_form_{rec_id}'):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        nv = st.text_input('Value', value=row['Value'])
                    with c2:
                        ns = st.text_input('Start Date', value=row['Start'])
                    with c3:
                        ne = st.text_input('End Date', value=row['End'])

                    st.markdown(f"Previous value: `{row['Previous'] or '(blank)'}`")

                    confirm_del = st.checkbox('Confirm deletion', key=f'confirm_del_{rec_id}')

                    c_save, c_del, c_spacer = st.columns([1, 1, 3])
                    with c_save:
                        save_btn = st.form_submit_button('💾 Save', type='primary')
                    with c_del:
                        del_btn = st.form_submit_button('🗑️ Delete')

                # Handle actions outside the form for clean rerun behaviour
                if save_btn:
                    ok, msg = update_history_record(rec_id, nv, ns, ne)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

                if del_btn:
                    if confirm_del:
                        ok, msg = delete_history_record(rec_id)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.warning('Check the confirmation box to delete')
            else:
                st.info('Select a record from the table above to edit or delete.')

        _history_modal()

    st.divider()
    if st.button('🧹  Remove Redundant Records', type='secondary'):
        with st.spinner('Cleaning…'):
            deleted = compact_history()
        st.success(f'Removed **{deleted}** redundant record(s)')

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DB TOOLS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == 'dbtools':
    page_header('DB Tools', 'Database maintenance and diagnostics')
    if not db_connected():
        st.error('Connect TiDB first.'); st.stop()

    emp_count, active_count, hist_count = get_db_stats()

    if emp_count is not None:
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(stat_card('Total Employees', f'{emp_count:,}',  '👥'), unsafe_allow_html=True)
        with c2: st.markdown(stat_card('Active',          f'{active_count:,}','✅', '#22c55e'), unsafe_allow_html=True)
        with c3: st.markdown(stat_card('History Records', f'{hist_count:,}',  '📋', 'var(--brand-orange)'), unsafe_allow_html=True)
    else:
        st.error('Could not fetch database stats.')

    section_label('Maintenance')
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.markdown('**🧹 Compact History**')
            st.caption('Removes records where `value == prev_value` (no actual change).')
            if st.button('Run Compact', type='primary', key='db_compact'):
                with st.spinner('Compacting…'):
                    deleted = compact_history()
                st.success(f'Removed **{deleted}** redundant entries')
    with col2:
        with st.container(border=True):
            st.markdown('**🔄 Clear Cache**')
            st.caption('Forces a fresh reload of all data from TiDB on next page load.')
            if st.button('Clear Cache', type='secondary', key='db_cache'):
                st.cache_data.clear()
                st.toast('Cache cleared!', icon='✅')
                st.rerun()

    section_label('Recent Uploads')
    engine = get_engine()
    if engine:
        try:
            with engine.connect() as conn:
                logs = conn.execute(
                    text('SELECT * FROM upload_log ORDER BY upload_date DESC LIMIT 15')
                ).fetchall()
            if logs:
                logs_df = pd.DataFrame(logs,
                    columns=['ID','Date','Rows Processed','Inserted','Updated','Skipped (Manual)'])
                st.dataframe(logs_df, use_container_width=True, hide_index=True)
            else:
                st.info('No uploads logged yet.')
        except Exception as e:
            st.error(f'Could not load upload log: {e}')
