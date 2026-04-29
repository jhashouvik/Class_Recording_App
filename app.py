import json
import os
import re
import socket
import math
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, unquote
#this is test 
import requests
import streamlit as st
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2 import service_account

load_dotenv()

st.set_page_config(page_title="DevOps Recordings", page_icon="🎥", layout="wide")

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

# Proxy is only viable when a local credentials file exists (i.e. running on a developer machine).
# On Streamlit Cloud there is no local filesystem key, so use_proxy will be False at runtime.
def _proxy_available() -> bool:
    path = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        or os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    )
    return bool(path) and os.path.isfile(path)


def inject_app_css(dark: bool = False) -> None:
    bg         = "#0f172a"   if dark else "#f8fafc"
    surface    = "#1e293b"   if dark else "#ffffff"
    surface2   = "#162032"   if dark else "#f8fafc"
    border     = "#334155"   if dark else "#e2e8f0"
    text_pri   = "#f1f5f9"   if dark else "#0f172a"
    text_sec   = "#94a3b8"   if dark else "#64748b"
    accent     = "#22d3ee"   if dark else "#0891b2"
    accent_dk  = "#06b6d4"   if dark else "#0e7490"
    accent_lt  = "#0e4f6e"   if dark else "#ecfeff"
    accent_lt2 = "#155e75"   if dark else "#cffafe"
    chip_bg    = "#1e3a4f"   if dark else "#ecfeff"
    chip_brd   = "#22d3ee55" if dark else "#a5f3fc"
    chip_sel   = "#22d3ee"   if dark else "#0891b2"
    chip_text  = "#7dd3fc"   if dark else "#0e7490"
    pill_bg    = "#1e293b"   if dark else "#f1f5f9"
    pill_brd   = "#334155"   if dark else "#e2e8f0"
    pill_text  = "#cbd5e1"   if dark else "#334155"
    hero_from  = "#060d18"   if dark else "#0f172a"
    hero_to    = "#0e4f6e"
    body_bg    = "#0a1628"   if dark else "#f0f4f8"
    expander_bg= "#1a2744"   if dark else "#fafbfc"

    st.markdown(
        f"""
<style>
    /* ── Root background ── */
    .stApp, .main, [data-testid="stAppViewContainer"] {{
        background: {body_bg} !important;
    }}
    [data-testid="stSidebar"] {{
        background: {surface2} !important;
        border-right: 1px solid {border} !important;
    }}
    [data-testid="stSidebar"] * {{
        color: {text_pri} !important;
    }}

    /* ── Layout ── */
    .block-container {{
        padding-top: 0.75rem !important;
        max-width: 1380px !important;
    }}

    /* ── Hero banner ── */
    .hero-banner {{
        background: linear-gradient(135deg, {hero_from} 0%, {hero_to} 60%, #0891b2 100%);
        border-radius: 20px;
        padding: 1.6rem 2rem 1.4rem 2rem;
        margin-bottom: 1.25rem;
        position: relative;
        overflow: hidden;
    }}
    .hero-banner::before {{
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse at 80% 50%, rgba(8,145,178,0.35) 0%, transparent 65%);
        pointer-events: none;
    }}
    /* ── Floating particles ── */
    @keyframes float-up {{
        0%   {{ transform: translateY(0) scale(1); opacity: 0.25; }}
        50%  {{ opacity: 0.55; }}
        100% {{ transform: translateY(-80px) scale(1.3); opacity: 0; }}
    }}
    .hero-particle {{
        position: absolute;
        border-radius: 50%;
        background: rgba(255,255,255,0.18);
        animation: float-up linear infinite;
        pointer-events: none;
    }}
    .hero-title {{
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -0.04em;
        color: #f0f9ff !important;
        margin: 0 0 0.3rem 0;
    }}
    .hero-sub {{
        color: #bae6fd;
        font-size: 0.92rem;
        margin: 0;
    }}
    .hero-badge {{
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: rgba(255,255,255,0.12);
        border: 1px solid rgba(255,255,255,0.2);
        border-radius: 999px;
        padding: 0.28rem 0.8rem;
        font-size: 0.78rem;
        color: #e0f2fe;
        font-weight: 600;
        margin-top: 0.7rem;
    }}
    .dark-toggle {{
        position: absolute;
        top: 1.1rem; right: 1.2rem;
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.25);
        border-radius: 999px;
        padding: 0.3rem 0.85rem;
        font-size: 0.8rem;
        color: #e0f2fe;
        font-weight: 700;
        cursor: pointer;
        backdrop-filter: blur(6px);
        transition: background 0.2s;
        z-index: 10;
    }}
    .dark-toggle:hover {{ background: rgba(255,255,255,0.28); }}

    /* ── Stats bar ── */
    @keyframes count-up {{
        from {{ transform: translateY(12px); opacity: 0; }}
        to   {{ transform: translateY(0);    opacity: 1; }}
    }}
    .stats-bar {{
        display: flex;
        gap: 1rem;
        margin-bottom: 1.1rem;
        flex-wrap: wrap;
    }}
    .stat-card {{
        flex: 1;
        min-width: 120px;
        background: {surface};
        border: 1px solid {border};
        border-radius: 14px;
        padding: 0.75rem 1rem;
        text-align: center;
        transition: transform 0.18s ease, box-shadow 0.18s ease;
        animation: count-up 0.5s ease both;
    }}
    .stat-card:nth-child(1) {{ animation-delay: 0.05s; }}
    .stat-card:nth-child(2) {{ animation-delay: 0.12s; }}
    .stat-card:nth-child(3) {{ animation-delay: 0.19s; }}
    .stat-card:nth-child(4) {{ animation-delay: 0.26s; }}
    .stat-card:hover {{
        transform: translateY(-5px) scale(1.03);
        box-shadow: 0 12px 32px rgba(8,145,178,0.18);
    }}
    .stat-number {{
        font-size: 1.7rem;
        font-weight: 800;
        color: {accent};
        line-height: 1;
    }}
    .stat-label {{
        font-size: 0.72rem;
        color: {text_sec};
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 0.2rem;
    }}

    /* ── Topic filter chips ── */
    .chips-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
        margin: 0.6rem 0 0.9rem 0;
        align-items: center;
    }}
    .chip {{
        display: inline-flex;
        align-items: center;
        gap: 0.28rem;
        background: {chip_bg};
        border: 1.5px solid {chip_brd};
        border-radius: 999px;
        padding: 0.28rem 0.75rem;
        font-size: 0.78rem;
        color: {chip_text};
        font-weight: 600;
        cursor: pointer;
        transition: background 0.18s, border-color 0.18s, transform 0.15s, box-shadow 0.18s;
        user-select: none;
    }}
    .chip:hover {{
        background: {accent_dk}22;
        border-color: {accent};
        transform: translateY(-2px);
        box-shadow: 0 4px 12px {accent}33;
    }}
    .chip.active {{
        background: {chip_sel};
        border-color: {chip_sel};
        color: #fff;
        box-shadow: 0 4px 16px {chip_sel}55;
        transform: translateY(-1px);
    }}
    .chips-label {{
        font-size: 0.76rem;
        font-weight: 700;
        color: {text_sec};
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-right: 0.25rem;
    }}

    /* ── Progress donut ── */
    .progress-ring-wrap {{
        display: flex;
        align-items: center;
        gap: 1rem;
        background: {surface};
        border: 1px solid {border};
        border-radius: 16px;
        padding: 0.85rem 1.1rem;
        margin-bottom: 0.9rem;
    }}
    .progress-ring-svg {{ flex-shrink: 0; }}
    .progress-ring-bg {{ fill: none; stroke: {border}; stroke-width: 6; }}
    .progress-ring-fill {{
        fill: none;
        stroke: {accent};
        stroke-width: 6;
        stroke-linecap: round;
        transform-origin: 50% 50%;
        transform: rotate(-90deg);
        transition: stroke-dashoffset 0.8s cubic-bezier(0.4,0,0.2,1);
    }}
    .progress-ring-text {{
        font-size: 0.72rem;
        font-weight: 700;
        fill: {text_pri};
    }}
    .progress-ring-pct {{
        font-size: 1rem;
        font-weight: 800;
        fill: {accent};
    }}
    .ring-detail {{}}
    .ring-detail-title {{
        font-size: 0.84rem;
        font-weight: 700;
        color: {text_pri};
        margin: 0 0 0.2rem 0;
    }}
    .ring-detail-sub {{
        font-size: 0.75rem;
        color: {text_sec};
        margin: 0;
    }}
    .ring-streak {{
        margin-top: 0.4rem;
        font-size: 0.73rem;
        font-weight: 700;
        color: #f59e0b;
    }}

    /* ── Search bar ── */
    [data-testid="stTextInput"] label {{
        font-weight: 700 !important;
        color: {text_pri} !important;
        font-size: 0.85rem !important;
        letter-spacing: 0.03em !important;
        text-transform: uppercase !important;
    }}
    [data-testid="stTextInput"] [data-baseweb="input"] {{
        border-radius: 14px !important;
        border-color: {border} !important;
        background: {surface} !important;
        color: {text_pri} !important;
        transition: border-color 0.2s, box-shadow 0.2s, background 0.2s !important;
    }}
    [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {{
        border-color: {accent} !important;
        background: {surface} !important;
        box-shadow: 0 0 0 4px {accent}26 !important;
    }}
    [data-testid="stTextInput"] input {{
        color: {text_pri} !important;
    }}

    /* ── Sidebar library panel ── */
    .lib-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }}
    .lib-title {{
        font-size: 1rem;
        font-weight: 700;
        color: {text_pri};
    }}
    .lib-count {{
        background: {accent};
        color: #fff;
        border-radius: 999px;
        padding: 0.18rem 0.6rem;
        font-size: 0.72rem;
        font-weight: 700;
    }}
    .date-chip {{
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        color: {accent_dk};
        background: linear-gradient(135deg, {accent_lt} 0%, {accent_lt2} 100%);
        border: 1px solid {chip_brd};
        border-radius: 999px;
        padding: 0.32rem 0.8rem;
        margin: 0.85rem 0 0.45rem 0;
    }}

    /* ── Playlist radio (scoped to sidebar column only) ── */
    [data-testid="stColumn"] div[data-testid="stRadio"] {{
        margin-top: 0.2rem;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] > div:first-child {{
        display: none;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] label {{
        display: flex !important;
        align-items: center !important;
        width: 100% !important;
        border: 1.5px solid {border} !important;
        border-radius: 10px !important;
        padding: 0.58rem 0.9rem !important;
        margin-bottom: 0.28rem !important;
        background: {surface} !important;
        transition: border-color 0.18s, background 0.18s, transform 0.15s, box-shadow 0.18s !important;
        cursor: pointer !important;
        font-size: 0.84rem !important;
        color: {text_pri} !important;
        font-weight: 500 !important;
        line-height: 1.3 !important;
        border-radius: 10px !important;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] label:hover {{
        border-color: {accent} !important;
        background: {accent_lt} !important;
        transform: translateX(4px) !important;
        box-shadow: 0 3px 12px {accent}1a !important;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] label:has(input:checked) {{
        border-color: {accent} !important;
        background: linear-gradient(135deg, {accent_lt} 0%, {accent_lt2} 100%) !important;
        box-shadow: 0 0 0 3px {accent}22, inset 4px 0 0 {accent} !important;
        color: {accent_dk} !important;
        font-weight: 700 !important;
        transform: translateX(4px) !important;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] label input[type="radio"] {{
        position: absolute !important;
        width: 1px !important;
        height: 1px !important;
        opacity: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }}
    [data-testid="stColumn"] div[data-testid="stRadio"] label > div {{
        font-size: 0.84rem !important;
        line-height: 1.35 !important;
    }}
    .playlist-date-sep {{
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: {text_sec};
        padding: 0.55rem 0.2rem 0.2rem 0.2rem;
        border-bottom: 1px solid {border};
        margin-bottom: 0.18rem;
    }}

    /* ── Topic filter radio as chips (outside columns — not overridden by playlist CSS) ── */
    div[data-testid="stRadio"] {{
        margin-top: 0;
    }}
    div[data-testid="stRadio"] > div:first-child {{
        display: none !important;
    }}
    div[data-testid="stRadio"] [data-testid="stRadioOptions"] {{
        display: flex !important;
        flex-wrap: wrap !important;
        gap: 0.15rem !important;
        row-gap: 0.25rem !important;
    }}
    div[data-testid="stRadio"] label {{
        display: inline-flex !important;
        align-items: center !important;
        width: auto !important;
        background: {chip_bg} !important;
        border: 1.5px solid {chip_brd} !important;
        border-radius: 999px !important;
        padding: 0.28rem 0.85rem !important;
        margin-bottom: 0 !important;
        font-size: 0.8rem !important;
        color: {chip_text} !important;
        font-weight: 600 !important;
        cursor: pointer !important;
        transition: background 0.18s, border-color 0.18s, transform 0.15s, box-shadow 0.18s !important;
        transform: none !important;
        line-height: normal !important;
        gap: 0 !important;
    }}
    div[data-testid="stRadio"] label:hover {{
        background: {accent_dk}22 !important;
        border-color: {accent} !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px {accent}33 !important;
    }}
    div[data-testid="stRadio"] label:has(input:checked) {{
        background: {chip_sel} !important;
        border-color: {chip_sel} !important;
        color: #fff !important;
        box-shadow: 0 4px 14px {chip_sel}55 !important;
        transform: translateY(-1px) !important;
    }}
    div[data-testid="stRadio"] label input[type="radio"] {{
        position: absolute !important;
        width: 1px !important; height: 1px !important;
        opacity: 0 !important; margin: 0 !important; padding: 0 !important;
        pointer-events: none !important;
    }}
    div[data-testid="stRadio"] label > div:first-child {{
        display: none !important;
    }}

    /* ── Buttons ── */
    .stButton > button {{
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease, background 0.15s ease !important;
    }}
    .stButton > button:hover {{
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 18px rgba(15,23,42,0.14) !important;
    }}

    /* ── Now Playing badge ── */
    @keyframes pulse-ring {{
        0%   {{ box-shadow: 0 0 0 0 {accent}80; }}
        70%  {{ box-shadow: 0 0 0 10px {accent}00; }}
        100% {{ box-shadow: 0 0 0 0 {accent}00; }}
    }}
    @keyframes blink-dot {{
        0%, 100% {{ opacity: 1; transform: scale(1); }}
        50%       {{ opacity: 0.5; transform: scale(0.7); }}
    }}
    .now-playing {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        background: linear-gradient(135deg, {accent}, {accent_dk});
        color: #fff;
        border-radius: 999px;
        padding: 0.32rem 0.9rem;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        animation: pulse-ring 2s infinite;
        margin-bottom: 0.5rem;
    }}
    .now-playing-dot {{
        width: 7px;
        height: 7px;
        background: #7dd3fc;
        border-radius: 50%;
        animation: blink-dot 1.2s infinite;
    }}

    /* ── Topic title ── */
    .player-title {{
        font-size: 1.65rem;
        font-weight: 800;
        color: {text_pri};
        letter-spacing: -0.03em;
        line-height: 1.2;
        margin: 0.2rem 0 0.5rem 0;
    }}

    /* ── Metadata pill row ── */
    .meta-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-bottom: 0.9rem;
    }}
    .meta-pill {{
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        background: {pill_bg};
        border: 1px solid {pill_brd};
        border-radius: 999px;
        padding: 0.28rem 0.75rem;
        font-size: 0.78rem;
        color: {pill_text};
        font-weight: 600;
    }}
    .meta-pill span.icon {{ font-size: 0.85rem; }}

    /* ── Video player ── */
    .stVideo {{
        border-radius: 18px !important;
        overflow: hidden !important;
        border: 2px solid {border} !important;
        box-shadow: 0 24px 64px -12px rgba(15,23,42,0.22) !important;
        transition: box-shadow 0.3s ease !important;
    }}
    .stVideo:hover {{
        box-shadow: 0 32px 80px -12px {accent}44 !important;
    }}
    .stVideo video {{ border-radius: 18px !important; }}

    /* ── Drive iframe embed (16:9) ── */
    .drive-player-wrap {{
        position: relative;
        width: 100%;
        padding-bottom: 56.25%;
        height: 0;
        overflow: hidden;
        border-radius: 18px;
        border: 2px solid {border};
        box-shadow: 0 24px 64px -12px rgba(15,23,42,0.22);
        transition: box-shadow 0.3s ease;
        margin-bottom: 0.5rem;
    }}
    .drive-player-wrap:hover {{
        box-shadow: 0 32px 80px -12px {accent}44;
    }}
    .drive-player-wrap iframe {{
        position: absolute;
        top: 0; left: 0;
        width: 100%;
        height: 100%;
        border: 0;
        border-radius: 16px;
    }}

    /* ── Link buttons ── */
    [data-testid="stLinkButton"] a {{
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease !important;
    }}
    [data-testid="stLinkButton"] a:hover {{
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 18px {accent}33 !important;
    }}

    /* ── Metrics ── */
    [data-testid="stMetric"] {{
        background: {surface} !important;
        border: 1px solid {border} !important;
        border-radius: 14px !important;
        padding: 0.6rem 1rem !important;
        transition: box-shadow 0.18s !important;
    }}
    [data-testid="stMetric"]:hover {{
        box-shadow: 0 4px 14px rgba(15,23,42,0.08) !important;
    }}
    [data-testid="stMetric"] label {{ color: {text_sec} !important; }}
    [data-testid="stMetric"] [data-testid="stMetricValue"] {{ color: {text_pri} !important; font-weight: 700 !important; }}

    /* ── Expanders ── */
    div[data-testid="stExpander"] {{
        border-radius: 14px !important;
        border: 1px solid {border} !important;
        background: {expander_bg} !important;
    }}

    /* ── Search match bar ── */
    .match-bar-wrap {{ margin: 0.35rem 0 0.9rem 0; }}
    .match-bar-bg {{
        background: {border};
        border-radius: 999px;
        height: 6px;
        overflow: hidden;
    }}
    .match-bar-fill {{
        height: 100%;
        background: linear-gradient(90deg, {accent}, {accent_dk});
        border-radius: 999px;
        transition: width 0.6s cubic-bezier(0.4,0,0.2,1);
    }}
    .match-label {{ font-size: 0.72rem; color: {text_sec}; margin-bottom: 0.2rem; }}

    /* ── Action hint ── */
    .action-hint {{ font-size: 0.82rem; color: {text_sec}; margin: 0.4rem 0 0.6rem 0; }}

    /* ── Divider ── */
    .soft-divider {{
        border: none;
        border-top: 1px solid {border};
        margin: 0.9rem 0;
    }}

    /* ── Keyboard shortcut panel ── */
    .kbd-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.5rem 1.2rem;
        margin-top: 0.3rem;
    }}
    .kbd-row {{
        display: flex;
        align-items: center;
        gap: 0.5rem;
        font-size: 0.8rem;
        color: {text_sec};
    }}
    .kbd {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: {surface2};
        border: 1.5px solid {border};
        border-radius: 6px;
        padding: 0.15rem 0.45rem;
        font-size: 0.76rem;
        font-weight: 700;
        color: {text_pri};
        min-width: 1.6rem;
        box-shadow: 0 2px 0 {border};
        font-family: monospace;
    }}

    /* ── Selectbox dark ── */
    [data-testid="stSelectbox"] > div > div {{
        background: {surface} !important;
        border-color: {border} !important;
        color: {text_pri} !important;
    }}

    /* ── Toast notification ── */
    @keyframes slide-in-right {{
        from {{ transform: translateX(120px); opacity: 0; }}
        to   {{ transform: translateX(0);     opacity: 1; }}
    }}
    @keyframes slide-out-right {{
        from {{ transform: translateX(0);     opacity: 1; }}
        to   {{ transform: translateX(120px); opacity: 0; }}
    }}
    .toast {{
        position: fixed;
        bottom: 2rem;
        right: 2rem;
        z-index: 9999;
        background: {surface};
        border: 1.5px solid {accent};
        border-radius: 14px;
        padding: 0.7rem 1.2rem;
        font-size: 0.84rem;
        font-weight: 600;
        color: {text_pri};
        box-shadow: 0 12px 40px rgba(8,145,178,0.22);
        display: flex;
        align-items: center;
        gap: 0.6rem;
        animation: slide-in-right 0.35s ease, slide-out-right 0.35s ease 2.5s forwards;
    }}

    /* ── Caption / markdown text ── */
    .stMarkdown p, .stMarkdown span, .stCaption, [data-testid="stCaptionContainer"] {{
        color: {text_sec} !important;
    }}
    p {{ color: {text_pri}; }}
</style>
        """,
        unsafe_allow_html=True,
    )


def get_config(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value:
        return value

    try:
        return st.secrets[name]
    except Exception:
        return default


API_KEY = get_config("GOOGLE_API_KEY")
FOLDER_ID = get_config("GOOGLE_DRIVE_FOLDER_ID")
# Use os.getenv only so missing optional keys do not touch st.secrets (avoids "No secrets files found").
FIELDS = os.getenv(
    "GOOGLE_DRIVE_API_FIELDS",
    "files(id,name,webViewLink,createdTime,mimeType)",
)


def service_account_credentials_path() -> str:
    return (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        or os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    )


def service_account_credentials() -> Optional[service_account.Credentials]:
    # 1. st.secrets section [gcp_service_account] — works on Streamlit Cloud and local secrets.toml
    try:
        sa_info = st.secrets.get("gcp_service_account")
        if sa_info:
            return service_account.Credentials.from_service_account_info(
                dict(sa_info),
                scopes=[DRIVE_READONLY_SCOPE],
            )
    except Exception:
        pass

    # 2. Local JSON file via env var — works in local dev
    path = service_account_credentials_path()
    if not path or not os.path.isfile(path):
        return None
    return service_account.Credentials.from_service_account_file(
        path,
        scopes=[DRIVE_READONLY_SCOPE],
    )


def drive_auth_headers_and_params(
    api_key: str,
) -> Tuple[Optional[Dict[str, str]], Dict]:
    """
    Private Drive folders require OAuth (e.g. service account). API keys only work for public data.
    """
    creds = service_account_credentials()
    if creds:
        creds.refresh(Request())
        return {"Authorization": f"Bearer {creds.token}"}, {}
    if api_key:
        return None, {"key": api_key}
    return None, {}

MONTH_MAP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def clean_filename(name: str) -> str:
    name = re.sub(r"\.mp4$", "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", name).strip()


def extract_drive_file_id(url: str) -> Optional[str]:
    match = re.search(r"/d/([^/]+)", url or "")
    return match.group(1) if match else None


def drive_preview_url(web_view_link: str) -> str:
    file_id = extract_drive_file_id(web_view_link)
    return f"https://drive.google.com/file/d/{file_id}/preview" if file_id else web_view_link


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@st.cache_resource
def drive_video_proxy_port(api_key: str) -> int:
    """
    Local HTTP proxy so the browser can use Range requests against Drive `alt=media`.
    Playback starts after the first chunk (fast), without downloading the whole file.
    """
    port = _find_free_port()

    def make_handler(key: str):
        class DriveVideoProxyHandler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args) -> None:
                pass

            def do_OPTIONS(self) -> None:
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Range")
                self.end_headers()

            def do_GET(self) -> None:
                if not self.path.startswith("/video/"):
                    self.send_error(404)
                    return
                raw = self.path[len("/video/") :].split("?", 1)[0]
                file_id = unquote(raw)
                if not file_id:
                    self.send_error(400)
                    return
                url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
                headers, extra_params = drive_auth_headers_and_params(key)
                params = {"alt": "media", "supportsAllDrives": "true", **extra_params}
                req_headers = dict(headers or {})
                rng = self.headers.get("Range")
                if rng:
                    req_headers["Range"] = rng
                try:
                    with requests.get(
                        url,
                        headers=req_headers,
                        params=params,
                        stream=True,
                        timeout=(60, 60 * 60),
                    ) as upstream:
                        if upstream.status_code not in (200, 206):
                            body = (upstream.text or "")[:500]
                            self.send_response(502)
                            self.send_header("Content-Type", "text/plain; charset=utf-8")
                            self.end_headers()
                            self.wfile.write(
                                f"Drive error {upstream.status_code}: {body}".encode("utf-8", errors="replace")
                            )
                            return
                        self.send_response(upstream.status_code)
                        for h in (
                            "Content-Type",
                            "Content-Length",
                            "Content-Range",
                            "Accept-Ranges",
                            "Cache-Control",
                        ):
                            if h in upstream.headers:
                                self.send_header(h, upstream.headers[h])
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        for chunk in upstream.iter_content(chunk_size=64 * 1024):
                            if not chunk:
                                continue
                            try:
                                self.wfile.write(chunk)
                            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                                return
                            except OSError as exc:
                                # Windows: 10053 aborted, 10054 reset — client closed (seek, tab switch, rerun)
                                if getattr(exc, "winerror", None) in (10053, 10054):
                                    return
                                raise
                except requests.RequestException as exc:
                    self.send_response(502)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(str(exc).encode("utf-8", errors="replace")[:500])

        return DriveVideoProxyHandler

    httpd = ThreadingHTTPServer(("127.0.0.1", port), make_handler(api_key))
    Thread(target=httpd.serve_forever, daemon=True).start()
    return port


def drive_stream_playback_url(file_id: str, api_key: str) -> str:
    port = drive_video_proxy_port(api_key)
    return f"http://127.0.0.1:{port}/video/{quote(file_id, safe='')}"


# ── Static-file video streaming ───────────────────────────────────────────────
# Streamlit's built-in static file server (Tornado) supports HTTP Range requests,
# so videos streamed to ./static/ are fully seekable in the browser with zero RAM cost.
_STATIC_VIDEO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def _get_file_size(file_id: str) -> int:
    """Return Drive file size in bytes via metadata. Returns 0 on failure."""
    try:
        headers, extra_params = drive_auth_headers_and_params(API_KEY)
        params = {"fields": "size", "supportsAllDrives": "true", **extra_params}
        resp = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            headers=headers or {},
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        return int(resp.json().get("size", 0))
    except Exception:
        return 0


def stream_video_to_static(file_id: str, progress_bar=None) -> None:
    """
    Stream Drive video to ./static/{file_id}.mp4 using the service account.
    Writes to disk in 4 MB chunks — no RAM usage regardless of file size.
    The file is then served by Streamlit's Tornado static server at /app/static/{file_id}.mp4
    which natively supports Range requests (fully seekable).
    """
    os.makedirs(_STATIC_VIDEO_DIR, exist_ok=True)
    out_path = os.path.join(_STATIC_VIDEO_DIR, f"{file_id}.mp4")

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return  # already cached on disk

    headers, extra_params = drive_auth_headers_and_params(API_KEY)
    params = {"alt": "media", "supportsAllDrives": "true", **extra_params}
    tmp_path = out_path + ".downloading"
    try:
        with requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            headers=headers or {},
            params=params,
            timeout=(60, 3600),
            stream=True,
        ) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_bar and total:
                            pct = min(downloaded / total, 1.0)
                            progress_bar.progress(
                                pct,
                                text=f"Downloading… {downloaded // (1024*1024)} MB / {total // (1024*1024)} MB",
                            )
        os.replace(tmp_path, out_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def format_topic(text: str) -> str:
    special_words = {
        "git": "GIT",
        "ddos": "DDOS",
        "ci": "CI",
        "cd": "CD",
        "vm": "VM",
        "aws": "AWS",
        "azure": "Azure",
        "docker": "Docker",
        "kubernetes": "Kubernetes",
        "linux": "Linux",
        "devops": "DevOps",
        "yt": "YT",
    }

    words = text.split()
    formatted = []

    for word in words:
        key = word.lower()
        if key in special_words:
            formatted.append(special_words[key])
        else:
            formatted.append(word.capitalize())

    return " ".join(formatted)


def month_bucket_key(date_iso: str) -> str:
    if not date_iso or len(date_iso) < 7:
        return "Unknown"
    return date_iso[:7]


def month_bucket_label(bucket_key: str) -> str:
    if bucket_key == "Unknown":
        return "Unknown month"
    try:
        return datetime.strptime(bucket_key, "%Y-%m").strftime("%b %Y")
    except Exception:
        return bucket_key


def parse_recording_name(filename: str) -> Dict[str, str]:
    """
    Expected format:
    1st_April_26_Devops_Docker_Container.mp4
    10th_March_26_Devops_Virtual_Machine_Class_Part1.mp4
    """
    cleaned = clean_filename(filename)
    parts = cleaned.split()

    default = {
        "date": "",
        "subject": "DevOps",
        "topic": cleaned,
        "title": cleaned,
    }

    if len(parts) < 5:
        return default

    day_text = re.sub(r"(st|nd|rd|th)$", "", parts[0], flags=re.IGNORECASE)
    month_text = parts[1].lower()
    year_text = parts[2]
    subject = parts[3]
    topic = " ".join(parts[4:])

    try:
        day = int(day_text)
        month = MONTH_MAP[month_text]
        year = int(f"20{year_text}" if len(year_text) == 2 else year_text)
        iso_date = datetime(year, month, day).strftime("%Y-%m-%d")
    except Exception:
        iso_date = ""

    return {
        "date": iso_date,
        "subject": format_topic(subject),
        "topic": format_topic(topic),
        "title": cleaned,
    }


@st.cache_data(ttl=300)
def fetch_drive_videos(api_key: str, folder_id: str) -> List[Dict]:
    if not folder_id:
        return []

    headers, extra_params = drive_auth_headers_and_params(api_key)
    if headers is None and "key" not in extra_params:
        return []

    all_files = []
    page_token = None

    while True:
        url = "https://www.googleapis.com/drive/v3/files"
        params = {
            "q": f"'{folder_id}' in parents and trashed = false and mimeType contains 'video/'",
            "fields": f"nextPageToken,{FIELDS}",
            "pageSize": 1000,
            "orderBy": "name",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
            **extra_params,
        }

        if page_token:
            params["pageToken"] = page_token

        response = requests.get(url, params=params, headers=headers or {}, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_files.extend(data.get("files", []))
        page_token = data.get("nextPageToken")

        if not page_token:
            break

    parsed_files = []

    for index, file in enumerate(all_files, start=1):
        parsed = parse_recording_name(file.get("name", ""))
        parsed_files.append(
            {
                "id": file.get("id", str(index)),
                "file_name": file.get("name", ""),
                "drive_link": file.get("webViewLink", ""),
                "created_time": file.get("createdTime", ""),
                **parsed,
            }
        )

    parsed_files.sort(key=lambda x: (x.get("date", ""), x.get("file_name", "")), reverse=True)
    return parsed_files


def load_youtube_videos() -> List[Dict]:
    """Load manually curated YouTube video entries from youtube_videos.json."""
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "youtube_videos.json")
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            entries = json.load(f)
    except Exception:
        return []
    results = []
    for entry in entries:
        file_name = entry.get("file_name", "")
        yt_url = entry.get("youtube_url", "")
        yt_id = yt_url.split("v=")[-1].split("&")[0] if "v=" in yt_url else ""
        parsed = parse_recording_name(file_name)
        results.append({
            "id": f"yt_{yt_id}",
            "file_name": file_name,
            "drive_link": "",
            "youtube_url": yt_url,
            "youtube_id": yt_id,
            "created_time": entry.get("created_time", ""),
            "source": "youtube",
            **parsed,
        })
    return results


if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

if "topic_filter" not in st.session_state:
    st.session_state.topic_filter = "All"

inject_app_css(dark=st.session_state.dark_mode)

# ── Password gate ─────────────────────────────────────────────────────────────
# Set APP_PASSWORD in Streamlit Cloud Secrets (or .streamlit/secrets.toml locally).
# If no password is configured the gate is skipped (open access).
_APP_PASSWORD = ""
try:
    _APP_PASSWORD = str(st.secrets.get("APP_PASSWORD", "")).strip()
except Exception:
    pass

if _APP_PASSWORD:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.markdown(
            """
<div style="max-width:400px;margin:6rem auto 0 auto;padding:2.5rem 2rem;background:#fff;
border:1.5px solid #e2e8f0;border-radius:20px;box-shadow:0 20px 60px rgba(15,23,42,0.12);text-align:center">
    <div style="font-size:2.5rem;margin-bottom:0.5rem">🔐</div>
    <h2 style="font-size:1.4rem;font-weight:800;color:#0f172a;margin:0 0 0.4rem 0">DevOps Recordings</h2>
    <p style="color:#64748b;font-size:0.88rem;margin:0 0 1.4rem 0">Enter the class password to continue</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            pwd = st.text_input("Password", type="password", label_visibility="collapsed",
                                placeholder="Enter password…")
            if st.button("Unlock 🔓", use_container_width=True, type="primary"):
                if pwd == _APP_PASSWORD:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Incorrect password. Please try again.")
        st.stop()

# ── End password gate ──────────────────────────────────────────────────────────

dark_icon = "☀️ Light" if st.session_state.dark_mode else "🌙 Dark"
hero_col, toggle_col = st.columns([5, 1])
with toggle_col:
    if st.button(dark_icon, key="dark_mode_toggle", use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

st.markdown(
    """
<div class="hero-banner">
    <span class="hero-particle" style="width:18px;height:18px;left:8%;bottom:10%;animation-duration:6s;animation-delay:0s;"></span>
    <span class="hero-particle" style="width:10px;height:10px;left:20%;bottom:5%;animation-duration:4.5s;animation-delay:1s;"></span>
    <span class="hero-particle" style="width:14px;height:14px;left:50%;bottom:8%;animation-duration:5s;animation-delay:0.5s;"></span>
    <span class="hero-particle" style="width:8px;height:8px;left:70%;bottom:12%;animation-duration:7s;animation-delay:2s;"></span>
    <span class="hero-particle" style="width:20px;height:20px;left:85%;bottom:4%;animation-duration:5.5s;animation-delay:1.5s;"></span>
    <span class="hero-particle" style="width:12px;height:12px;left:35%;bottom:15%;animation-duration:8s;animation-delay:0.8s;"></span>
    <div class="hero-title">🎥 DevOps Class Recordings</div>
    <p class="hero-sub">Auto-pulled from your Google Drive folder · grouped by date · streams instantly</p>
    <div class="hero-badge">🟢 Live from Drive</div>
</div>
    """,
    unsafe_allow_html=True,
)

has_sa = service_account_credentials() is not None
if not FOLDER_ID:
    st.error("Missing GOOGLE_DRIVE_FOLDER_ID.")
    st.stop()
if not has_sa and not API_KEY:
    st.error(
        "Missing authentication. For a **private** folder you need a Google **service account** JSON key; "
        "an API key alone cannot list your files (403)."
    )
    st.markdown(
        "1. In Google Cloud Console, create a service account and download its JSON key.\n"
        "2. Set `GOOGLE_APPLICATION_CREDENTIALS` to the full path of that file (or add it to `.env`).\n"
        "3. In Google Drive, **share your recordings folder** with the service account email "
        "(from the JSON `client_email` field) as **Viewer**."
    )
    st.code(
        "set GOOGLE_APPLICATION_CREDENTIALS=D:\\path\\to\\service-account.json\n"
        "set GOOGLE_DRIVE_FOLDER_ID=your_folder_id\n"
        "streamlit run app.py",
        language="powershell",
    )
    st.stop()

try:
    recordings = fetch_drive_videos(API_KEY, FOLDER_ID)
except requests.HTTPError as exc:
    err_body = ""
    try:
        err_body = exc.response.json().get("error", {}).get("message", "")
    except Exception:
        pass
    if exc.response.status_code == 403:
        api_disabled = (
            "has not been used" in err_body
            or "is disabled" in err_body.lower()
            or "SERVICE_DISABLED" in err_body
        )
        if api_disabled:
            st.error("The **Google Drive API** is not enabled for your Cloud project (or changes are still propagating).")
            proj = re.search(r"project[=\s]+(\d+)", err_body)
            enable_url = (
                f"https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project={proj.group(1)}"
                if proj
                else "https://console.cloud.google.com/apis/library/drive.googleapis.com"
            )
            st.markdown(f"1. Open **[Enable Google Drive API]({enable_url})** and click **Enable**.")
            st.markdown("2. Wait **2–5 minutes**, then refresh this app.")
            if err_body:
                st.caption(err_body)
        elif (
            "are blocked" in err_body.lower()
            or "DriveFiles.List" in err_body
            or ("drive method" in err_body.lower() and "blocked" in err_body.lower())
        ):
            st.error(
                "Google is **blocking** `DriveFiles.List` for this project — usually **Google Workspace / "
                "organization policy**, not folder sharing."
            )
            if err_body:
                st.caption(err_body)
            st.markdown(
                "**If this is a school or work Google account (Workspace):**\n"
                "- A **Google Workspace admin** may need to allow Drive API access for your Cloud project or "
                "trusted apps (Admin console → Security → Access and data control → API controls).\n\n"
                "**If you use Google Cloud under a company / org:**\n"
                "- An org admin may need to check **Organization policies** (IAM & Admin) for restrictions on "
                "`drive.googleapis.com` or service accounts.\n\n"
                "**Things to try:**\n"
                "- Create the Cloud project and service account under a **personal** Google account (no org), "
                "enable Drive API, share the folder with that service account’s `client_email`.\n"
                "- Or use **OAuth “Sign in with Google”** (user credentials) instead of a service account — "
                "that path is not blocked the same way by some org policies."
            )
        else:
            st.error("Google Drive returned **403 Forbidden** — the app cannot read that folder.")
            if err_body:
                st.caption(err_body)
            st.markdown(
                "- If you use a **service account**: open the folder in Drive → Share → add the "
                "**client_email** from your JSON key with Viewer access.\n"
                "- If you only use an **API key**: it works only for **public** content; private folders need a service account."
            )
    else:
        st.error(f"Google Drive API error: {exc}")
    st.stop()
except Exception as exc:
    st.error(f"Could not load recordings: {exc}")
    st.stop()

recordings = sorted(
    recordings + load_youtube_videos(),
    key=lambda x: (x.get("date", ""), x.get("file_name", "")),
    reverse=True,
)

if not recordings:
    st.warning("No video files found in the selected Google Drive folder.")
    st.stop()

# ── Stats bar ──────────────────────────────────────────────────────────────
all_dates = sorted({r["date"] for r in recordings if r["date"]}, reverse=True)
all_topics = sorted({r["topic"] for r in recordings})
latest_date = datetime.strptime(all_dates[0], "%Y-%m-%d").strftime("%d %b %Y") if all_dates else "—"

stats_html = f"""
<div class="stats-bar">
    <div class="stat-card">
        <div class="stat-number">{len(recordings)}</div>
        <div class="stat-label">Total Recordings</div>
    </div>
    <div class="stat-card">
        <div class="stat-number">{len(all_dates)}</div>
        <div class="stat-label">Class Days</div>
    </div>
    <div class="stat-card">
        <div class="stat-number">{len(all_topics)}</div>
        <div class="stat-label">Topics Covered</div>
    </div>
    <div class="stat-card">
        <div class="stat-number" style="font-size:1rem;padding-top:0.2rem">{latest_date}</div>
        <div class="stat-label">Latest Class</div>
    </div>
</div>
"""
st.markdown(stats_html, unsafe_allow_html=True)

# ── Search + sort controls ─────────────────────────────────────────────────
ctrl_l, ctrl_r = st.columns([3, 1])
with ctrl_l:
    search = st.text_input(
        "🔍  Search Recordings",
        placeholder="Type a topic, keyword, or date…",
        help="Filters by topic name or filename.",
    )
with ctrl_r:
    sort_order = st.selectbox(
        "Sort by",
        ["📅 Newest First", "📅 Oldest First", "🔤 A → Z"],
        index=0,
    )

# ── Topic filter chips ─────────────────────────────────────────────────────
PREDEFINED_TOPICS = [
    "All",
    "Docker",
    "Terraform",
    "Kubernetes",
    "Virtual Network",
    "Ansible",
    "GIT",
    "CICD",
    "Board",
    "Linux",
    "Infra",
    "Virtual Machine",
    "Network",
    "Entra ID",
    "Cloud Basic",
]
top_topics = PREDEFINED_TOPICS

# Single horizontal radio styled as chips
_current_idx = top_topics.index(st.session_state.topic_filter) if st.session_state.topic_filter in top_topics else 0
_chosen_topic = st.radio(
    "",
    top_topics,
    index=_current_idx,
    horizontal=True,
    key="topic_filter_radio",
    label_visibility="collapsed",
)
if _chosen_topic != st.session_state.topic_filter:
    st.session_state.topic_filter = _chosen_topic
    st.rerun()

filtered = [
    item
    for item in recordings
    if (search.lower() in item["topic"].lower() or search.lower() in item["file_name"].lower())
    and (
        st.session_state.topic_filter == "All"
        or st.session_state.topic_filter.lower() in item["topic"].lower()
        or st.session_state.topic_filter.lower() in item["file_name"].lower()
    )
]

if sort_order == "📅 Oldest First":
    filtered = sorted(filtered, key=lambda x: (x.get("date", ""), x.get("file_name", "")))
elif sort_order == "🔤 A → Z":
    filtered = sorted(filtered, key=lambda x: x.get("topic", ""))
# "Newest First" is already the default sort from fetch_drive_videos

# ── Match progress bar ─────────────────────────────────────────────────────
if search or st.session_state.topic_filter != "All":
    pct = int(len(filtered) / max(len(recordings), 1) * 100)
    st.markdown(
        f"""
<div class="match-bar-wrap">
    <div class="match-label">Showing {len(filtered)} of {len(recordings)} recordings</div>
    <div class="match-bar-bg"><div class="match-bar-fill" style="width:{pct}%"></div></div>
</div>
        """,
        unsafe_allow_html=True,
    )

if not filtered:
    st.info("No matching recordings found. Try a different search term.")
    st.stop()

if "selected_file_id" not in st.session_state:
    st.session_state.selected_file_id = filtered[0]["id"]

if "library_page" not in st.session_state:
    st.session_state.library_page = 1

if "library_page_size" not in st.session_state:
    st.session_state.library_page_size = 25

if "last_selected_file_id_for_page_sync" not in st.session_state:
    st.session_state.last_selected_file_id_for_page_sync = st.session_state.selected_file_id

if "favorite_ids" not in st.session_state:
    st.session_state.favorite_ids = []

if "watched_ids" not in st.session_state:
    st.session_state.watched_ids = []

if "last_watched_id" not in st.session_state:
    st.session_state.last_watched_id = None

if "_toast" not in st.session_state:
    st.session_state._toast = None

if "show_kbd" not in st.session_state:
    st.session_state.show_kbd = False

# Keep favorites clean in case items disappear from Drive.
valid_ids = {item["id"] for item in recordings}
st.session_state.favorite_ids = [fid for fid in st.session_state.favorite_ids if fid in valid_ids]
st.session_state.watched_ids = [wid for wid in st.session_state.watched_ids if wid in valid_ids]
if st.session_state.last_watched_id not in valid_ids:
    st.session_state.last_watched_id = None

left, right = st.columns([1.1, 1.9])

with left:
    # Header row
    st.markdown(
        f"""
<div class="lib-header">
    <span class="lib-title">📚 Library</span>
    <span class="lib-count">{len(filtered)}</span>
</div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("🔄 Refresh from Drive", key="btn_refresh_drive", use_container_width=True):
        fetch_drive_videos.clear()
        drive_video_proxy_port.clear()
        st.rerun()

    if st.session_state.last_watched_id in valid_ids:
        if st.button("⏯ Resume Last Watched", key="btn_resume_last_watched", use_container_width=True):
            st.session_state.selected_file_id = st.session_state.last_watched_id
            st.rerun()

    # Month-wise grouping for easier browsing at scale.
    month_counts: Dict[str, int] = {}
    for item in filtered:
        bucket = month_bucket_key(item.get("date", ""))
        month_counts[bucket] = month_counts.get(bucket, 0) + 1

    month_keys_known = sorted((k for k in month_counts if k != "Unknown"), reverse=True)
    month_options = ["All months"]
    month_option_to_key: Dict[str, str] = {}
    for month_key in month_keys_known + (["Unknown"] if "Unknown" in month_counts else []):
        option = f"{month_bucket_label(month_key)} ({month_counts[month_key]})"
        month_options.append(option)
        month_option_to_key[option] = month_key

    month_selected = st.selectbox("Month group", month_options, key="library_month_filter")
    if month_selected == "All months":
        library_filtered = filtered
    else:
        selected_month_key = month_option_to_key[month_selected]
        library_filtered = [
            item for item in filtered if month_bucket_key(item.get("date", "")) == selected_month_key
        ]

    if not library_filtered:
        st.info("No recordings available for this month filter.")
        st.stop()

    recordings_by_id = {item["id"]: item for item in recordings}
    favorite_items = [
        recordings_by_id[fid]
        for fid in st.session_state.favorite_ids
        if fid in recordings_by_id
    ]
    if favorite_items:
        with st.expander(f"⭐ Pinned classes ({len(favorite_items)})", expanded=True):
            for fav in favorite_items:
                fav_date = ""
                if fav.get("date"):
                    try:
                        fav_date = datetime.strptime(fav["date"], "%Y-%m-%d").strftime("%d %b '%y")
                    except Exception:
                        pass
                watched_prefix = "✅ " if fav["id"] in st.session_state.watched_ids else ""
                fav_label = f"{watched_prefix}{fav['topic']}{'  ·  ' + fav_date if fav_date else ''}"
                if st.button(fav_label, key=f"fav_open_{fav['id']}", use_container_width=True):
                    st.session_state.selected_file_id = fav["id"]
                    st.rerun()

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    page_size_options = [15, 25, 50, 100]
    if st.session_state.library_page_size not in page_size_options:
        st.session_state.library_page_size = 25
    page_size = st.selectbox(
        "Classes per page",
        page_size_options,
        key="library_page_size",
    )

    total_pages = max(1, math.ceil(len(library_filtered) / page_size))
    st.session_state.library_page = max(1, min(st.session_state.library_page, total_pages))

    selected_idx_all = next(
        (i for i, item in enumerate(library_filtered) if item["id"] == st.session_state.selected_file_id),
        0,
    )
    selected_page = (selected_idx_all // page_size) + 1
    if st.session_state.last_selected_file_id_for_page_sync != st.session_state.selected_file_id:
        st.session_state.library_page = selected_page
    st.session_state.last_selected_file_id_for_page_sync = st.session_state.selected_file_id

    page_col_l, page_col_r = st.columns(2)
    with page_col_l:
        if st.button("◀ Page", key="btn_prev_page", use_container_width=True, disabled=st.session_state.library_page <= 1):
            st.session_state.library_page -= 1
            st.rerun()
    with page_col_r:
        if st.button(
            "Page ▶",
            key="btn_next_page",
            use_container_width=True,
            disabled=st.session_state.library_page >= total_pages,
        ):
            st.session_state.library_page += 1
            st.rerun()

    st.caption(f"Page {st.session_state.library_page} of {total_pages}")
    st.caption(f"Watched: {len(st.session_state.watched_ids)} / {len(recordings)}")

    start_idx = (st.session_state.library_page - 1) * page_size
    end_idx = start_idx + page_size
    visible_items = library_filtered[start_idx:end_idx]

    # Build flat playlist labels (topic + short date), deduplicating identical labels
    playlist_labels: List[str] = []
    playlist_ids: List[str] = []
    seen_labels: Dict[str, int] = {}
    for item in visible_items:
        date_short = ""
        if item["date"]:
            try:
                date_short = datetime.strptime(item["date"], "%Y-%m-%d").strftime("%d %b '%y")
            except Exception:
                pass
        watched_prefix = "✅ " if item["id"] in st.session_state.watched_ids else ""
        base = f"{watched_prefix}{item['topic']}{'  ·  ' + date_short if date_short else ''}"
        if base in seen_labels:
            seen_labels[base] += 1
            label = f"{base} ({seen_labels[base]})"
        else:
            seen_labels[base] = 1
            label = base
        playlist_labels.append(label)
        playlist_ids.append(item["id"])

    if not playlist_ids:
        st.info("No recordings available on this page.")
        st.stop()

    current_idx = 0
    if st.session_state.selected_file_id in playlist_ids:
        current_idx = playlist_ids.index(st.session_state.selected_file_id)

    chosen = st.radio(
        "playlist",
        range(len(playlist_labels)),
        format_func=lambda i: playlist_labels[i],
        index=current_idx,
        label_visibility="collapsed",
    )

    if chosen is not None and playlist_ids[chosen] != st.session_state.selected_file_id:
        st.session_state.selected_file_id = playlist_ids[chosen]
        st.rerun()

selected = next((x for x in library_filtered if x["id"] == st.session_state.selected_file_id), None)

if not selected:
    selected = library_filtered[0]
    st.session_state.selected_file_id = selected["id"]

# Find the 1-based position of the selected item in filtered list
selected_index = next((i + 1 for i, x in enumerate(library_filtered) if x["id"] == selected["id"]), 1)

with right:
    # Now Playing badge
    st.markdown(
        '<div class="now-playing"><span class="now-playing-dot"></span> NOW PLAYING</div>',
        unsafe_allow_html=True,
    )

    # Title
    st.markdown(f'<div class="player-title">{selected["topic"]}</div>', unsafe_allow_html=True)

    # Metadata pill row
    created_fmt = ""
    if selected.get("created_time"):
        try:
            created_fmt = datetime.fromisoformat(
                selected["created_time"].replace("Z", "+00:00")
            ).strftime("%d %b %Y, %I:%M %p")
        except Exception:
            created_fmt = selected["created_time"]

    meta_html = f"""
<div class="meta-row">
    <span class="meta-pill"><span class="icon">📂</span> {selected["subject"]}</span>
    <span class="meta-pill"><span class="icon">📅</span> {selected["date"] or "Unknown date"}</span>
    <span class="meta-pill"><span class="icon">🎬</span> Recording #{selected_index} of {len(library_filtered)}</span>
    {"<span class='meta-pill'><span class='icon'>🕒</span> Uploaded " + created_fmt + "</span>" if created_fmt else ""}
</div>
    """
    st.markdown(meta_html, unsafe_allow_html=True)

    preview_url = "" if selected.get("source") == "youtube" else drive_preview_url(selected["drive_link"])
    file_id = selected["id"]

    # Initialise play-request state
    if "play_requested_id" not in st.session_state:
        st.session_state.play_requested_id = None

    # Reset play state when user switches recording
    if st.session_state.play_requested_id != file_id:
        st.session_state.play_requested_id = None

    if selected.get("source") == "youtube":
        yt_url = selected.get("youtube_url", "")
        yt_id = selected.get("youtube_id", "")
        yt_start = 0
        if "t=" in yt_url:
            try:
                yt_start = int(yt_url.split("t=")[-1].rstrip("s"))
            except Exception:
                yt_start = 0
        embed_url = f"https://www.youtube.com/embed/{yt_id}?start={yt_start}&rel=0"
        st.markdown(
            f'<div class="drive-player-wrap"><iframe src="{embed_url}" allowfullscreen allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"></iframe></div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<p class="action-hint">▶ YouTube video — <a href="{yt_url}" target="_blank">Open on YouTube ↗</a></p>',
            unsafe_allow_html=True,
        )
    elif _proxy_available():
        # Local dev: stream via range-proxy (fast, no full download)
        try:
            stream_url = drive_stream_playback_url(file_id, API_KEY)
            st.video(stream_url)
            st.markdown(
                '<p class="action-hint">⚡ Streams via local proxy (HTTP Range) — fast start.</p>',
                unsafe_allow_html=True,
            )
        except Exception as exc:
            st.error(f"Could not start playback stream: {exc}")
    elif service_account_credentials() is not None:
        # Cloud + service account:
        # Stream video to ./static/ on disk (no RAM limit), serve via Tornado static file
        # server which supports Range requests → fully seekable, any file size.
        if st.session_state.play_requested_id == file_id:
            pb = st.progress(0, text="Starting download…")
            try:
                stream_video_to_static(file_id, progress_bar=pb)
                pb.empty()
                # Build the full public URL — st.video() needs http(s):// to treat it
                # as a network URL (not a local file path). Tornado serves ./static/ at
                # /app/static/ with Range request support → fully seekable, any size.
                try:
                    host = st.context.headers.get("host", "localhost:8501")
                except Exception:
                    host = "localhost:8501"
                scheme = "https" if ("localhost" not in host and "127.0.0.1" not in host) else "http"
                video_url = f"{scheme}://{host}/app/static/{file_id}.mp4"
                st.video(video_url)
                st.markdown(
                    '<p class="action-hint">⚡ Served securely via service account — fully seekable, no Google sign-in needed.</p>',
                    unsafe_allow_html=True,
                )
            except Exception as exc:
                pb.empty()
                st.error(f"Playback error: {exc}")
                st.session_state.play_requested_id = None
        else:
            st.markdown(
                f'<div class="drive-player-wrap"><iframe src="{preview_url}" allowfullscreen></iframe></div>',
                unsafe_allow_html=True,
            )
            cached = os.path.exists(
                os.path.join(_STATIC_VIDEO_DIR, f"{file_id}.mp4")
            )
            btn_label = "▶ Play Video (cached ⚡)" if cached else "▶ Play Video"
            if st.button(btn_label, key=f"play_btn_{file_id}", type="primary", use_container_width=True):
                st.session_state.play_requested_id = file_id
                st.rerun()
            if not cached:
                file_size = _get_file_size(file_id)
                size_mb = file_size // (1024 * 1024) if file_size else 0
                st.caption(
                    f"Click Play to download & stream securely ({size_mb} MB). "
                    "First load takes a minute — subsequent plays are instant."
                )
    else:
        # No credentials at all — iframe only
        st.markdown(
            f'<div class="drive-player-wrap"><iframe src="{preview_url}" allowfullscreen></iframe></div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p class="action-hint">▶ Embedded Drive player — sign in with your Google account if prompted.</p>',
            unsafe_allow_html=True,
        )

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    # Navigation buttons
    nav_prev, nav_next = st.columns(2)
    current_pos = next((i for i, x in enumerate(library_filtered) if x["id"] == selected["id"]), 0)
    with nav_prev:
        if current_pos > 0:
            if st.button("⬅ Previous", key="btn_prev", use_container_width=True):
                st.session_state.selected_file_id = library_filtered[current_pos - 1]["id"]
                st.rerun()
        else:
            st.button("⬅ Previous", key="btn_prev", use_container_width=True, disabled=True)
    with nav_next:
        if current_pos < len(library_filtered) - 1:
            if st.button("Next ➡", key="btn_next", use_container_width=True):
                st.session_state.selected_file_id = library_filtered[current_pos + 1]["id"]
                st.rerun()
        else:
            st.button("Next ➡", key="btn_next", use_container_width=True, disabled=True)

    with st.expander("📋 File details"):
        st.json(
            {
                "file_name": selected["file_name"],
                "topic": selected["topic"],
                "subject": selected["subject"],
                "date": selected["date"],
                "created_time": selected.get("created_time", ""),
            }
        )

with st.expander("Expected file naming format"):
    st.code(
        "1st_April_26_Devops_Docker_Container.mp4\n"
        "10th_March_26_Devops_Virtual_Machine_Class_Part1.mp4",
        language="text",
    )