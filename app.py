import os
import re
import socket
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


def inject_app_css() -> None:
    st.markdown(
        """
<style>
    /* ── Layout ── */
    .block-container {
        padding-top: 0.75rem !important;
        max-width: 1380px !important;
    }

    /* ── Hero banner ── */
    .hero-banner {
        background: linear-gradient(135deg, #0f172a 0%, #0e4f6e 60%, #0891b2 100%);
        border-radius: 20px;
        padding: 1.6rem 2rem 1.4rem 2rem;
        margin-bottom: 1.25rem;
        position: relative;
        overflow: hidden;
    }
    .hero-banner::before {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse at 80% 50%, rgba(8,145,178,0.35) 0%, transparent 65%);
        pointer-events: none;
    }
    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -0.04em;
        color: #f0f9ff !important;
        margin: 0 0 0.3rem 0;
    }
    .hero-sub {
        color: #bae6fd;
        font-size: 0.92rem;
        margin: 0;
    }
    .hero-badge {
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
    }

    /* ── Stats bar ── */
    .stats-bar {
        display: flex;
        gap: 1rem;
        margin-bottom: 1.1rem;
        flex-wrap: wrap;
    }
    .stat-card {
        flex: 1;
        min-width: 120px;
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 0.75rem 1rem;
        text-align: center;
        transition: transform 0.18s ease, box-shadow 0.18s ease;
    }
    .stat-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 24px rgba(15,23,42,0.10);
    }
    .stat-number {
        font-size: 1.7rem;
        font-weight: 800;
        color: #0891b2;
        line-height: 1;
    }
    .stat-label {
        font-size: 0.72rem;
        color: #64748b;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 0.2rem;
    }

    /* ── Search bar ── */
    [data-testid="stTextInput"] label {
        font-weight: 700 !important;
        color: #0f172a !important;
        font-size: 0.85rem !important;
        letter-spacing: 0.03em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stTextInput"] [data-baseweb="input"] {
        border-radius: 14px !important;
        border-color: #e2e8f0 !important;
        background: #f8fafc !important;
        transition: border-color 0.2s, box-shadow 0.2s, background 0.2s !important;
    }
    [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {
        border-color: #0891b2 !important;
        background: #fff !important;
        box-shadow: 0 0 0 4px rgba(8,145,178,0.15) !important;
    }

    /* ── Sidebar library panel ── */
    .lib-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }
    .lib-title {
        font-size: 1rem;
        font-weight: 700;
        color: #0f172a;
    }
    .lib-count {
        background: #0891b2;
        color: #fff;
        border-radius: 999px;
        padding: 0.18rem 0.6rem;
        font-size: 0.72rem;
        font-weight: 700;
    }
    .date-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        color: #0e7490;
        background: linear-gradient(135deg, #ecfeff 0%, #cffafe 100%);
        border: 1px solid #a5f3fc;
        border-radius: 999px;
        padding: 0.32rem 0.8rem;
        margin: 0.85rem 0 0.45rem 0;
    }

    /* ── Playlist radio ── */
    div[data-testid="stRadio"] {
        margin-top: 0.2rem;
    }
    /* Hide the widget label */
    div[data-testid="stRadio"] > div:first-child {
        display: none;
    }
    div[data-testid="stRadio"] label {
        display: flex !important;
        align-items: center !important;
        width: 100% !important;
        border: 1.5px solid #e2e8f0 !important;
        border-radius: 10px !important;
        padding: 0.58rem 0.9rem !important;
        margin-bottom: 0.28rem !important;
        background: #fff !important;
        transition: border-color 0.18s, background 0.18s, transform 0.15s, box-shadow 0.18s !important;
        cursor: pointer !important;
        font-size: 0.84rem !important;
        color: #334155 !important;
        font-weight: 500 !important;
        line-height: 1.3 !important;
    }
    div[data-testid="stRadio"] label:hover {
        border-color: #67e8f9 !important;
        background: #f0fdff !important;
        transform: translateX(4px) !important;
        box-shadow: 0 3px 12px rgba(8,145,178,0.1) !important;
    }
    div[data-testid="stRadio"] label:has(input:checked) {
        border-color: #0891b2 !important;
        background: linear-gradient(135deg, #ecfeff 0%, #ddf6fd 100%) !important;
        box-shadow: 0 0 0 3px rgba(8,145,178,0.13), inset 4px 0 0 #0891b2 !important;
        color: #0e4f6e !important;
        font-weight: 700 !important;
        transform: translateX(4px) !important;
    }
    /* Hide the radio circle dot */
    div[data-testid="stRadio"] label input[type="radio"] {
        position: absolute !important;
        width: 1px !important;
        height: 1px !important;
        opacity: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    div[data-testid="stRadio"] label > div {
        font-size: 0.84rem !important;
        line-height: 1.35 !important;
    }
    .playlist-date-sep {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #94a3b8;
        padding: 0.55rem 0.2rem 0.2rem 0.2rem;
        border-bottom: 1px solid #f1f5f9;
        margin-bottom: 0.18rem;
    }

    /* ── Buttons ── */
    .stButton > button {
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease, background 0.15s ease !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 18px rgba(15,23,42,0.14) !important;
    }

    /* ── Now Playing badge ── */
    @keyframes pulse-ring {
        0%   { box-shadow: 0 0 0 0 rgba(8,145,178,0.5); }
        70%  { box-shadow: 0 0 0 8px rgba(8,145,178,0); }
        100% { box-shadow: 0 0 0 0 rgba(8,145,178,0); }
    }
    .now-playing {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        background: linear-gradient(135deg, #0891b2, #0e7490);
        color: #fff;
        border-radius: 999px;
        padding: 0.32rem 0.9rem;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        animation: pulse-ring 2s infinite;
        margin-bottom: 0.5rem;
    }
    .now-playing-dot {
        width: 7px;
        height: 7px;
        background: #7dd3fc;
        border-radius: 50%;
        animation: pulse-ring 1.2s infinite;
    }

    /* ── Topic title ── */
    .player-title {
        font-size: 1.65rem;
        font-weight: 800;
        color: #0f172a;
        letter-spacing: -0.03em;
        line-height: 1.2;
        margin: 0.2rem 0 0.5rem 0;
    }

    /* ── Metadata pill row ── */
    .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-bottom: 0.9rem;
    }
    .meta-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        background: #f1f5f9;
        border: 1px solid #e2e8f0;
        border-radius: 999px;
        padding: 0.28rem 0.75rem;
        font-size: 0.78rem;
        color: #334155;
        font-weight: 600;
    }
    .meta-pill span.icon { font-size: 0.85rem; }

    /* ── Video player (local proxy) ── */
    .stVideo {
        border-radius: 18px !important;
        overflow: hidden !important;
        border: 2px solid #e2e8f0 !important;
        box-shadow: 0 24px 64px -12px rgba(15,23,42,0.22) !important;
        transition: box-shadow 0.3s ease !important;
    }
    .stVideo:hover {
        box-shadow: 0 32px 80px -12px rgba(8,145,178,0.28) !important;
    }
    .stVideo video { border-radius: 18px !important; }

    /* ── Responsive Drive iframe embed (16:9) ── */
    .drive-player-wrap {
        position: relative;
        width: 100%;
        padding-bottom: 56.25%;
        height: 0;
        overflow: hidden;
        border-radius: 18px;
        border: 2px solid #e2e8f0;
        box-shadow: 0 24px 64px -12px rgba(15,23,42,0.22);
        transition: box-shadow 0.3s ease;
        margin-bottom: 0.5rem;
    }
    .drive-player-wrap:hover {
        box-shadow: 0 32px 80px -12px rgba(8,145,178,0.28);
    }
    .drive-player-wrap iframe {
        position: absolute;
        top: 0; left: 0;
        width: 100%;
        height: 100%;
        border: 0;
        border-radius: 16px;
    }

    /* ── Link buttons ── */
    [data-testid="stLinkButton"] a {
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease !important;
    }
    [data-testid="stLinkButton"] a:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 18px rgba(8,145,178,0.22) !important;
    }

    /* ── Metrics ── */
    [data-testid="stMetric"] {
        background: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 14px !important;
        padding: 0.6rem 1rem !important;
        transition: box-shadow 0.18s !important;
    }
    [data-testid="stMetric"]:hover {
        box-shadow: 0 4px 14px rgba(15,23,42,0.08) !important;
    }
    [data-testid="stMetric"] label { color: #64748b !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { color: #0f172a !important; font-weight: 700 !important; }

    /* ── Expanders ── */
    div[data-testid="stExpander"] {
        border-radius: 14px !important;
        border: 1px solid #e2e8f0 !important;
        background: #fafbfc !important;
    }

    /* ── Progress bar (search match) ── */
    .match-bar-wrap {
        margin: 0.35rem 0 0.9rem 0;
    }
    .match-bar-bg {
        background: #e2e8f0;
        border-radius: 999px;
        height: 6px;
        overflow: hidden;
    }
    .match-bar-fill {
        height: 100%;
        background: linear-gradient(90deg, #0891b2, #06b6d4);
        border-radius: 999px;
        transition: width 0.4s ease;
    }
    .match-label {
        font-size: 0.72rem;
        color: #64748b;
        margin-bottom: 0.2rem;
    }

    /* ── Action hint ── */
    .action-hint {
        font-size: 0.82rem;
        color: #64748b;
        margin: 0.4rem 0 0.6rem 0;
    }

    /* ── Divider ── */
    .soft-divider {
        border: none;
        border-top: 1px solid #e2e8f0;
        margin: 0.9rem 0;
    }

    /* ════════════════════════════════════════
       MOBILE  ≤ 768 px
    ════════════════════════════════════════ */
    @media (max-width: 768px) {
        /* Tighter page padding */
        .block-container {
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            padding-top: 0.5rem !important;
        }

        /* Hero: smaller text, less padding */
        .hero-banner {
            padding: 1.1rem 1.1rem 1rem 1.1rem;
            border-radius: 14px;
            margin-bottom: 0.85rem;
        }
        .hero-title { font-size: 1.3rem; }
        .hero-sub   { font-size: 0.78rem; }

        /* Stats: 2-column grid on phone */
        .stats-bar {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.55rem;
        }
        .stat-card { min-width: unset; padding: 0.55rem 0.5rem; }
        .stat-number { font-size: 1.35rem; }
        .stat-label  { font-size: 0.65rem; }

        /* Search + sort stack vertically */
        div[data-testid="stHorizontalBlock"] > div {
            min-width: 100% !important;
        }

        /* Columns: stack player ABOVE playlist on mobile so video appears first */
        section[data-testid="stMain"] > div > div[data-testid="stVerticalBlock"]
            > div[data-testid="stHorizontalBlock"] {
            flex-direction: column-reverse !important;
        }

        /* Player title */
        .player-title { font-size: 1.15rem; }

        /* Meta pills: smaller */
        .meta-pill { font-size: 0.7rem; padding: 0.22rem 0.55rem; }

        /* Now playing badge */
        .now-playing { font-size: 0.68rem; padding: 0.25rem 0.7rem; }

        /* Playlist radio cards: bigger tap target, no slide animation */
        div[data-testid="stRadio"] label {
            padding: 0.75rem 0.9rem !important;
            font-size: 0.88rem !important;
        }
        div[data-testid="stRadio"] label:hover,
        div[data-testid="stRadio"] label:has(input:checked) {
            transform: none !important;
        }

        /* Nav buttons full-width */
        .stButton > button { font-size: 0.82rem !important; }

        /* Video player: slightly smaller radius on small screens */
        .drive-player-wrap { border-radius: 12px; }
        .drive-player-wrap iframe { border-radius: 10px; }

        /* Expanders */
        div[data-testid="stExpander"] { border-radius: 10px !important; }
    }

    /* ════════════════════════════════════════
       SMALL PHONE  ≤ 480 px
    ════════════════════════════════════════ */
    @media (max-width: 480px) {
        .hero-title { font-size: 1.1rem; }
        .hero-sub   { display: none; }  /* hide subtitle to save space */
        .player-title { font-size: 1rem; }
        .stats-bar { grid-template-columns: 1fr 1fr; gap: 0.4rem; }
        .stat-number { font-size: 1.15rem; }
        .meta-row { gap: 0.3rem; }
    }
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


@st.cache_data(ttl=1800, max_entries=2)
def fetch_video_bytes(file_id: str) -> bytes:
    """Fetch video bytes server-side using SA credentials (called only on explicit Play)."""
    creds = service_account_credentials()
    if creds is None:
        return b""
    try:
        creds.refresh(Request())
    except Exception:
        return b""
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {"alt": "media", "supportsAllDrives": "true"}
    headers = {"Authorization": f"Bearer {creds.token}"}
    try:
        with requests.get(
            url, headers=headers, params=params,
            stream=True, timeout=(30, 600)
        ) as resp:
            resp.raise_for_status()
            return resp.content
    except Exception:
        return b""


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


inject_app_css()

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

st.markdown(
    """
<div class="hero-banner">
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

filtered = [
    item
    for item in recordings
    if search.lower() in item["topic"].lower()
    or search.lower() in item["file_name"].lower()
]

if sort_order == "📅 Oldest First":
    filtered = sorted(filtered, key=lambda x: (x.get("date", ""), x.get("file_name", "")))
elif sort_order == "🔤 A → Z":
    filtered = sorted(filtered, key=lambda x: x.get("topic", ""))
# "Newest First" is already the default sort from fetch_drive_videos

# ── Match progress bar ─────────────────────────────────────────────────────
if search:
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

# play_requested_id tracks which recording the user explicitly clicked Play for.
# Keeping it separate from selected_file_id prevents auto-loading bytes on every render.
if "play_requested_id" not in st.session_state:
    st.session_state.play_requested_id = None

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

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    # Build flat playlist labels (topic + short date), deduplicating identical labels
    playlist_labels: List[str] = []
    playlist_ids: List[str] = []
    seen_labels: Dict[str, int] = {}
    for item in filtered:
        date_short = ""
        if item["date"]:
            try:
                date_short = datetime.strptime(item["date"], "%Y-%m-%d").strftime("%d %b '%y")
            except Exception:
                pass
        base = f"{item['topic']}{'  ·  ' + date_short if date_short else ''}"
        if base in seen_labels:
            seen_labels[base] += 1
            label = f"{base} ({seen_labels[base]})"
        else:
            seen_labels[base] = 1
            label = base
        playlist_labels.append(label)
        playlist_ids.append(item["id"])

    current_idx = next(
        (i for i, fid in enumerate(playlist_ids) if fid == st.session_state.selected_file_id),
        0,
    )

    chosen = st.radio(
        "playlist",
        range(len(playlist_labels)),
        format_func=lambda i: playlist_labels[i],
        index=current_idx,
        label_visibility="collapsed",
    )

    if chosen is not None and playlist_ids[chosen] != st.session_state.selected_file_id:
        st.session_state.selected_file_id = playlist_ids[chosen]
        st.session_state.play_requested_id = None   # reset so new recording shows Play button
        st.rerun()

selected = next((x for x in filtered if x["id"] == st.session_state.selected_file_id), None)

if not selected:
    selected = filtered[0]
    st.session_state.selected_file_id = selected["id"]

# Find the 1-based position of the selected item in filtered list
selected_index = next((i + 1 for i, x in enumerate(filtered) if x["id"] == selected["id"]), 1)

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
    <span class="meta-pill"><span class="icon">🎬</span> Recording #{selected_index} of {len(filtered)}</span>
    {"<span class='meta-pill'><span class='icon'>🕒</span> Uploaded " + created_fmt + "</span>" if created_fmt else ""}
</div>
    """
    st.markdown(meta_html, unsafe_allow_html=True)

    preview_url = drive_preview_url(selected["drive_link"])
    file_id = selected["id"]

    # Local dev  → fast localhost range-proxy (no download needed)
    # Streamlit Cloud → only fetch bytes after explicit Play click (prevents OOM crash)
    if _proxy_available():
        try:
            stream_url = drive_stream_playback_url(file_id, API_KEY)
            st.video(stream_url)
            st.markdown(
                '<p class="action-hint">⚡ Streams via local proxy — fast start, no full download.</p>',
                unsafe_allow_html=True,
            )
        except Exception as exc:
            st.error(f"Could not start playback stream: {exc}")
    else:
        if st.session_state.play_requested_id != file_id:
            # Show Play button — do NOT auto-load bytes
            st.markdown(
                f"""
<div style="background:#0f172a;border-radius:16px;padding:5rem 2rem;
text-align:center;margin-bottom:0.5rem;cursor:pointer;">
    <div style="font-size:3.5rem;margin-bottom:0.75rem">▶️</div>
    <div style="color:#f0f9ff;font-size:1rem;font-weight:700">{selected['topic']}</div>
    <div style="color:#94a3b8;font-size:0.8rem;margin-top:0.4rem">Click the button below to load and play</div>
</div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("▶\u2002Load & Play this recording", key="btn_play",
                         type="primary", use_container_width=True):
                st.session_state.play_requested_id = file_id
                st.rerun()
        else:
            # User explicitly requested this file — now safe to load
            with st.spinner("⏳ Loading recording from Drive…"):
                video_bytes = fetch_video_bytes(file_id)
            if video_bytes:
                st.video(video_bytes)
                st.markdown(
                    '<p class="action-hint">▶ Served via secure service account — no Google login needed.</p>',
                    unsafe_allow_html=True,
                )
            else:
                st.error("🔑 Could not load video. Check that **[gcp_service_account]** is set in Streamlit Cloud Secrets.")

    st.markdown('<hr class="soft-divider">', unsafe_allow_html=True)

    # Navigation buttons
    nav_prev, nav_next = st.columns(2)
    current_pos = next((i for i, x in enumerate(filtered) if x["id"] == selected["id"]), 0)
    with nav_prev:
        if current_pos > 0:
            if st.button("⬅ Previous", key="btn_prev", use_container_width=True):
                st.session_state.selected_file_id = filtered[current_pos - 1]["id"]
                st.session_state.play_requested_id = None
                st.rerun()
        else:
            st.button("⬅ Previous", key="btn_prev", use_container_width=True, disabled=True)
    with nav_next:
        if current_pos < len(filtered) - 1:
            if st.button("Next ➡", key="btn_next", use_container_width=True):
                st.session_state.selected_file_id = filtered[current_pos + 1]["id"]
                st.session_state.play_requested_id = None
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