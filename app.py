import os
import re
import socket
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import requests
import streamlit as st
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2 import service_account

load_dotenv()

st.set_page_config(page_title="DevOps Recordings", page_icon="🎥", layout="wide")

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"


def inject_app_css() -> None:
    st.markdown(
        """
<style>
    .block-container {
        padding-top: 1.25rem !important;
        max-width: 1280px !important;
    }
    h1 {
        font-weight: 700 !important;
        letter-spacing: -0.03em !important;
        color: #0f172a !important;
        margin-bottom: 0.25rem !important;
    }
    .hero-caption {
        color: #64748b;
        font-size: 0.95rem;
        margin-top: 0;
        margin-bottom: 1rem;
    }
    [data-testid="stTextInput"] label {
        font-weight: 600 !important;
        color: #334155 !important;
    }
    [data-testid="stTextInput"] input {
        border-radius: 12px !important;
    }
    [data-testid="stTextInput"] [data-baseweb="input"] {
        border-radius: 12px !important;
        border-color: #e2e8f0 !important;
        transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
    }
    [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {
        border-color: #0891b2 !important;
        box-shadow: 0 0 0 3px rgba(8, 145, 178, 0.12) !important;
    }
    .date-chip {
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #0e7490;
        background: linear-gradient(135deg, #ecfeff 0%, #cffafe 100%);
        border: 1px solid #a5f3fc;
        border-radius: 999px;
        padding: 0.35rem 0.75rem;
        margin: 0.75rem 0 0.5rem 0;
    }
    .stButton > button {
        border-radius: 12px !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease !important;
        font-weight: 500 !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.12) !important;
    }
    [data-testid="stMetric"] {
        background: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 12px !important;
        padding: 0.5rem 0.75rem !important;
    }
    [data-testid="stMetric"] label {
        color: #64748b !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #0f172a !important;
    }
    .stVideo {
        border-radius: 16px !important;
        overflow: hidden !important;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 20px 50px -12px rgba(15, 23, 42, 0.18) !important;
    }
    .stVideo video {
        border-radius: 16px !important;
    }
    [data-testid="stLinkButton"] a {
        border-radius: 12px !important;
        transition: transform 0.15s ease, box-shadow 0.15s ease !important;
    }
    [data-testid="stLinkButton"] a:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 14px rgba(8, 145, 178, 0.2) !important;
    }
    .action-hint {
        font-size: 0.85rem;
        color: #64748b;
        margin: 0.5rem 0 0.75rem 0;
    }
    div[data-testid="stExpander"] {
        border-radius: 12px !important;
        border: 1px solid #e2e8f0 !important;
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
st.title("🎥 DevOps Class Recordings")
st.markdown(
    '<p class="hero-caption">Auto-pulled from your Google Drive folder and grouped by date.</p>',
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

search = st.text_input(
    "Search recordings",
    placeholder="Type a topic or file name…",
    help="Filters the list by topic or filename.",
)

filtered = [
    item
    for item in recordings
    if search.lower() in item["topic"].lower()
    or search.lower() in item["file_name"].lower()
]

if not filtered:
    st.info("No matching recordings found.")
    st.stop()

if "selected_file_id" not in st.session_state:
    st.session_state.selected_file_id = filtered[0]["id"]

left, right = st.columns([1.1, 1.9])

with left:
    st.markdown("##### 📚 Library")
    st.caption(f"{len(filtered)} recording(s)")

    if st.button("🔄 Refresh from Drive", key="btn_refresh_drive", use_container_width=True):
        fetch_drive_videos.clear()
        drive_video_proxy_port.clear()
        st.rerun()

    grouped: Dict[str, List[Dict]] = {}
    for item in filtered:
        grouped.setdefault(item["date"] or "Unknown Date", []).append(item)

    for date_key, items in grouped.items():
        display_date = date_key
        if date_key != "Unknown Date":
            display_date = datetime.strptime(date_key, "%Y-%m-%d").strftime("%d %b %Y")

        st.markdown(f'<div class="date-chip">{display_date}</div>', unsafe_allow_html=True)

        for item in items:
            button_label = item["topic"]
            is_selected = item["id"] == st.session_state.selected_file_id
            btn_kwargs: Dict = {
                "key": f"btn_{item['id']}",
                "use_container_width": True,
            }
            if is_selected:
                btn_kwargs["type"] = "primary"
            if st.button(button_label, **btn_kwargs):
                st.session_state.selected_file_id = item["id"]

selected = next((x for x in filtered if x["id"] == st.session_state.selected_file_id), None)

if not selected:
    selected = filtered[0]
    st.session_state.selected_file_id = selected["id"]

with right:
    st.markdown(f"### {selected['topic']}")
    st.markdown(
        f'<p class="action-hint">📄 {selected["file_name"]}</p>',
        unsafe_allow_html=True,
    )

    m1, m2 = st.columns(2)
    m1.metric("Subject", selected["subject"])
    m2.metric("Date", selected["date"] or "Unknown")

    preview_url = drive_preview_url(selected["drive_link"])
    file_id = selected["id"]

    can_stream = service_account_credentials() is not None or bool(API_KEY)
    if not can_stream:
        st.caption("Add a service account JSON (or API key for public files) for in-app playback.")
        st.components.v1.iframe(preview_url, height=560, scrolling=False)
    else:
        try:
            stream_url = drive_stream_playback_url(file_id, API_KEY)
            st.video(stream_url)
            st.markdown(
                '<p class="action-hint">Streams via local proxy (HTTP Range) — fast start, no full download.</p>',
                unsafe_allow_html=True,
            )
        except Exception as exc:
            st.error(f"Could not start playback stream: {exc}")
            st.components.v1.iframe(preview_url, height=560, scrolling=False)

    link_a, link_b = st.columns(2)
    with link_a:
        st.link_button("Open in Google Drive", selected["drive_link"], use_container_width=True)
    with link_b:
        st.link_button("Preview in new tab", preview_url, use_container_width=True)

    with st.expander("Selected file details"):
        st.write(
            {
                "file_name": selected["file_name"],
                "title": selected["title"],
                "topic": selected["topic"],
                "date": selected["date"],
                "subject": selected["subject"],
                "drive_link": selected["drive_link"],
                "created_time": selected["created_time"],
            }
        )

with st.expander("Expected file naming format"):
    st.code(
        "1st_April_26_Devops_Docker_Container.mp4\n"
        "10th_March_26_Devops_Virtual_Machine_Class_Part1.mp4",
        language="text",
    )