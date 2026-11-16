"""
config.py
Only the bare minimum lives in environment variables. Everything else
(channels, captions, timers, admins...) is configured from the Telegram admin
panel and stored in MongoDB.
"""
import os
import re
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = _required("BOT_TOKEN")
MONGO_URI = _required("MONGO_URI")
OWNER_ID = int(_required("OWNER_ID"))

# Kept for Pyrogram compatibility; python-telegram-bot does not need them.
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")

# Public https URL of this deployment, no trailing slash (e.g. https://myapp.koyeb.app). Needed for Stream/Download links.
def _auto_base_url() -> str:
    """Render and Koyeb tell the app its own public address; used only when BASE_URL is not set."""
    if os.getenv("RENDER_EXTERNAL_URL"):
        return os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if os.getenv("KOYEB_PUBLIC_DOMAIN"):
        return "https://" + os.getenv("KOYEB_PUBLIC_DOMAIN", "").strip()
    return ""


BASE_URL = (os.getenv("BASE_URL", "").strip() or _auto_base_url()).rstrip("/")

# Cloudflare Worker that makes every share link permanent (see cloudflare-worker/worker.js). Optional.
WORKER_URL = os.getenv("WORKER_URL", "").strip().rstrip("/")
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Invisible padding so message bubbles are as wide as their inline buttons (0 = off). See utils/padding.py
PAD_WIDTH = int(os.getenv("PAD_WIDTH", "34") or 0)

DB_NAME = os.getenv("DB_NAME", "filesharebot")
PORT = int(os.getenv("PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Storage quota of your MongoDB plan in MB (Atlas free M0 = 512) - used for the "storage left" bar in Stats
MONGO_LIMIT_MB = float(os.getenv("MONGO_LIMIT_MB", "512"))


# ---------------------------------------------------------------------------------------------------
# Multi-server mode (several free Koyeb accounts behind ONE bot). Leave CLUSTER_SECRET empty = single server.
# Every server runs the same code + the same MONGO_URI / BOT_TOKEN. Only these differ per server:
#   NODE_ID    short unique name, e.g. n1, n2, n3
#   NODE_ROLE  "gateway" on exactly ONE server (it receives Telegram's webhook and routes users), "worker" on the others
#   BASE_URL   that server's own public https URL
# ---------------------------------------------------------------------------------------------------
CLUSTER_SECRET = os.getenv("CLUSTER_SECRET", "").strip()                       # same long random text on every server
NODE_ID = re.sub(r"[^A-Za-z0-9_-]", "", os.getenv("NODE_ID", "").strip())[:20]
NODE_NAME = os.getenv("NODE_NAME", "").strip()[:24]                              # e.g. "🔥 Phoenix" (default: picked from NODE_ID)
NODE_ROLE = os.getenv("NODE_ROLE", "worker").strip().lower()                     # gateway | worker
NODE_CAPACITY = max(1, int(os.getenv("NODE_CAPACITY", "30") or 30))              # active users this server should carry
NODE_MAX_TASKS = max(1, int(os.getenv("NODE_MAX_TASKS", "20") or 20))            # requests being processed at the same moment
NODE_MAX_STREAMS = max(1, int(os.getenv("NODE_MAX_STREAMS", "8") or 8))          # simultaneous Stream/Download viewers
ACTIVE_WINDOW = max(30, int(os.getenv("ACTIVE_WINDOW", "300") or 300))           # seconds without a request = user is inactive
