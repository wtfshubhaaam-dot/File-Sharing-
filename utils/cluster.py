"""
utils/cluster.py - ONE bot, MANY servers (e.g. several free Koyeb accounts) sharing ONE MongoDB.

How it works
  * Telegram can send updates to only one place, so one server is the GATEWAY (NODE_ROLE=gateway).
    Telegram -> gateway (/tg/<secret>) -> the gateway looks at the user and forwards the update to the
    least-loaded server (/internal/update). That server handles it completely (checks, files, Stream/Download
    links on ITS OWN domain, ads), so file traffic is spread over all servers.
  * Every server writes a heartbeat to MongoDB every 10 s: active users, running tasks, streams, CPU, RAM.
  * A user is pinned to a server while he is active (ACTIVE_WINDOW seconds without a request = inactive).
    A new / returning user goes to the server with the lowest load score:
        40 % active users vs capacity + 20 % running tasks + 15 % streams + 15 % CPU + 10 % RAM
    Full or overheated servers are skipped while another one is free. If ALL are full, the least loaded one still
    takes the user (an "overflow" is counted and the owner is told to add a server).
  * A dead server (no heartbeat / forward failed) is skipped; its users move to another server automatically.
  * Admins, channel posts and chat-member updates always run on the gateway (the admin panel keeps its state there).
  * Background jobs (auto-delete, autoposter, backup) run on the gateway only.
  * The gateway samples the cluster every 15 s -> hourly peak stats in MongoDB (shown in Panel -> Servers).

Add a server later: deploy the same code on a new Koyeb account with a new NODE_ID, NODE_ROLE=worker and its own
BASE_URL. It joins automatically after its first heartbeat; nothing else has to be changed or restarted.
"""
import asyncio
import datetime as dt
import hashlib
import hmac
import html
import logging
import math
import os
import re
import time
from collections import deque

import aiohttp
from aiohttp import web
from pymongo import UpdateOne
from telegram import Update

import config
from database import db as dbm

log = logging.getLogger(__name__)

ENABLED = bool(config.CLUSTER_SECRET and config.NODE_ID)
IS_GATEWAY = ENABLED and config.NODE_ROLE == "gateway"
WINDOW = config.ACTIVE_WINDOW
HB_EVERY = 10          # seconds between heartbeats
STALE_AFTER = 35       # seconds without heartbeat -> server counts as down
REFRESH_EVERY = 5      # gateway re-reads the server list this often
PING_EVERY = 240       # gateway pings every server (keeps free Koyeb instances awake, wakes sleeping ones)
SAMPLE_EVERY = 15      # cluster sampling for the peak statistics
FLUSH_EVERY = 60
PERSIST_EVERY = 30     # user -> server pins are saved to MongoDB (survive a gateway restart)
DOWN_FOR = 30          # a server that failed a forward is skipped for this long
KEEP_DAYS = 400
ALLOWED_UPDATES = ["message", "callback_query", "channel_post", "my_chat_member"]

_app = None            # the telegram Application (set by attach())
_tasks: set = set()
_session = None
_sem = None
_seen_ids: deque = deque(maxlen=4000)
_seen_set: set = set()


# ----------------------------------------------------------------------------- server names
_NAMES = [("⚡", "Falcon"), ("🔥", "Phoenix"), ("🌩", "Thunder"), ("🐉", "Dragon"), ("🦅", "Eagle"),
          ("🐺", "Wolf"), ("🚀", "Rocket"), ("💎", "Diamond"), ("🌊", "Tsunami"), ("🛡", "Titan")]


def _default_name(node_id: str) -> str:
    m = re.search(r"(\d+)", node_id or "")
    i = (int(m.group(1)) - 1) if m else sum(ord(c) for c in node_id or "x")
    emoji, name = _NAMES[i % len(_NAMES)]
    return f"{emoji} {name}"


NODE_LABEL = config.NODE_NAME or _default_name(config.NODE_ID)       # shown to users, e.g. "⚡ Falcon"
HOST = "Render" if os.getenv("RENDER") else ("Koyeb" if os.getenv("KOYEB_APP_NAME") else "")


def short_name(label: str) -> str:
    """Name without the emoji, for tables."""
    return re.sub(r"^[^\w]+", "", label or "").strip() or label


def _derive(label: str) -> str:
    return hashlib.sha256(f"{label}:{config.BOT_TOKEN}:{config.CLUSTER_SECRET}".encode()).hexdigest()[:48]


WEBHOOK_SECRET = _derive("tg")


class _Live:
    tasks = 0           # updates being processed on THIS server right now
    tasks_peak = 0


live = _Live()


def attach(app):
    global _app
    _app = app


def _spawn(coro):
    t = asyncio.ensure_future(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)
    return t


# ----------------------------------------------------------------------------- local metrics
def _read_cgroup_mem():
    """(used_mb, limit_mb) of the container, or None. Works with cgroup v2 and v1."""
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            limit = int(raw)
            with open("/sys/fs/cgroup/memory.current") as f:
                cur = int(f.read())
            inactive = 0
            try:
                with open("/sys/fs/cgroup/memory.stat") as f:
                    for line in f:
                        if line.startswith("inactive_file "):
                            inactive = int(line.split()[1])
                            break
            except OSError:
                pass
            return max(cur - inactive, 0) / 1048576, limit / 1048576
    except (OSError, ValueError):
        pass
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read())
        if limit < (1 << 50):
            with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f:
                cur = int(f.read())
            return cur / 1048576, limit / 1048576
    except (OSError, ValueError):
        pass
    return None


_prev = {"t": time.time(), "p": time.process_time()}
_mcache = {"t": 0.0, "v": {}}


def local_metrics() -> dict:
    """CPU / RAM / tasks / streams of THIS server (cached for 4 s, so several callers share one reading)."""
    now = time.time()
    if now - _mcache["t"] < 4 and _mcache["v"]:
        v = dict(_mcache["v"])
        v["tasks"], v["streams"] = live.tasks, _streams()
        return v
    from utils import usage
    p = time.process_time()
    proc_cpu = max(0.0, min(100.0, 100.0 * (p - _prev["p"]) / max(now - _prev["t"], 1e-3)))
    _prev["t"], _prev["p"] = now, p
    last = usage.last or {}
    cpu = max(proc_cpu, float(last.get("cpu", 0.0)))
    mem = _read_cgroup_mem()
    if mem and mem[1] > 0:
        used, total = mem
    elif last:
        used, total = float(last.get("ram_used_mb", 0)), float(last.get("ram_total_mb", 0))
    else:
        used, total = usage._read_rss_mb(), 0.0
    ram = 100.0 * used / total if total else 0.0
    v = {"cpu": round(cpu, 1), "ram": round(ram, 1), "ram_used_mb": round(used), "ram_total_mb": round(total),
         "tasks": live.tasks, "streams": _streams()}
    _mcache["t"], _mcache["v"] = now, dict(v)
    return v


def _streams() -> int:
    from utils import usage
    return usage.live.streams


def self_doc() -> dict:
    return {"url": config.BASE_URL, "name": NODE_LABEL, "host": HOST, "role": "gateway" if IS_GATEWAY else "worker",
            "cap": config.NODE_CAPACITY,
            "max_tasks": config.NODE_MAX_TASKS, "max_streams": config.NODE_MAX_STREAMS,
            "hb": time.time(), **local_metrics()}


# ----------------------------------------------------------------------------- scoring (pure functions)
def load_score(n: dict, users: int) -> float:
    cap = max(1, n.get("cap", 30))
    mt = max(1, n.get("max_tasks", 20))
    ms = max(1, n.get("max_streams", 8))
    return (0.40 * min(users / cap, 1.5) + 0.20 * min(n.get("tasks", 0) / mt, 1.5)
            + 0.15 * min(n.get("streams", 0) / ms, 1.5) + 0.15 * min(n.get("cpu", 0) / 100, 1.0)
            + 0.10 * min(n.get("ram", 0) / 100, 1.0))


def is_hot(n: dict) -> bool:
    return n.get("cpu", 0) >= 90 or n.get("ram", 0) >= 92 or n.get("tasks", 0) >= n.get("max_tasks", 20)


def choose(nodes: dict, counts: dict):
    """nodes: {id: doc} of HEALTHY servers. Returns (node id or None, overflow flag)."""
    if not nodes:
        return None, False
    free = [i for i, n in nodes.items() if counts.get(i, 0) < n.get("cap", 30) and not is_hot(n)]
    overflow = not free
    pool = free or [i for i, n in nodes.items() if not is_hot(n)] or list(nodes)
    best = min(pool, key=lambda i: (load_score(nodes[i], counts.get(i, 0)), counts.get(i, 0), i))
    return best, overflow


def hour_key(ts: float = None) -> str:
    d = dt.datetime.fromtimestamp(ts or time.time(), dbm.LIMIT_TZ)
    return d.strftime("%Y-%m-%d-%H")


# ----------------------------------------------------------------------------- the router (gateway only)
class Router:
    def __init__(self):
        self.nodes: dict = {}           # id -> heartbeat doc
        self.down_until: dict = {}      # id -> time until which it is skipped
        self.users: dict = {}           # user id -> [node id, last seen, last saved]
        self.ctr = {"assign": 0, "overflow": 0, "failover": 0}

    # ---- health / counts
    def healthy(self, nid: str, now: float) -> bool:
        n = self.nodes.get(nid)
        if not n or self.down_until.get(nid, 0) > now:
            return False
        return nid == config.NODE_ID or now - n.get("hb", 0) < STALE_AFTER

    def healthy_nodes(self, now: float) -> dict:
        return {i: n for i, n in self.nodes.items() if self.healthy(i, now)}

    def counts(self, now: float) -> dict:
        c: dict = {}
        for ent in self.users.values():
            if now - ent[1] < WINDOW:
                c[ent[0]] = c.get(ent[0], 0) + 1
        return c

    # ---- routing
    def route(self, uid: int, now: float) -> str:
        ent = self.users.get(uid)
        if ent and now - ent[1] < WINDOW and self.healthy(ent[0], now):
            ent[1] = now                                  # still active on the same server
            return ent[0]
        if ent and now - ent[1] < WINDOW:                 # its server died -> move it
            self.ctr["failover"] += 1
        nid, overflow = choose(self.healthy_nodes(now), self.counts(now))
        nid = nid or config.NODE_ID                       # nothing healthy: the gateway itself takes it
        self.users[uid] = [nid, now, 0.0]
        self.ctr["assign"] += 1
        if overflow:
            self.ctr["overflow"] += 1
        return nid

    def mark_down(self, nid: str):
        if nid != config.NODE_ID:
            self.down_until[nid] = time.time() + DOWN_FOR
            self.ctr["failover"] += 1

    def target(self, data: dict) -> str:
        """Which server must process this Telegram update?"""
        uid, kind = _update_user(data)
        if uid is None or kind not in ("message", "callback_query") or _is_admin(uid):
            return config.NODE_ID
        return self.route(uid, time.time())

    # ---- refresh from MongoDB
    async def refresh(self):
        docs = await dbm.cl_nodes_col.find({}).to_list(None)
        nodes = {d["_id"]: d for d in docs}
        me = self_doc()
        nodes[config.NODE_ID] = {**nodes.get(config.NODE_ID, {}), **me}
        self.nodes = nodes

    async def restore(self):
        try:
            cur = dbm.cl_assign_col.find({"seen": {"$gte": time.time() - WINDOW}})
            async for d in cur:
                self.users[d["_id"]] = [d["node"], d["seen"], d["seen"]]
        except Exception:
            log.warning("Could not restore user -> server pins", exc_info=True)

    async def persist(self, now: float):
        ops = []
        for uid, ent in self.users.items():
            if ent[1] - ent[2] >= PERSIST_EVERY - 1 and now - ent[1] < WINDOW:
                ops.append(UpdateOne({"_id": uid}, {"$set": {
                    "node": ent[0], "seen": ent[1],
                    "expire_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)}}, upsert=True))
                ent[2] = ent[1]
        for i in range(0, len(ops), 500):
            await dbm.cl_assign_col.bulk_write(ops[i:i + 500], ordered=False)
        for uid in [u for u, e in self.users.items() if now - e[1] > WINDOW * 6]:   # forget long-gone users
            del self.users[uid]

    def snapshot(self) -> dict:
        """Everything the Servers screen needs, right now."""
        now = time.time()
        counts = self.counts(now)
        rows = []
        for nid, n in sorted(self.nodes.items()):
            ok = self.healthy(nid, now)
            users = counts.get(nid, 0)
            if not ok:
                state = "DOWN"
            elif users >= n.get("cap", 30):
                state = "FULL"
            elif is_hot(n):
                state = "HOT"
            else:
                state = "OK"
            rows.append({"id": nid, "name": n.get("name") or nid, "host": n.get("host", ""), "state": state, "users": users, "cap": n.get("cap", 30), "tasks": n.get("tasks", 0),
                         "streams": n.get("streams", 0), "cpu": n.get("cpu", 0), "ram": n.get("ram", 0),
                         "ram_mb": n.get("ram_used_mb", 0), "ram_total": n.get("ram_total_mb", 0),
                         "load": round(100 * min(load_score(n, users), 1.0)) if ok else 0,
                         "gateway": n.get("role") == "gateway", "age": int(now - n.get("hb", 0)), "url": n.get("url", "")})
        return {"nodes": rows, "active": sum(r["users"] for r in rows),
                "capacity": sum(r["cap"] for r in rows if r["state"] != "DOWN"),
                "up": sum(1 for r in rows if r["state"] != "DOWN")}


router = Router()


def _update_user(data: dict):
    for key in ("message", "callback_query", "edited_message", "my_chat_member", "chat_member"):
        obj = data.get(key)
        if isinstance(obj, dict) and isinstance(obj.get("from"), dict):
            if key == "message" and (obj.get("chat") or {}).get("type") not in (None, "private"):
                return obj["from"].get("id"), "group"
            return obj["from"].get("id"), key
    return None, next((k for k in data if k != "update_id"), "")


def _is_admin(uid: int) -> bool:
    from utils.helpers import is_admin
    return is_admin(uid)


# ----------------------------------------------------------------------------- processing an update on THIS server
async def process_local(data: dict):
    uid = data.get("update_id")
    if uid in _seen_set:                                   # the same update arrived twice (retry) -> once only
        return
    if uid is not None:
        if len(_seen_ids) == _seen_ids.maxlen:
            _seen_set.discard(_seen_ids[0])
        _seen_ids.append(uid)
        _seen_set.add(uid)
    update = Update.de_json(data, _app.bot)
    if update is None:
        return
    user_id, kind = _update_user(data)
    if user_id is not None and kind in ("message", "callback_query"):
        _touch(user_id)
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(256)
    live.tasks += 1
    live.tasks_peak = max(live.tasks_peak, live.tasks)
    try:
        async with _sem:
            await _app.process_update(update)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Update %s failed", uid)
    finally:
        live.tasks -= 1


# ----------------------------------------------------------------------------- "connecting" animation
_act: dict = {}          # user id -> monotonic time of his last update on THIS server
_pending: set = set()    # users who just (re)connected to this server and have not seen the animation yet


def _touch(uid: int):
    now = time.monotonic()
    last = _act.get(uid)
    _act[uid] = now
    if last is None or now - last > WINDOW:      # new here, or came back after being inactive
        _pending.add(uid)
    if len(_act) > 20000:
        for u in [u for u, t in _act.items() if now - t > WINDOW * 2]:
            del _act[u]
            _pending.discard(u)


def server_line() -> str:
    """One line for the user: which server he is connected to ('' when not in multi-server mode)."""
    return f"🖥 <b>Server:</b> {html.escape(NODE_LABEL)}" if ENABLED else ""


def _frames() -> list:
    lab = html.escape(NODE_LABEL)
    return [
        "📡 <b>Connecting…</b>\n\n▰▱▱▱▱▱▱▱▱▱ 10%\n<i>Scanning servers</i>",
        "🛰 <b>Connecting…</b>\n\n▰▰▰▱▱▱▱▱▱▱ 30%\n<i>Finding the fastest route</i>",
        "🔐 <b>Connecting…</b>\n\n▰▰▰▰▰▰▱▱▱▱ 60%\n<i>Securing the connection</i>",
        f"⚡ <b>Connecting…</b>\n\n▰▰▰▰▰▰▰▰▱▱ 85%\n<i>Syncing with {lab}</i>",
        f"✅ <b>Connected</b>\n\n🖥 Server: <b>{lab}</b>\n📶 Status: Online · Secure\n\n▰▰▰▰▰▰▰▰▰▰ 100%",
    ]


async def _delete_later(bot, chat_id: int, message_id: int, after: float):
    await asyncio.sleep(after)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def connect_animation(bot, chat_id: int):
    """Shown once when a user connects (or reconnects) to this server. Never blocks or breaks the file delivery."""
    if not ENABLED or chat_id not in _pending:
        return
    _pending.discard(chat_id)
    if _is_admin(chat_id):
        return
    from telegram.constants import ParseMode
    frames = _frames()
    try:
        m = await bot.send_message(chat_id, frames[0], parse_mode=ParseMode.HTML)
        for f in frames[1:]:
            await asyncio.sleep(0.6)
            await bot.edit_message_text(f, chat_id=chat_id, message_id=m.message_id, parse_mode=ParseMode.HTML)
        _spawn(_delete_later(bot, chat_id, m.message_id, 8))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.debug("Connect animation skipped: %s", type(e).__name__)


def _http():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20, connect=10),
            connector=aiohttp.TCPConnector(limit=200, keepalive_timeout=60))
    return _session


async def forward(nid: str, data: dict) -> bool:
    url = (router.nodes.get(nid) or {}).get("url")
    if not url:
        return False
    try:
        async with _http().post(f"{url}/internal/update", json=data,
                                headers={"X-Cluster-Secret": config.CLUSTER_SECRET}) as r:
            if r.status == 200:
                return True
            log.warning("Server %s answered HTTP %s", nid, r.status)
    except Exception as e:
        log.warning("Forward to %s failed: %s", nid, type(e).__name__)
    return False


async def dispatch(data: dict):
    """Gateway: send the update to the server chosen for this user (with failover)."""
    try:
        nid = router.target(data)
        for _ in range(3):
            if nid == config.NODE_ID:
                break
            if await forward(nid, data):
                return
            router.mark_down(nid)
            nid = router.target(data)                      # the dead server is skipped now
    except Exception:
        log.exception("Routing failed - handling the update locally")
    await process_local(data)


# ----------------------------------------------------------------------------- HTTP routes
def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8", "ignore"), b.encode("utf-8", "ignore"))


async def tg_webhook(request: web.Request):
    if (request.match_info["secret"] != WEBHOOK_SECRET
            or not _same(request.headers.get("X-Telegram-Bot-Api-Secret-Token", ""), WEBHOOK_SECRET)):
        raise web.HTTPForbidden()
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()
    _spawn(dispatch(data))
    return web.Response(text="ok")                         # answer Telegram at once; processing continues


async def internal_update(request: web.Request):
    if not _same(request.headers.get("X-Cluster-Secret", ""), config.CLUSTER_SECRET):
        raise web.HTTPForbidden()
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()
    _spawn(process_local(data))
    return web.Response(text="ok")


def setup_routes(app: web.Application):
    if not ENABLED:
        return
    app.router.add_post("/internal/update", internal_update)
    if IS_GATEWAY:
        app.router.add_post("/tg/{secret}", tg_webhook)


# ----------------------------------------------------------------------------- background loops
async def beat():
    await dbm.cl_nodes_col.update_one({"_id": config.NODE_ID}, {"$set": {**self_doc(), "tasks_peak": live.tasks_peak}},
                                      upsert=True)
    live.tasks_peak = live.tasks


async def heartbeat_loop():
    n = 0
    while True:
        try:
            await beat()
            n += 1
            if not IS_GATEWAY and n % 3 == 0:              # workers pick up panel changes made on the gateway
                await dbm.settings.load()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Heartbeat failed: %s", type(e).__name__)
        await asyncio.sleep(HB_EVERY)


async def _ping_all():
    for nid, n in list(router.nodes.items()):
        if nid == config.NODE_ID or not n.get("url"):
            continue
        try:
            async with _http().get(f"{n['url']}/health", timeout=aiohttp.ClientTimeout(total=30)) as r:
                await r.read()
        except Exception:
            pass


async def router_loop():
    """Gateway: keep the server list fresh, keep the other servers awake."""
    await router.restore()
    last_ping = 0.0
    while True:
        try:
            await router.refresh()
            if time.time() - last_ping >= PING_EVERY:
                last_ping = time.time()
                _spawn(_ping_all())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Router refresh failed: %s", type(e).__name__)
        await asyncio.sleep(REFRESH_EVERY)


def _new_acc(key: str) -> dict:
    return {"key": key, "samples": 0, "sum": 0, "peak": 0, "cap": 0, "up": 0, "n": {}}


async def _flush(acc: dict):
    if not acc["samples"]:
        return
    c, router.ctr = router.ctr, {"assign": 0, "overflow": 0, "failover": 0}
    mx = {"peak_active": acc["peak"], "cap_total": acc["cap"], "nodes_up": acc["up"]}
    for nid, v in acc["n"].items():
        for k, val in v.items():
            mx[f"n.{nid}.{k}"] = val
    try:
        await dbm.cl_stats_col.update_one(
            {"_id": acc["key"]},
            {"$inc": {"samples": acc["samples"], "active_sum": acc["sum"], **c}, "$max": mx,
             "$set": {"expire_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=KEEP_DAYS)}}, upsert=True)
    except Exception:
        for k, v in c.items():                              # keep the counters for the next flush
            router.ctr[k] += v
        log.warning("Could not save cluster stats", exc_info=True)
    acc.update(samples=0, sum=0, peak=0, cap=0, up=0, n={})


async def stats_loop(bot):
    """Gateway: peak statistics + owner warning when every server is (almost) full."""
    from utils.helpers import notify_owner
    acc = _new_acc(hour_key())
    last_flush = last_persist = time.time()
    try:
        while True:
            await asyncio.sleep(SAMPLE_EVERY)
            try:
                now = time.time()
                key = hour_key(now)
                if key != acc["key"]:
                    await _flush(acc)
                    acc = _new_acc(key)
                snap = router.snapshot()
                acc["samples"] += 1
                acc["sum"] += snap["active"]
                acc["peak"] = max(acc["peak"], snap["active"])
                acc["cap"] = max(acc["cap"], snap["capacity"])
                acc["up"] = max(acc["up"], snap["up"])
                for r in snap["nodes"]:
                    if r["state"] == "DOWN":
                        continue
                    cur = acc["n"].setdefault(r["id"], {})
                    for k, v in (("users", r["users"]), ("tasks", r["tasks"]), ("streams", r["streams"]),
                                 ("cpu", r["cpu"]), ("ram", r["ram"])):
                        cur[k] = max(cur.get(k, 0), v)
                if snap["capacity"] and snap["active"] >= 0.9 * snap["capacity"]:
                    await notify_owner(bot, "cluster_full",
                                       f"⚠️ Servers are almost full: {snap['active']}/{snap['capacity']} active users. "
                                       "Add one more server (see Panel → Servers → Scaling).", every=1800)
                if now - last_flush >= FLUSH_EVERY:
                    await _flush(acc)
                    last_flush = now
                if now - last_persist >= PERSIST_EVERY:
                    await router.persist(now)
                    last_persist = now
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Cluster sampling failed")
    except asyncio.CancelledError:
        await _flush(acc)
        raise


async def _set_webhook(bot):
    if not config.BASE_URL.startswith("https://"):
        log.error("Gateway needs BASE_URL = its public https URL, otherwise Telegram cannot reach it.")
        return
    url = f"{config.BASE_URL}/tg/{WEBHOOK_SECRET}"
    for attempt in range(6):
        try:
            await bot.set_webhook(url=url, secret_token=WEBHOOK_SECRET, allowed_updates=ALLOWED_UPDATES,
                                  max_connections=100, drop_pending_updates=False)
            log.info("Telegram webhook set -> %s/tg/***", config.BASE_URL)
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("set_webhook failed (%s), retrying", type(e).__name__)
            await asyncio.sleep(5 * (attempt + 1))


async def start(app) -> list:
    """Called from main.post_init when ENABLED. Returns the background tasks."""
    await beat()                                            # appear in the server list at once
    tasks = [asyncio.create_task(heartbeat_loop())]
    if IS_GATEWAY:
        await router.refresh()
        tasks.append(asyncio.create_task(router_loop()))
        tasks.append(asyncio.create_task(stats_loop(app.bot)))
        tasks.append(asyncio.create_task(_set_webhook(app.bot)))
    log.info("Cluster mode: node %s (%s), capacity %s users", config.NODE_ID,
             "GATEWAY" if IS_GATEWAY else "worker", config.NODE_CAPACITY)
    return tasks


async def shutdown():
    try:                                                    # tell the gateway at once that this server is leaving
        await dbm.cl_nodes_col.update_one({"_id": config.NODE_ID}, {"$set": {"hb": 0}})
    except Exception:
        pass
    if _session and not _session.closed:
        await _session.close()


# ----------------------------------------------------------------------------- reading the statistics (panel)
async def load_hours(since: dt.date) -> list:
    try:
        cur = dbm.cl_stats_col.find({"_id": {"$gte": since.strftime("%Y-%m-%d") + "-00"}}).sort("_id", 1)
        return await cur.to_list(None)
    except Exception:
        log.exception("Could not read cluster stats")
        return []


def servers_needed(peak: int, cap_per_node: int, headroom: float = 1.25) -> int:
    return max(1, math.ceil(peak * headroom / max(cap_per_node, 1)))
