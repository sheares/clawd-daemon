#!/usr/bin/env python3
"""
BLE Notify Daemon
Bridges localhost HTTP → AtomS3R BLE NUS (Nordic UART Service).

Endpoints:
  GET /thinking      — Claude is processing
  GET /question      — Claude is waiting for input
  GET /notify        — Claude finished (done)
  GET /clear         — reset to standby
  GET /theme/<name>  — manually set theme (testing)
  GET /tool/<name>   — show tool icon in badge slot during thinking
  GET /tool          — clear tool icon

Side effects:
  • /thinking firing >10 times in 30s → sends "dizzy" instead.
  • On BLE connect → picks a theme from today's date (SG holidays + weekends)
    and sends "theme <name>".
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import deque
from datetime import date, datetime
from pathlib import Path
from aiohttp import web, ClientSession, ClientTimeout
from bleak import BleakClient, BleakScanner

DEVICE_NAME = "AtomS3R-Notify"
NUS_RX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
PORT        = 8765

# Rapid-fire glitch
RAPID_WINDOW_S   = 30
RAPID_THRESHOLD  = 10
RAPID_COOLDOWN_S = 60

# Health probe: macOS sleep can tear down BLE silently, leaving _client.is_connected
# stuck at True. A periodic write-with-response forces the OS to detect the dead link.
HEALTH_CHECK_S    = 60
HEALTH_PROBE_TO_S = 3.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ble-notify] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_client: BleakClient | None = None
_loop:   asyncio.AbstractEventLoop | None = None
_thinking_history: deque = deque()
_last_dizzy_ts: float = 0.0
_current_theme: str = ""

# Per-session tracking. Sessions older than SESSION_TTL_S are evicted.
#   _session_seen   : sid → last-seen monotonic timestamp
#   _session_states : sid → 'T'/'W'/'D' (state char for the bar)
#   _session_slots  : sid → 0..3 (stable slot index, freed only on eviction)
_session_seen:   dict[str, float] = {}
_session_states: dict[str, str]   = {}
_session_slots:  dict[str, int]   = {}
_session_labels: dict[str, str]   = {}
_last_bar_payload:    str = ""
_last_face_state:     str = ""
_last_labels_payload: str = ""
SESSION_TTL_S = 30 * 60
MAX_SLOTS     = 8   # firmware MAX_SESSIONS in waveshare-clawd/main/main.c
LCD_WIDTH     = 240 # Waveshare 1.69" — was 128 for the old Atom target
CHAR_PX_BIG   = 6      # 5x8 font + 1px spacing (used at 1-2 slots)
CHAR_PX_TINY  = 4      # 3x5 font + 1px spacing (used at 3+ slots)
SEG_PADDING   = 2      # divider + a hair of margin
LABEL_HARD_MAX = 21    # firmware buffer cap (s_labels[4][22])


# ─── Singapore holiday calendar ──────────────────────────────────────────────
# Variable-date holidays are listed per year. Update yearly.
_FIXED_HOLIDAYS = {
    (1, 1):  "new_year",
    (5, 1):  "default",       # Labour Day — no specific theme
    (8, 9):  "national_day",
    (12, 25): "christmas",
    (12, 31): "new_year",
}

_VARIABLE_HOLIDAYS = {
    2026: {
        (2, 17): "cny",        # Chinese New Year day 1
        (2, 18): "cny",        # Chinese New Year day 2
        (3, 21): "default",    # Hari Raya Puasa
        (4, 3):  "default",    # Good Friday
        (5, 27): "default",    # Hari Raya Haji
        (5, 31): "default",    # Vesak Day
        (11, 8): "deepavali",
    },
    2027: {
        (2, 6):  "cny",
        (2, 7):  "cny",
        (3, 10): "default",
        (3, 26): "default",
        (5, 16): "default",
        (5, 21): "default",
        (10, 28): "deepavali",
    },
}


def _theme_for_date(d: date) -> str:
    key = (d.month, d.day)
    if key in _FIXED_HOLIDAYS:
        return _FIXED_HOLIDAYS[key]
    yr = _VARIABLE_HOLIDAYS.get(d.year, {})
    if key in yr:
        return yr[key]
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return "weekend"
    return "default"


# ─── BLE connection management ────────────────────────────────────────────────

def _on_disconnect(client: BleakClient) -> None:
    global _client
    log.warning("disconnected — scanning for device...")
    _client = None
    if _loop:
        asyncio.run_coroutine_threadsafe(_connect_loop(), _loop)


async def _connect_loop() -> None:
    global _client
    while True:
        try:
            log.info("scanning for '%s'...", DEVICE_NAME)
            device = await BleakScanner.find_device_by_name(DEVICE_NAME, timeout=10.0)
            if device is None:
                log.warning("not found, retrying in 5 s")
                await asyncio.sleep(5)
                continue
            client = BleakClient(device, disconnected_callback=_on_disconnect)
            await client.connect()
            _client = client
            log.info("connected to %s (%s)", DEVICE_NAME, device.address)
            # Push today's theme + current hour + last-known weather on connect
            await _push_theme()
            await _push_time()
            if _last_weather:
                await _send(f"weather {_last_weather}")
            # Force a re-broadcast of bar + labels + face so reconnects refresh state.
            global _last_bar_payload, _last_face_state, _last_labels_payload
            _last_bar_payload = ""
            _last_face_state = ""
            _last_labels_payload = ""
            await _broadcast_session_view()
            return
        except Exception as exc:
            log.error("connection failed: %s — retrying in 5 s", exc)
            await asyncio.sleep(5)


async def _send(cmd: str) -> None:
    if _client is None or not _client.is_connected:
        log.warning("not connected — command dropped: %s", cmd)
        return
    try:
        await _client.write_gatt_char(NUS_RX_UUID, cmd.encode(), response=False)
        log.info("→ %s", cmd)
    except Exception as exc:
        log.error("send failed: %s", exc)


async def _push_theme() -> None:
    global _current_theme
    theme = _theme_for_date(date.today())
    _current_theme = theme
    await _send(f"theme {theme}")


def _evict_old_sessions() -> None:
    now = time.monotonic()
    stale = [sid for sid, ts in _session_seen.items() if now - ts > SESSION_TTL_S]
    for sid in stale:
        _session_seen.pop(sid, None)
        _session_states.pop(sid, None)
        _session_slots.pop(sid, None)
        _session_labels.pop(sid, None)


def _alloc_slot(sid: str) -> int | None:
    """Assign a stable 0..MAX_SLOTS-1 slot to sid. Returns None if full."""
    if sid in _session_slots:
        return _session_slots[sid]
    used = set(_session_slots.values())
    for i in range(MAX_SLOTS):
        if i not in used:
            _session_slots[sid] = i
            return i
    return None  # no free slot — session is tracked but not shown on bar


def _build_bar_payload() -> str:
    """Build the bar codes string up to the highest occupied slot index + 1.
    Empty interior slots show as '.'. Returns '' if no slots are occupied."""
    if not _session_slots:
        return ""
    highest = max(_session_slots.values())
    slots = ["."] * (highest + 1)
    for sid, idx in _session_slots.items():
        slots[idx] = _session_states.get(sid, ".")
    return "".join(slots)


def _chars_per_segment(n_slots: int) -> int:
    """How many chars fit in one bar segment when n_slots share the LCD width.
    Mirrors firmware: 1-2 slots use 5x8 font, 3+ slots switch to the tiny 3x5.
    Auto-sizes down as n_slots grows toward MAX_SLOTS (8)."""
    if n_slots < 1:
        return LABEL_HARD_MAX
    seg_w = LCD_WIDTH // n_slots
    char_px = CHAR_PX_TINY if n_slots >= 3 else CHAR_PX_BIG
    fits = (seg_w - SEG_PADDING) // char_px
    return max(2, min(LABEL_HARD_MAX, fits))


_TOKEN_SPLIT = re.compile(r"[ _\-]+")


def _fit_label(raw: str, max_chars: int) -> str:
    """Trim a label to max_chars. If it doesn't fit, prefer the longest single
    token (split on _ - space) that does; fall back to hard truncation."""
    if len(raw) <= max_chars:
        return raw
    tokens = [t for t in _TOKEN_SPLIT.split(raw) if t]
    fits = [t for t in tokens if len(t) <= max_chars]
    if fits:
        return max(fits, key=len)
    return raw[:max_chars]


def _build_labels_payload() -> str:
    """Build pipe-separated labels string aligned to bar slots.
    Per-label width depends on slot count (more slots → tighter budget).
    Returns '' if no slots."""
    if not _session_slots:
        return ""
    highest = max(_session_slots.values())
    n_slots = highest + 1
    budget = _chars_per_segment(n_slots)
    slots = [""] * n_slots
    for sid, idx in _session_slots.items():
        raw = _session_labels.get(sid, "")
        slots[idx] = _fit_label(raw, budget) if raw else ""
    return "|".join(slots)


def _compute_face_state() -> str:
    """Pick the face command based on highest-urgency state across sessions.
    Priority: W (waiting) > D (done) > T (thinking) > standby."""
    states = set(_session_states.values())
    if "W" in states: return "waiting"
    if "D" in states: return "done"
    if "T" in states: return "thinking"
    return "standby"


async def _broadcast_session_view() -> None:
    """Send bar + labels + face commands if any has changed since last broadcast."""
    global _last_bar_payload, _last_face_state, _last_labels_payload
    bar = _build_bar_payload()
    labels = _build_labels_payload()
    face = _compute_face_state()
    if bar != _last_bar_payload:
        _last_bar_payload = bar
        if bar:
            await _send(f"bar {bar}")
        # No-bar case: nothing to send; firmware will fall back on next render.
    if labels != _last_labels_payload:
        _last_labels_payload = labels
        if labels:
            await _send(f"labels {labels}")
    if face != _last_face_state:
        _last_face_state = face
        await _send(face)


async def _set_session_state(
    session_id: str | None,
    state: str | None,
    label: str | None = None,
) -> None:
    """Mark a session as seen and (optionally) update its bar state and label.
    state in {'T','W','D', None}; None just refreshes the seen-time (e.g. /tool).
    label is a short repo/cwd name (5 chars max). Broadcasts if anything changed."""
    if session_id:
        _session_seen[session_id] = time.monotonic()
        if state is not None:
            _alloc_slot(session_id)
            _session_states[session_id] = state
        if label:
            new_label = label[:LABEL_HARD_MAX]
            prev_label = _session_labels.get(session_id, "")
            # Panel-swap animation: when an existing session's label actually
            # changes (e.g. user typed "change session name to X"), tell the
            # firmware to play the swap animation on that slot BEFORE the new
            # labels broadcast arrives.
            if (prev_label and prev_label != new_label
                    and session_id in _session_slots):
                slot = _session_slots[session_id]
                await _send(f"labelswap {slot}")
            _session_labels[session_id] = new_label
    _evict_old_sessions()
    await _broadcast_session_view()


async def _session_evictor() -> None:
    """Periodically evict stale sessions and re-broadcast if anything changed."""
    while True:
        await asyncio.sleep(30)
        _evict_old_sessions()
        await _broadcast_session_view()


# ─── CLI /rename → Clawd label mirror ────────────────────────────────────────
# Claude Code's built-in /rename command doesn't fire any hook we can intercept
# — but it does persist the new name to ~/.claude/sessions/<pid>.json. Polling
# those files lets us mirror /rename onto the Clawd bar with ≈CLI_WATCH_INTERVAL_S
# lag, no CLI cooperation required.
#
# Normalisation matches the (now-removed) /session skill helper: alnum + hyphen,
# up to 3 tokens, upper, ≤12 chars. This is intentionally the same rule as the
# hook-side topic detection so labels look identical regardless of which path
# set them.
#
# On daemon startup we do a priming pass that seeds the cache without pushing —
# otherwise a restart would clobber any hook-set label (e.g. keyword topic) back
# to the CLI name.

CLI_SESSIONS_DIR = Path.home() / ".claude" / "sessions"
CLI_WATCH_INTERVAL_S = 2

# sid → last-observed normalised CLI name. Separate from _session_labels so a
# manual label set elsewhere never confuses the change-detection.
_cli_name_cache: dict[str, str] = {}


def _normalise_cli_name(raw: str) -> str:
    parts = re.split(r'[ \t\n\r,;:()\[\]{}"|]+', raw)
    tokens: list[str] = []
    for p in parts:
        cleaned = re.sub(r'[^a-zA-Z0-9-]', '', p)
        if cleaned:
            tokens.append(cleaned)
        if len(tokens) >= 3:
            break
    if not tokens:
        return ""
    return "-".join(tokens).upper()[:12]


async def _cli_name_watcher() -> None:
    priming = True
    while True:
        try:
            files = list(CLI_SESSIONS_DIR.glob("*.json"))
        except OSError:
            files = []
        for f in files:
            try:
                d = json.loads(f.read_text())
            except (OSError, ValueError):
                continue
            sid = d.get("sessionId")
            name = d.get("name")
            if not sid or not name:
                continue
            normalised = _normalise_cli_name(name)
            if not normalised:
                continue
            if _cli_name_cache.get(sid) == normalised:
                continue
            _cli_name_cache[sid] = normalised
            if not priming:
                log.info("cli-watcher: sid=%s /rename → %s", sid[:8], normalised)
                await _set_session_state(sid, None, normalised)
        priming = False
        await asyncio.sleep(CLI_WATCH_INTERVAL_S)


async def _push_time() -> None:
    await _send(f"time {datetime.now().hour}")


async def _health_check_loop() -> None:
    """Probe the BLE link every HEALTH_CHECK_S; force reconnect on silent failure."""
    global _client
    while True:
        await asyncio.sleep(HEALTH_CHECK_S)
        client = _client
        if client is None:
            continue
        if not client.is_connected:
            log.warning("health: is_connected==False, kicking reconnect")
            _client = None
            asyncio.create_task(_connect_loop())
            continue
        try:
            await asyncio.wait_for(
                client.write_gatt_char(NUS_RX_UUID, b"ping", response=True),
                timeout=HEALTH_PROBE_TO_S,
            )
        except Exception as exc:
            log.warning("health probe failed (%s) — forcing reconnect", exc)
            _client = None
            try:
                await client.disconnect()
            except Exception:
                pass
            asyncio.create_task(_connect_loop())


async def _hourly_time_pusher() -> None:
    """Re-push current hour shortly after each hour ticks over."""
    while True:
        now = datetime.now()
        # Sleep until 30s after the next hour boundary, so the firmware
        # crosses mood bands close to but slightly after the hour change.
        seconds_until_next_hour = 3600 - (now.minute * 60 + now.second) + 30
        await asyncio.sleep(seconds_until_next_hour)
        await _push_time()


# ─── Weather poller (Open-Meteo, Singapore) ───────────────────────────────────

# Singapore coordinates for the Open-Meteo current-weather query.
SG_LAT = 1.3521
SG_LON = 103.8198
WEATHER_POLL_S = 600   # 10 min

_last_weather: str = ""

def _wmo_to_code(wmo: int) -> str:
    """Open-Meteo WMO weather code → firmware's 6-code vocabulary."""
    if wmo == 0:                      return "clear"
    if wmo in (1, 2, 3):              return "clouds"   # mainly clear/partly/overcast
    if wmo in (45, 48):               return "fog"
    if 51 <= wmo <= 67:               return "rain"     # drizzle + rain
    if 80 <= wmo <= 82:               return "rain"     # rain showers
    if 71 <= wmo <= 77:               return "snow"
    if wmo in (85, 86):               return "snow"
    if wmo == 95 or wmo in (96, 99):  return "thunder"
    return "clouds"   # fallback


async def _fetch_weather_once() -> str | None:
    """One Open-Meteo poll → returns our 6-code string, or None on error."""
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={SG_LAT}&longitude={SG_LON}&current=weather_code")
    try:
        async with ClientSession(timeout=ClientTimeout(total=8)) as s:
            async with s.get(url) as resp:
                if resp.status != 200:
                    log.warning("weather: HTTP %d", resp.status)
                    return None
                data = await resp.json()
        wmo = int(data.get("current", {}).get("weather_code", -1))
        if wmo < 0:
            return None
        return _wmo_to_code(wmo)
    except Exception as exc:
        log.warning("weather: fetch failed: %s", exc)
        return None


async def _weather_poller() -> None:
    """Poll Open-Meteo every 10 min; push `weather <code>` on change."""
    global _last_weather
    # Initial delay so we don't race against BLE connect on startup.
    await asyncio.sleep(15)
    while True:
        code = await _fetch_weather_once()
        if code and code != _last_weather:
            _last_weather = code
            log.info("weather → %s", code)
            await _send(f"weather {code}")
        await asyncio.sleep(WEATHER_POLL_S)


# ─── Anthropic OAuth quota (drives Clawd's corner rings) ─────────────────────
# GET /api/oauth/usage returns the same numbers claude.ai settings shows.
# Auth: bearer OAuth token that Claude Code already stores locally. We try
# an env override first (easy to seed from the browser DevTools once), then
# fall back to macOS Keychain. Endpoint is undocumented but read-only and
# unmetered.

USAGE_POLL_S       = 120       # 2 min — the numbers only nudge each request
USAGE_ENV_VAR      = "ANTHROPIC_OAUTH_TOKEN"
USAGE_URL          = "https://api.anthropic.com/api/oauth/usage"
USAGE_BETA_HEADER  = "oauth-2025-04-20"
KEYCHAIN_SERVICE   = "Claude Code-credentials"
KEYCHAIN_ACCOUNT   = "root"

_last_usage: tuple[int, int] | None = None
_logged_raw_usage = False


def _read_oauth_token() -> str | None:
    """Env var first, then Keychain. Returns None if neither works."""
    env_tok = os.environ.get(USAGE_ENV_VAR, "").strip()
    if env_tok:
        return env_tok
    try:
        raw = subprocess.check_output(
            ["security", "find-generic-password",
             "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT, "-w"],
            stderr=subprocess.DEVNULL, timeout=3,
        ).decode().strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None
    # The blob may be a bare token, or JSON wrapping one. Try JSON first.
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return raw if raw else None
    # Common shapes: {"claudeAiOauth":{"accessToken":"..."}} or {"accessToken":"..."}
    for path in (("claudeAiOauth", "accessToken"),
                 ("accessToken",),
                 ("access_token",)):
        cur = obj
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok and isinstance(cur, str) and cur:
            return cur
    return None


def _extract_pct(node) -> int | None:
    """Coerce a usage node into an integer 0..100. Handles float 0..1,
    float 0..100, int, and {utilization|percent|value|used} wrappers."""
    if node is None:
        return None
    if isinstance(node, (int, float)):
        v = float(node)
        if v <= 1.0:
            v *= 100.0
        return max(0, min(100, int(round(v))))
    if isinstance(node, dict):
        for key in ("utilization", "utilisation", "percent",
                    "percent_used", "percentUsed", "used", "value"):
            if key in node:
                return _extract_pct(node[key])
    return None


async def _fetch_usage_once() -> tuple[int, int] | None:
    """One /api/oauth/usage poll → (five_hour_pct, seven_day_pct), or None."""
    global _logged_raw_usage
    tok = _read_oauth_token()
    if not tok:
        return None
    headers = {
        "Authorization": f"Bearer {tok}",
        "anthropic-beta": USAGE_BETA_HEADER,
    }
    try:
        async with ClientSession(timeout=ClientTimeout(total=8)) as s:
            async with s.get(USAGE_URL, headers=headers) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    log.warning("usage: HTTP %d — %s", resp.status, body)
                    return None
                data = await resp.json()
    except Exception as exc:
        log.warning("usage: fetch failed: %s", exc)
        return None

    # First successful response — dump raw so we can confirm the field shape.
    if not _logged_raw_usage:
        _logged_raw_usage = True
        log.info("usage: first raw response keys=%s",
                 list(data.keys()) if isinstance(data, dict) else type(data).__name__)
        log.info("usage: full raw = %s", json.dumps(data, default=str)[:600])

    five = _extract_pct(data.get("five_hour") if isinstance(data, dict) else None)
    # For weekly, prefer the aggregate "all models" bucket; fall back to seven_day.
    seven = None
    if isinstance(data, dict):
        for key in ("seven_day", "seven_day_all", "weekly", "seven_day_total"):
            if key in data:
                seven = _extract_pct(data[key])
                if seven is not None:
                    break
    if five is None and seven is None:
        return None
    return (five if five is not None else -1,
            seven if seven is not None else -1)


async def _usage_poller() -> None:
    """Poll /api/oauth/usage every 2 min; push `usage <5h> <7d>` on change."""
    global _last_usage
    await asyncio.sleep(20)   # let BLE settle before first push
    while True:
        pair = await _fetch_usage_once()
        if pair is not None and pair != _last_usage:
            _last_usage = pair
            log.info("usage → 5h=%d%% 7d=%d%%", pair[0], pair[1])
            await _send(f"usage {pair[0]} {pair[1]}")
        await asyncio.sleep(USAGE_POLL_S)


# ─── HTTP handlers ────────────────────────────────────────────────────────────

def _sid(req: web.Request) -> str | None:
    """Pull session_id from query string. Returns None if absent."""
    sid = req.query.get("session_id")
    return sid if sid else None


def _label(req: web.Request) -> str | None:
    """Pull session label from query string. Sanitised; daemon trims width
    later based on how many slots will share the bar."""
    raw = req.query.get("label", "")
    clean = "".join(c for c in raw if c.isalnum() or c in "_- ")
    clean = clean.strip()[:LABEL_HARD_MAX]
    return clean if clean else None


async def handle_thinking(req: web.Request) -> web.Response:
    global _last_dizzy_ts
    sid = _sid(req)
    now = time.monotonic()
    _thinking_history.append(now)
    # Drop entries older than the window
    while _thinking_history and _thinking_history[0] < now - RAPID_WINDOW_S:
        _thinking_history.popleft()

    if (len(_thinking_history) > RAPID_THRESHOLD
            and (now - _last_dizzy_ts) > RAPID_COOLDOWN_S):
        _last_dizzy_ts = now
        _thinking_history.clear()
        log.info("rapid-fire detected → dizzy")
        # Still update bar state so it's accurate after dizzy clears.
        await _set_session_state(sid, "T", _label(req))
        await _send("dizzy")
        return web.Response(text="OK (dizzy)\n")

    await _set_session_state(sid, "T", _label(req))
    return web.Response(text="OK\n")


async def handle_question(req: web.Request) -> web.Response:
    await _set_session_state(_sid(req), "W", _label(req))
    return web.Response(text="OK\n")


async def handle_notify(req: web.Request) -> web.Response:
    await _set_session_state(_sid(req), "D", _label(req))
    return web.Response(text="OK\n")


async def handle_clear(req: web.Request) -> web.Response:
    # Manual override — forces face to standby without touching per-session state.
    # (Per-session state is still refreshed via the seen-time path.)
    await _set_session_state(_sid(req), None, _label(req))
    await _send("standby")
    return web.Response(text="OK\n")


async def handle_end(req: web.Request) -> web.Response:
    # Called from Claude Code's SessionEnd hook. Immediately evicts the session
    # so its strip disappears from Clawd without waiting for SESSION_TTL_S.
    sid = _sid(req)
    if not sid:
        return web.Response(status=400, text="missing session_id\n")
    _session_seen.pop(sid, None)
    _session_states.pop(sid, None)
    _session_slots.pop(sid, None)
    _session_labels.pop(sid, None)
    await _broadcast_session_view()
    return web.Response(text=f"OK ended={sid}\n")


async def handle_reset(req: web.Request) -> web.Response:
    # User-triggered "reset clawd" — wipe ALL per-session state so Clawd returns
    # to env mode with an empty bar. Firmware also clears its slot_order/drag
    # state when sessions_count drops.
    n_cleared = len(_session_seen)
    _session_seen.clear()
    _session_states.clear()
    _session_slots.clear()
    _session_labels.clear()
    await _broadcast_session_view()
    await _send("standby")
    return web.Response(text=f"OK reset (cleared {n_cleared} sessions)\n")


async def handle_theme(req: web.Request) -> web.Response:
    name = req.match_info.get("name", "default")
    # Whitelist to avoid junk reaching firmware parser
    allowed = {"default", "weekend", "cny", "christmas", "national_day", "deepavali", "new_year"}
    if name not in allowed:
        return web.Response(status=400, text=f"unknown theme: {name}\n")
    await _send(f"theme {name}")
    return web.Response(text=f"OK theme={name}\n")


async def handle_snack(req: web.Request) -> web.Response:
    """Force a snack break for visual iteration.
    spec is 'kitkat'/'oreo'/'onigiri' (or 0/1/2)."""
    spec = req.match_info.get("spec", "").lower()
    table = {"kitkat": 0, "kit-kat": 0, "0": 0,
             "oreo": 1, "1": 1,
             "onigiri": 2, "2": 2}
    if spec not in table:
        return web.Response(status=400, text=f"unknown snack: {spec}\n")
    await _send(f"snack {table[spec]}")
    return web.Response(text=f"OK snack={spec}\n")


async def handle_mug(req: web.Request) -> web.Response:
    """Force a specific cup design for visual iteration.
    Spec is `<brand>` (random form) or `<brand>-<form>` (ceramic|cup), or `auto`.
    """
    spec = req.match_info.get("spec", "").lower()
    table = {
        "auto":          "-1 -1",
        "starbucks":     "-1 0",
        "luckin":        "-1 1",
        "chagee":        "-1 2",
        "starbucks-mug": "0 0",
        "luckin-mug":    "0 1",
        "chagee-mug":    "0 2",
        "starbucks-cup": "2 0",
        "luckin-cup":    "2 1",
        "chagee-cup":    "2 2",
    }
    if spec not in table:
        return web.Response(status=400, text=f"unknown mug spec: {spec}\n")
    await _send(f"mug {table[spec]}")
    return web.Response(text=f"OK mug={spec}\n")


async def handle_weather(req: web.Request) -> web.Response:
    # Manual weather override for visual iteration. Real auto-fetch from
    # Open-Meteo lives in _weather_poller (added later); this endpoint stays
    # for debugging.
    code = req.match_info.get("code", "")
    allowed = {"clear", "clouds", "rain", "thunder", "snow", "fog"}
    if code not in allowed:
        return web.Response(status=400, text=f"unknown weather: {code}\n")
    await _send(f"weather {code}")
    return web.Response(text=f"OK weather={code}\n")


async def handle_emote(req: web.Request) -> web.Response:
    # Direct emotion trigger — bypasses session state, fires `emote <name>` on the
    # firmware. Used for visual iteration on emotion designs.
    name = req.match_info.get("name", "")
    allowed = {"idle", "happy", "sleepy", "surprised", "waving",
               "sad", "angry", "confused", "dizzy", "loving", "clock",
               "thinking", "waiting", "done",
               "embarrassed", "smug", "focused", "scared", "mindblown",
               "dance",
               "nod", "shake", "stretch", "wobble", "flip",
               "butterfly", "ball", "water", "kite",
               "newspaper", "tea", "music",
               "morse",
               "bird", "snail", "ladybug"}
    if name not in allowed:
        return web.Response(status=400, text=f"unknown emote: {name}\n")
    await _send(f"emote {name}")
    return web.Response(text=f"OK emote={name}\n")


# Map raw Claude Code tool names to the short tokens the firmware parses.
# Firmware does prefix match on: edit/multiedit/read/write/bash/web/fetch/grep/glob/task/agent.
# Anything else → TOOL_OTHER (three dots).
_TOOL_ALIAS = {
    "edit": "edit",
    "multiedit": "multiedit",
    "read": "read",
    "write": "write",
    "bash": "bash",
    "bashoutput": "bash",
    "killshell": "bash",
    "grep": "grep",
    "glob": "glob",
    "webfetch": "fetch",
    "websearch": "web",
    "task": "task",
    "agent": "agent",
    "notebookedit": "edit",
}


async def handle_tool(req: web.Request) -> web.Response:
    # Tool calls just refresh seen-time; they don't change the bar state.
    sid = _sid(req)
    await _set_session_state(sid, None, _label(req))
    name = req.match_info.get("name", "").strip().lower()
    if not name:
        await _send("tool")  # clear
        return web.Response(text="OK tool=(clear)\n")
    token = _TOOL_ALIAS.get(name, "other")
    # Include the session's bar slot index so the firmware can attribute the
    # tool name to the correct session strip. Older firmware ignores the
    # leading digit and treats the whole arg as the tool name (graceful fallback).
    slot = _session_slots.get(sid) if sid else None
    if slot is not None:
        await _send(f"tool {slot} {token}")
    else:
        await _send(f"tool {token}")
    return web.Response(text=f"OK tool={token} slot={slot}\n")


async def handle_tool_clear(req: web.Request) -> web.Response:
    await _set_session_state(_sid(req), None, _label(req))
    await _send("tool")
    return web.Response(text="OK tool=(clear)\n")


async def handle_usage(req: web.Request) -> web.Response:
    """Debug override: GET /usage/<5h>/<7d>. Force-pushes the ring values so
    layout can be verified without waiting for the OAuth poller. Both args
    are ints 0..100 (or -1 to blank a ring)."""
    try:
        a = int(req.match_info.get("a", ""))
        b = int(req.match_info.get("b", ""))
    except ValueError:
        return web.Response(status=400, text="a and b must be ints -1..100\n")
    for v in (a, b):
        if v < -1 or v > 100:
            return web.Response(status=400, text="a and b must be -1..100\n")
    await _send(f"usage {a} {b}")
    return web.Response(text=f"OK usage 5h={a}% 7d={b}%\n")


async def handle_sessions(req: web.Request) -> web.Response:
    """Manual debug override: GET /sessions/<n>. Sends legacy `sessions <n>`
    plus a synthetic `bar` with n idle slots — useful for layout testing.
    Does NOT touch the real session tracker."""
    try:
        n = int(req.match_info.get("n", ""))
    except ValueError:
        return web.Response(status=400, text=f"n must be 0-{MAX_SLOTS}\n")
    if n < 0 or n > MAX_SLOTS:
        return web.Response(status=400, text=f"n must be 0-{MAX_SLOTS}\n")
    await _send(f"sessions {n}")
    if n > 0:
        await _send(f"bar {'.' * n}")
    return web.Response(text=f"OK sessions={n}\n")


async def handle_time(req: web.Request) -> web.Response:
    """Manual override: GET /time/<hh>. Firmware derives mood from hour."""
    try:
        hh = int(req.match_info.get("hh", ""))
    except ValueError:
        return web.Response(status=400, text="hh must be 0-23\n")
    if hh < 0 or hh > 23:
        return web.Response(status=400, text="hh must be 0-23\n")
    await _send(f"time {hh}")
    return web.Response(text=f"OK time={hh}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _loop
    _loop = asyncio.get_running_loop()

    # HTTP server starts FIRST so endpoints respond even while Clawd is offline.
    # BLE connect runs as a background task that retries until the device appears;
    # _send() no-ops (with a warning log) when the client isn't connected yet.
    app = web.Application()
    app.router.add_get("/thinking",     handle_thinking)
    app.router.add_get("/question",     handle_question)
    app.router.add_get("/notify",       handle_notify)
    app.router.add_get("/clear",        handle_clear)
    app.router.add_get("/end",          handle_end)
    app.router.add_get("/reset",        handle_reset)
    app.router.add_get("/theme/{name}", handle_theme)
    app.router.add_get("/emote/{name}", handle_emote)
    app.router.add_get("/weather/{code}", handle_weather)
    app.router.add_get("/mug/{spec}",     handle_mug)
    app.router.add_get("/snack/{spec}",   handle_snack)
    app.router.add_get("/tool/{name}",  handle_tool)
    app.router.add_get("/tool",         handle_tool_clear)
    app.router.add_get("/time/{hh}",    handle_time)
    app.router.add_get("/sessions/{n}", handle_sessions)
    app.router.add_get("/usage/{a}/{b}", handle_usage)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT)
    await site.start()
    log.info("HTTP bridge ready on http://127.0.0.1:%d", PORT)
    log.info("today's theme: %s", _theme_for_date(date.today()))

    asyncio.create_task(_hourly_time_pusher())
    asyncio.create_task(_session_evictor())
    asyncio.create_task(_cli_name_watcher())
    asyncio.create_task(_weather_poller())
    asyncio.create_task(_usage_poller())
    asyncio.create_task(_health_check_loop())
    asyncio.create_task(_connect_loop())

    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("stopped")
        sys.exit(0)
