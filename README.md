# AtomS3R Claude Code Notifier (BLE)

A tiny ambient display that tells you what Claude Code is doing — across
all your open Claude sessions at once. Built for M5Stack AtomS3R, drives
the 128×128 GC9107 LCD, talks to your Mac over BLE NUS (no WiFi needed,
works behind enterprise captive portals).

![architecture](https://img.shields.io/badge/ESP--IDF-v6.0.1-blue) ![transport](https://img.shields.io/badge/transport-BLE%20NUS-green) ![sessions](https://img.shields.io/badge/sessions-up%20to%204-orange)

## What it shows

- **Face (top):** the most-urgent state across all your sessions.
  Priority order: `waiting > done > thinking > standby`. So it always
  shouts the most actionable thing.
- **Bottom status bar:** one colored segment per active Claude session
  (up to 4), each labeled with the session's `cwd` basename so you can
  tell *which* terminal is in which state.
  - 🔵 blue = waiting for your input
  - 🟢 green = done
  - 🟠 orange = thinking
  - ⚫ grey = idle
  - Labels auto-size: 5×8 font with 1–2 sessions, switch to a tiny
    3×5 uppercase font at 3+ sessions so they still fit.
- **Tool icon (top-right):** which tool Claude is using right now
  (edit/read/bash/grep/web/…).
- **Time-of-day mood:** backlight dims and eyes droop late at night,
  brighten in the morning.

## Architecture

```
Claude Code hook → ble_notify_hook.sh → curl localhost:8765
                                              ↓
                                       Python daemon (bleak)
                                              ↓
                                          BLE NUS
                                              ↓
                                         AtomS3R LCD
```

## Hardware

- M5Stack AtomS3R (ESP32-S3, 8MB flash, 8MB PSRAM)
- Built-in 128×128 GC9107 LCD, LP5562 backlight, BMI270 IMU
- No WiFi required — pure BLE

## Setup

### 1. Build & flash firmware

```bash
source ~/.espressif/v6.0.1/esp-idf/export.sh
idf.py build
idf.py -p /dev/cu.usbmodemXXXX flash
```

### 2. Set up the daemon's Python environment

```bash
python3 -m venv ~/ble_notify_venv
~/ble_notify_venv/bin/pip install bleak aiohttp
```

### 3. Wire up Claude Code hooks

Add to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [{"hooks": [{"type": "command", "command": "/path/to/clawd-daemon/scripts/ble_notify_hook.sh thinking"}]}],
    "Notification":     [{"hooks": [{"type": "command", "command": "/path/to/clawd-daemon/scripts/ble_notify_hook.sh question"}]}],
    "PreToolUse":       [{"hooks": [{"type": "command", "command": "/path/to/clawd-daemon/scripts/ble_notify_hook.sh tool"}]}],
    "PostToolUse":      [{"hooks": [{"type": "command", "command": "/path/to/clawd-daemon/scripts/ble_notify_hook.sh thinking && /path/to/clawd-daemon/scripts/ble_notify_hook.sh tool-clear"}]}],
    "Stop":             [{"hooks": [{"type": "command", "command": "/path/to/clawd-daemon/scripts/ble_notify_hook.sh notify"}]}]
  }
}
```

The hook script auto-starts the daemon if it's not running.

## BLE NUS protocol

Send ASCII commands to the Nordic UART RX characteristic
(`6e400002-b5a3-f393-e0a9-e50e24dcca9e`):

| Command | Effect |
|---|---|
| `thinking` | Face → orange "thinking…" |
| `waiting` | Face → blue "your input?" |
| `done` | Face → green "DONE!" |
| `standby` / `clear` | Face → standby |
| `dizzy` | Easter egg: spinning eyes |
| `bar <codes>` | Bottom bar segments. Codes: `T`/`W`/`D`/`.` per slot. E.g. `bar TWD.` = 4 segments: thinking, waiting, done, idle |
| `labels <a>\|<b>\|...` | Per-segment labels (pipe-separated, up to 4). E.g. `labels esp\|main\|\|blog` (empty slot 2) |
| `tool <name>` | Top-right tool icon (`edit`/`read`/`bash`/`grep`/`glob`/`web`/`fetch`/`task`/`agent`/`other`) |
| `tool` | Clear tool icon |
| `theme <name>` | Top-right theme badge (`weekend`/`cny`/`christmas`/`national_day`/`deepavali`/`new_year`/`default`) |
| `time <hh>` | Sets mood band (0-23) — controls backlight + eye droop |
| `sessions <n>` | Legacy session count (now used only by daemon for debug overrides) |

## Daemon HTTP endpoints

The daemon (`scripts/ble_notify_daemon.py`) exposes a tiny HTTP bridge
on `localhost:8765`:

| Endpoint | Effect |
|---|---|
| `GET /thinking?session_id=X` | Marks session X as thinking |
| `GET /question?session_id=X` | Marks session X as waiting |
| `GET /notify?session_id=X` | Marks session X as done |
| `GET /clear?session_id=X` | Force standby (does not clear per-session state) |
| `GET /tool/<name>?session_id=X` | Tool icon (`edit`/`read`/`bash`/etc.) |
| `GET /tool?session_id=X` | Clear tool icon |
| `GET /theme/<name>` | Manual theme override |
| `GET /time/<hh>` | Manual mood override |
| `GET /sessions/<n>` | Debug: synthesize `n` idle slots |

Per-session state is tracked with a 5-minute TTL. After every event the
daemon recomputes the bar codes, labels, and face state and broadcasts
whichever changed. Slots are stable: first-seen keeps slot 0 until its
TTL expires, so colors don't shift around when sessions come and go.

Labels come from each session's `cwd` basename (sent by the hook script).
The daemon picks the longest single token (split on `_-space`) that fits
the current per-segment budget. When sessions get crowded the firmware
switches to a tiny 3×5 uppercase font to keep labels readable.

## Easter eggs

- **Konami code** on the AtomS3R button (short-short-long-long-short) → 🪩 disco mode
- **Shake** the device for ~2 seconds → dizzy spinning eyes
- **Rapid-fire prompts** (10+ /thinking in 30s) → dizzy
- **5 min idle in standby** → sleepy Zzz with dim backlight
- **3/50 blink chance** → wink left / wink right / eye-roll
- **SG holidays** auto-set theme on connect (CNY / National Day / Christmas / Deepavali / NY)

## Files

```
main/
  main.c              — state machine + screen renderers + IMU + easter eggs
  ble_nus.c/h         — NimBLE NUS server + command parser
  gc9107.c/h          — LCD driver (SPI, BGR, custom 90° rotation)
  bmi270.c/h          — IMU driver (shared I2C bus with LP5562)
  lp5562.c/h          — backlight LED driver
  font.h              — 5×7 pixel font
scripts/
  ble_notify_daemon.py — HTTP-to-BLE bridge + session tracker
  ble_notify_hook.sh   — Claude Code hook wrapper (extracts session_id)
```

## Rollback

Every commit is a clean checkpoint. To revert any phase:

```bash
git log --oneline
git checkout <prev-commit> -- main/ scripts/
idf.py flash
pkill -f ble_notify_daemon  # daemon auto-restarts on next hook
```
