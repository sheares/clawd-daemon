#!/bin/bash
# BLE-notify hook wrapper. Reads the Claude Code hook JSON from stdin,
# extracts session_id (and tool_name for the "tool" endpoint), and forwards
# to the daemon. Used by all hooks so session-count tracking sees every event.
#
# Topic detection: on UserPromptSubmit (the "thinking" endpoint), the prompt
# is scanned for trigger phrases that signal a context shift ("switch to",
# "let's work on", "jump to", etc.). When matched, the next content-bearing
# word becomes the session label and is sticky in /tmp/clawd_topic.<sid> so
# later hooks for the same session keep using it instead of the cwd basename.
#
# Usage:
#   ble_notify_hook.sh thinking      → GET /thinking?session_id=...
#   ble_notify_hook.sh question      → GET /question?session_id=...
#   ble_notify_hook.sh notify        → GET /notify?session_id=...
#   ble_notify_hook.sh tool          → GET /tool/<tool_name>?session_id=...
#   ble_notify_hook.sh tool-clear    → GET /tool?session_id=...
#   ble_notify_hook.sh end           → GET /end?session_id=...  (SessionEnd)

ENDPOINT="$1"
J=$(cat 2>/dev/null || true)
S=$(printf '%s' "$J" | jq -r '.session_id // empty' 2>/dev/null || true)
CWD=$(printf '%s' "$J" | jq -r '.cwd // empty' 2>/dev/null || true)
[ -z "$CWD" ] && CWD="$PWD"
DEFAULT_LABEL=$(basename "$CWD" | tr -cd 'A-Za-z0-9_ -')

TOPIC_FILE=""
[ -n "$S" ] && TOPIC_FILE="/tmp/clawd_topic.$S"

# Topic detection — explicit only. Looks for any of these trigger phrases
# in the user's prompt and uses the next 1-3 meaningful words as the new
# label. Sticky in /tmp/clawd_topic.<sid>.
#   "change [the|this] session name to <X>"
#   "rename [the|this] session to <X>"
#   "set [the|this] session name to <X>"
if [ "$ENDPOINT" = "thinking" ] && [ -n "$TOPIC_FILE" ]; then
    PROMPT=$(printf '%s' "$J" | jq -r '.prompt // empty' 2>/dev/null || true)
    if [ -n "$PROMPT" ]; then
        NEW_TOPIC=$(printf '%s' "$PROMPT" | awk '
        BEGIN {
            n_trigs = 10
            TRIGS[1] = "change session name to"
            TRIGS[2] = "change the session name to"
            TRIGS[3] = "change this session name to"
            TRIGS[4] = "rename session to"
            TRIGS[5] = "rename the session to"
            TRIGS[6] = "rename this session to"
            TRIGS[7] = "set session name to"
            TRIGS[8] = "set the session name to"
            TRIGS[9] = "set this session name to"
            # Bare fragment — must come last in the array because longer
            # triggers containing this substring should win (best-pos logic
            # also picks longer-match on tie).
            TRIGS[10] = "session name to"
            # Words that should end the topic capture (politeness tail-words)
            STOP["please"]=1; STOP["thanks"]=1; STOP["thank"]=1
            STOP["now"]=1; STOP["then"]=1; STOP["ok"]=1; STOP["okay"]=1
            STOP["and"]=1; STOP["but"]=1; STOP["also"]=1; STOP["so"]=1
            STOP["while"]=1; STOP["when"]=1; STOP["if"]=1; STOP["for"]=1
            STOP["with"]=1; STOP["after"]=1; STOP["before"]=1
            STOP["because"]=1; STOP["since"]=1
        }
        { buf = buf " " $0 }
        END {
            line = tolower(buf)
            best_pos = 0; best_len = 0
            for (i = 1; i <= n_trigs; i++) {
                p = index(line, TRIGS[i])
                if (p > 0 && (best_pos == 0 || p < best_pos ||
                              (p == best_pos && length(TRIGS[i]) > best_len))) {
                    best_pos = p
                    best_len = length(TRIGS[i])
                }
            }
            if (best_pos == 0) exit
            rest = substr(buf, best_pos + best_len)
            # Cut at first sentence-terminator
            sub(/[.!?].*/, "", rest)
            n = split(rest, words, /[ \t\n\r,;:()\[\]{}"]+/)
            out = ""; count = 0
            for (i = 1; i <= n && count < 3; i++) {
                w = words[i]
                gsub(/[^a-zA-Z0-9-]/, "", w)
                if (w == "") continue
                lw = tolower(w)
                if (lw in STOP) break
                if (out == "") out = w; else out = out "-" w
                count++
            }
            if (out != "") print toupper(substr(out, 1, 12))
        }')
        if [ -n "$NEW_TOPIC" ]; then
            printf '%s' "$NEW_TOPIC" > "$TOPIC_FILE"
        fi
        # Diagnostic: log every thinking-hook with prompt snippet + detected topic.
        # Tail with: tail -f /tmp/clawd_hook.log
        {
            printf '[%s] sid=%s topic=%s prompt=%q\n' \
                "$(date +%H:%M:%S)" "${S:0:8}" "${NEW_TOPIC:-<none>}" \
                "$(printf '%s' "$PROMPT" | head -c 120)"
        } >> /tmp/clawd_hook.log 2>/dev/null
        # "reset clawd" → wipe all stored topics + clear all daemon-side session
        # state so Clawd returns to env mode with an empty monitor.
        if printf '%s' "$PROMPT" | grep -qi 'reset clawd'; then
            rm -f /tmp/clawd_topic.* 2>/dev/null
            curl -s --max-time 2 "http://127.0.0.1:8765/reset" > /dev/null 2>&1 || true
        fi

        # ── Easter egg phrases — fire emotes via daemon /emote/<name>.
        # First match wins (elif chain). Curl in background where the emote is
        # multi-step so the hook returns immediately and doesn't slow Claude.
        if printf '%s' "$PROMPT" | grep -qiE 'good ?night clawd'; then
            curl -s --max-time 2 "http://127.0.0.1:8765/emote/sleepy" > /dev/null 2>&1 || true
        elif printf '%s' "$PROMPT" | grep -qiE '(good )?morning clawd'; then
            # Happy first, then waving 2 s later (background so hook returns fast).
            curl -s --max-time 2 "http://127.0.0.1:8765/emote/happy" > /dev/null 2>&1 || true
            ( sleep 2; curl -s --max-time 2 "http://127.0.0.1:8765/emote/waving" > /dev/null 2>&1 ) &
        elif printf '%s' "$PROMPT" | grep -qiE '(i )?love you clawd'; then
            curl -s --max-time 2 "http://127.0.0.1:8765/emote/loving" > /dev/null 2>&1 || true
        elif printf '%s' "$PROMPT" | grep -qiw 'dance'; then
            curl -s --max-time 2 "http://127.0.0.1:8765/emote/dance" > /dev/null 2>&1 || true
        fi
    fi
fi

# SessionEnd: evict the session immediately and clean up local topic file
if [ "$ENDPOINT" = "end" ]; then
    [ -n "$TOPIC_FILE" ] && rm -f "$TOPIC_FILE"
    if [ -n "$S" ]; then
        curl -s -G --max-time 2 \
            --data-urlencode "session_id=$S" \
            "http://127.0.0.1:8765/end" > /dev/null 2>&1 || true
    fi
    exit 0
fi

# Prefer a sticky topic over the cwd basename, when one is set
LABEL="$DEFAULT_LABEL"
if [ -n "$TOPIC_FILE" ] && [ -f "$TOPIC_FILE" ]; then
    SAVED=$(cat "$TOPIC_FILE" 2>/dev/null)
    [ -n "$SAVED" ] && LABEL="$SAVED"
fi

case "$ENDPOINT" in
    tool)
        N=$(printf '%s' "$J" | jq -r '.tool_name // empty' 2>/dev/null || true)
        if [ -n "$N" ]; then
            URL="http://127.0.0.1:8765/tool/$N"
        else
            URL="http://127.0.0.1:8765/tool"
        fi
        ;;
    tool-clear)
        URL="http://127.0.0.1:8765/tool"
        ;;
    *)
        URL="http://127.0.0.1:8765/$ENDPOINT"
        ;;
esac

if [ -n "$S" ]; then
    curl -s -G --max-time 2 \
        --data-urlencode "session_id=$S" \
        --data-urlencode "label=$LABEL" \
        "$URL" > /dev/null 2>&1 || true
else
    curl -s --max-time 2 "$URL" > /dev/null 2>&1 || true
fi
