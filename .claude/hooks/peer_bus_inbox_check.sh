#!/bin/bash
# Sprint 10.2 / 10.3 — bus inbox check for peer Claude sessions.
#
# Multi-hook: wire into THREE Claude Code hook events to cover the
# full session lifecycle:
#
#   - SessionStart       — backlog catch-up at REPL open (Sprint 10.2)
#   - PostToolUse        — mid-session arrivals after every tool call
#                          (Sprint 10.3 — closes the "Sid at lunch" UX gap)
#   - UserPromptSubmit   — catches anything that arrived while Claude
#                          was idle, before next user turn (Sprint 10.3)
#
# Watermark-based dedup: outputs only events with event_id > watermark.
# Watermark at `.cross_comms/.last_hook_event_id` (atomic tmp+rename
# write so concurrent fires don't corrupt). First-ever fire on a fresh
# session has no watermark → shows full backlog. Subsequent fires
# silent unless something new arrived. Same script for all 3 hook
# events; behavior is differentiated only by the watermark state.
#
# Installed via `.claude/hooks/peer_bus_inbox_check.sh` in each peer
# repo (see `ops/peer_hooks/INSTALL.md`). Project-agnostic via
# $CLAUDE_PROJECT_DIR; falls back to $(pwd).

set -euo pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"
INBOX="$PROJECT_DIR/.cross_comms/inbox"
WATERMARK="$PROJECT_DIR/.cross_comms/.last_hook_event_id"

# Silent exit if no inbox dir (project hasn't onboarded to the bus yet)
if [ ! -d "$INBOX" ]; then
    exit 0
fi

# Read watermark (0 if missing → first ever fire on this session)
last_id=0
if [ -f "$WATERMARK" ]; then
    last_id=$(cat "$WATERMARK" 2>/dev/null || echo 0)
fi

# Use python for the heavy lifting: scan inbox, filter by watermark,
# emit ranked output, compute new watermark. Shell composition is too
# brittle for the multi-step logic.
python3 - "$INBOX" "$last_id" "$WATERMARK" <<'PY'
import json
import os
import sys
import tempfile
from pathlib import Path

inbox = Path(sys.argv[1])
last_id = int(sys.argv[2])
watermark_path = Path(sys.argv[3])

# Enumerate all inbox events with their event_ids
all_events: list[tuple[int, Path, dict]] = []
for f in inbox.iterdir():
    if not f.is_file() or f.suffix != ".json":
        continue
    try:
        event = json.loads(f.read_text())
    except (OSError, json.JSONDecodeError):
        continue
    try:
        event_id = int(event.get("event_id", 0))
    except (TypeError, ValueError):
        continue
    all_events.append((event_id, f, event))

# Filter: only events with event_id > last_id (the watermark)
new_events = [e for e in all_events if e[0] > last_id]

if not new_events:
    # Silent exit — nothing new since last fire
    sys.exit(0)

# Sort newest first (descending event_id)
new_events.sort(key=lambda e: e[0], reverse=True)

print(f"=== bus inbox: {len(new_events)} new event(s) since last check ===")
print()

# Render up to 5 most-recent new events
for event_id, f, event in new_events[:5]:
    topic = str(event.get("topic", "?"))
    sender = str(event.get("sender", "?"))
    payload = event.get("payload") or {}
    body = str(payload.get("body", ""))
    recipients = payload.get("recipients") or []
    correlation_id = event.get("correlation_id")

    priority = "    "
    if topic.startswith("directive.cto.") or topic.startswith("directive.coo."):
        priority = "*** "
    elif topic.startswith("override."):
        priority = "!!! "
    elif topic == "ratification_request":
        priority = "??? "
    elif topic.startswith("slack.inbound."):
        priority = ">>> "

    print(f"{priority}#{event_id} {topic}")
    line = f"    from: {sender}"
    if recipients:
        line += f"  to: {', '.join(str(r) for r in recipients)}"
    if correlation_id:
        line += f"  corr: {correlation_id}"
    print(line)
    if body:
        snippet = body.replace("\n", " ")
        if len(snippet) > 280:
            snippet = snippet[:280] + "…"
        print(f"    body: {snippet}")
    print(f"    file: {f}")
    print()

# Atomic watermark update — write to tmp + rename so concurrent fires
# don't truncate mid-write.
new_max_id = max(e[0] for e in new_events)
watermark_path.parent.mkdir(parents=True, exist_ok=True)
tmp_fd, tmp_path = tempfile.mkstemp(
    prefix=watermark_path.name + ".", dir=watermark_path.parent
)
try:
    with os.fdopen(tmp_fd, "w") as out:
        out.write(str(new_max_id))
    os.rename(tmp_path, watermark_path)
except Exception:
    try:
        os.unlink(tmp_path)
    except OSError:
        pass
    raise

print("Legend: *** directive / !!! override / ??? ratification_request / >>> slack inbound")
print("Consume: read each event, dispatch by topic, then mark_event_processed (move to .cross_comms/.sent/)")
print("Contract: ~/team/oak/vantage/docs/architecture/team_bus.md §D")
PY
