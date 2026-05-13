#!/usr/bin/env bash
# Sprint 11 Part A.3 — peer-side bus retention hook (SessionStart only).
#
# Wired ONLY to `SessionStart` (NOT PostToolUse / UserPromptSubmit) to
# avoid latency on every tool call. Once per Claude session is enough
# to keep .cross_comms/.sent/ + outbox_*/.sent/ + outbox_*/.failed/ +
# *.log files bounded.
#
# Silent unless retention actually deleted something. The retention
# module (bus/retention.py) handles missing DB / missing dirs
# gracefully — peers without events.db just get .sent/ + log cleanup.
#
# Wire via .claude/settings.json:
#   {
#     "hooks": {
#       "SessionStart": [
#         {"hooks": [{"type": "command",
#                     "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/peer_bus_retention.sh"}]}
#       ]
#     }
#   }

set -euo pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"
cd "$PROJECT_DIR"

# Skip silently if bus/retention.py isn't present (peer hasn't installed
# the shared retention module). Avoids spurious "module not found" noise.
if [ ! -f "$PROJECT_DIR/bus/retention.py" ]; then
    exit 0
fi

# Run retention with peer-friendly defaults. Peers may not have
# events.db / auth.db; the helpers no-op cleanly in that case.
# Output piped through awk to keep only summary lines that report
# non-zero deletes (suppress noise).
output=$(uv run python -m bus.retention prune \
    --keep-files-days 30 \
    --keep-episodes 500 \
    2>&1 || true)

# Show a banner only if something was actually deleted (deleted=1+,
# entries_deleted=1+). "scanned" alone doesn't merit user-visible output.
echo "$output" | awk '
    /deleted=[1-9]/ || /entries_deleted=[1-9]/ {
        if (!banner_shown) {
            print "=== bus retention sweep (SessionStart) ==="
            banner_shown = 1
        }
        print "  " $0
    }
    END {
        if (banner_shown) print ""
    }'
