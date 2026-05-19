---
name: arm-monitor
description: Arm a persistent Monitor on this peer's vantage-bus listener bridge log, so Claude wakes on every new bus event arrival. Auto-detects the project name from .cross_comms/config.json. Drain any backlog inbox files. Then idle.
---

# /arm-monitor — Activate vantage-bus event Monitor for this peer

You are in a Claude session running inside a vantage-bus peer project's repo. This skill arms a **persistent Monitor** on the listener bridge's stdout log so you wake on every new bus event without polling.

The bridge (a separate launchd process — `local.<peer>.bridge`) owns the WebSocket connection to the bus. When a bus event arrives, the bridge:
1. Writes the event atomically to `.cross_comms/inbox/<event_id>.json` (β-durable file).
2. Emits the event_id as a single line to its stdout, captured by launchd into `.cross_comms/listener_bridge_logs/bridge.stdout.log` (γ-durable signal).

Your job after this skill arms the Monitor: react to every new line on that log.

## What you must do (sequence is binding)

### Step 1 — Detect this peer's identity

Read `.cross_comms/config.json` from the current working directory and extract `project_name`. If the file is missing or unreadable, **abort and tell the user this REPL doesn't look like a vantage-bus peer** (point at `vantage peer add` for onboarding).

```python
import json
config = json.loads(open(".cross_comms/config.json").read())
project_name = config["project_name"]
```

### Step 2 — Verify the bridge log exists

Check that `.cross_comms/listener_bridge_logs/bridge.stdout.log` exists. If not, **abort and tell the user the bridge launchd job is not running** — they need to ask the operator to bootstrap `local.<peer>.bridge`.

### Step 3 — Arm the Monitor

Call the Monitor tool **exactly** like this (substituting the detected project name):

```python
Monitor(
    description=f"vantage-bus events for {project_name}",
    command="tail -F .cross_comms/listener_bridge_logs/bridge.stdout.log",
    persistent=True,
    timeout_ms=3600000,
)
```

The `-F` (capital) is load-bearing — it survives log rotation. `tail -f` (lowercase) would silently break when the launchd-captured log rotates.

### Step 4 — Drain backlog

Before idling, scan `.cross_comms/inbox/` for any pre-existing JSON event files (cold-start backlog). For each, process it per the dispatch table in §Dispatch below, then call `mark_event_processed(path, ".cross_comms/.processed")` to move the file out of inbox.

If `tools/bus_inbox.py` is importable (peer has a vendored copy or PYTHONPATH includes the vantage repo), use its `read_bus_inbox` + `process_event_with_dedup` helpers. Otherwise read+dispatch manually.

### Step 5 — Confirm + idle

Print exactly: `Monitor armed for <project_name>. Idle until next bus event.`

Then **idle**. NO proactive work between Monitor fires. Don't browse the codebase, don't read files, don't start tasks. Just wait for the next `<task-notification>`.

## When the Monitor fires

Each notification body is a **single line — the bare event_id** (e.g., `12284`). To handle:

1. Parse the integer event_id from the notification line.
2. Read `.cross_comms/inbox/<event_id padded to 10 digits>.json`. Filename uses zero-padded format like `0000012284.json`.
3. Look at the `topic` field. Dispatch per the table below.
4. After dispatching, move the file via `mark_event_processed(path, ".cross_comms/.processed")`. If dispatch failed, use `mark_event_failed(path, ".cross_comms/.failed", reason="<short>")`.
5. Return to idle.

### Dispatch table (canonical — same as `.claude/bus-listener-mode.md` if present)

| Topic | Action |
|---|---|
| `slack.inbound.<project>.*` | Draft a reply (1–4 sentences). Publish via `tools/bus_publish.py:publish_peer_slack_response(<project>, body=<reply>, channel=<from payload>, thread_ts=<from payload>, verb="answer", outbox_dir=Path(".cross_comms/outbox"), urgent=<from payload or False>, correlation_id=<from inbound>)`. The slack adapter routes the response back into the original Slack thread. |
| `directive.cto.*` / `directive.coo.*` | Summarize in your own context. If the body explicitly asks you to publish a response (e.g., a test ping), do so via outbox atomic-write. Otherwise log + acknowledge. |
| `override.*` | HALT. Escalate to operator. Do NOT auto-comply. |
| `contract.*.*` / `phase.boundary.*` | Log + acknowledge. No action. |
| `ratification_response` | Log. |
| `design_question` | If `recipients` includes your project AND the body asks a real question, draft a reply and publish another `design_question` event with same `correlation_id` (no separate response topic). Otherwise log. |
| `peer.heartbeat` / `peer.inbox_backlog_high` / `mesh.health.*` | Observability events. Log + ignore. |
| (any other) | Log "unhandled topic <name>" and continue idle. |

### Outbound atomic-write pattern (for direct outbox writes)

When publishing via `outbox/` rather than a `tools/bus_publish.py` helper:

```python
import json, os, uuid
from datetime import UTC, datetime
from pathlib import Path

key = f"<project>-pong-{uuid.uuid4().hex[:12]}"  # or any unique idempotency_key
event = {
    "topic": "status.published",  # or whatever topic applies
    "sender": "<project>",
    "ts_utc": datetime.now(UTC).isoformat(),
    "correlation_id": "<from inbound>",
    "idempotency_key": key,
    "payload": {"body": "<your reply>"},
}
outbox = Path(".cross_comms/outbox")
tmp = outbox / f"{key}.json.tmp"
final = outbox / f"{key}.json"
tmp.write_text(json.dumps(event, indent=2))
os.rename(tmp, final)
# Bridge's outbox_loop will pick this up and ship to the bus within ~1s.
```

## Common pitfalls

- **`tail -f` (lowercase) doesn't survive log rotation.** Use `-F` (uppercase).
- **The Monitor's stdout line is the event_id, NOT the event body.** Always read `.cross_comms/inbox/<id>.json` to get the topic + payload.
- **Don't write to `state.json` directly.** Cursor dedup is handled by `process_event_with_dedup` (or by the bridge's `last_processed_event_id` key).
- **Don't write to `.cross_comms/outbox/.sent/` directly.** That's the bridge's destination after successful ship. Only write `.cross_comms/outbox/<key>.json` (atomic via `.tmp` then rename).
- **Don't poll the inbox** between Monitor fires. Monitor's `tail -F` is the canonical wake signal.
- **If the Monitor stops** (timeout / explicit TaskStop), re-invoke `/arm-monitor` to re-arm. The bridge keeps logging regardless; armed-Monitor is the only thing that wakes you.

## End state after this skill runs

- Persistent Monitor is alive, watching `.cross_comms/listener_bridge_logs/bridge.stdout.log`.
- Any inbox backlog has been drained.
- You are idle, waiting for the next `<task-notification>`.

To verify from the operator side, oak can publish a test event addressed to this project; the bridge will log the event_id; your Monitor will fire; you'll process it.
