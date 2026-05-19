---
name: disarm-monitor
description: Detect any live Monitor task armed in this REPL session and tear it down gracefully via TaskStop. Counterpart to /arm-monitor. Useful before restarting Claude in the same tmux pane, before a long-running task that shouldn't interleave with bus events, or for arm/disarm test cycles.
---

# /disarm-monitor — Stop the vantage-bus event Monitor

You are in a peer-project Claude session that may have a live Monitor armed (from a prior `/arm-monitor` invocation). This skill finds the live Monitor task(s) and stops them via `TaskStop`.

## What you must do (sequence is binding)

### Step 1 — Ensure TaskList + TaskStop are loaded

`TaskList` and `TaskStop` are deferred tools. If they're not in your current available-tools surface, load them first:

```python
ToolSearch(query="select:TaskList,TaskStop", max_results=2)
```

(If they're already loaded in this session, the ToolSearch call is harmless — it'll just confirm.)

### Step 2 — Enumerate live tasks

Call `TaskList()`. It returns every active task this session has spawned, including their IDs and descriptions.

### Step 3 — Identify Monitor tasks for vantage-bus

A Monitor armed by `/arm-monitor` will have:
- **Description**: starts with `"vantage-bus events for "` (the canonical phrasing from arm-monitor's SKILL.md)
- **Type**: monitor (not agent or other)

Collect every task ID matching that pattern. If you find unrelated Monitors (e.g., a user manually armed one for a different purpose), **do NOT stop them** — only stop ones whose description matches the vantage-bus pattern.

### Step 4 — Stop each matching Monitor

For each collected task ID, call:

```python
TaskStop(task_id="<id>")
```

`TaskStop` is synchronous: it sends a kill signal to the underlying `tail -F` subprocess and waits for the process to exit. Allow it to return before moving to the next.

### Step 5 — Report

Print exactly one of:

- If you stopped 1+ Monitors:
  ```
  Disarmed <N> vantage-bus Monitor(s):
    - <task_id>: <description>
    ...
  This REPL no longer wakes on bus events. The bridge keeps writing inbox files; events accumulate until /arm-monitor re-arms or the operator processes them via the peer_bus_inbox_check.sh hook (PostToolUse / UserPromptSubmit).
  ```

- If no matching Monitors were found:
  ```
  No live vantage-bus Monitors detected in this session — already disarmed.
  ```

## What this does NOT do

- **Does NOT touch the bridge launchd job.** `local.<peer>.bridge` keeps running, keeps holding the WebSocket connection to the bus, keeps writing inbox files. You've only stopped the listener REPL's reactive surface.
- **Does NOT drain backlog.** Any events that arrive after disarm sit in `.cross_comms/inbox/` until you re-`/arm-monitor` (which drains backlog as part of its arming sequence) OR until the `peer_bus_inbox_check.sh` hook surfaces them on the next PostToolUse / UserPromptSubmit.
- **Does NOT modify `state.json`.** Cursor stays where it is. Re-arming later picks up cleanly from where you left off.
- **Does NOT stop unrelated tasks.** Background agents, other Monitors with different descriptions, scheduled wakeups — all untouched.

## When to use this

| Scenario | Use disarm? |
|---|---|
| About to restart `claude` in this tmux pane | ✅ Yes — the Monitor subprocess would otherwise leak as a dangling `tail` process. |
| About to do a long interactive task that you don't want bus events to interleave with | ⚠️ Maybe — usually queueing is safer. Only disarm if you specifically need silence. |
| Done with this session for a while; want to free Monitor capacity | ✅ Yes. |
| Testing arm/disarm cycle | ✅ Yes — pair with `/arm-monitor`. |
| Bus daemon is being restarted by operator | ❌ No — disarm doesn't help; the bridge handles the reconnect, your Monitor just keeps tailing the bridge log (it survives bridge restarts). |

## Safety net after disarm

The `.claude/settings.json` hooks (`peer_bus_inbox_check.sh` on `SessionStart` + `PostToolUse` + `UserPromptSubmit`) still fire. So even with the Monitor disarmed, bus events written to `.cross_comms/inbox/` will surface in your next user turn or tool call. You don't lose data; you only lose the autonomous-wake property.

## Re-arming

Just invoke `/arm-monitor` again. It will:
1. Detect this peer's identity from `.cross_comms/config.json`
2. Arm a fresh Monitor on the bridge stdout log
3. Drain any backlog inbox files (events that accumulated while disarmed)
4. Idle.
