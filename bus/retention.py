"""Sprint 11 Part A.1 — retention sweep for bus state surfaces.

Five prune helpers cover every growing surface vantage owns:

- ``prune_events_db``: DELETE rows from events table older than keep_days.
- ``prune_sent_dirs``: unlink *.json files under .sent/ and outbox_*/.sent/
  + outbox_*/.failed/ older than keep_days (mtime). Never touches inbox/.
- ``prune_auth_tokens``: DELETE rows from auth tokens table where
  revoked_at < now - keep_days. Active tokens are never deleted.
- ``prune_log_files``: unlink *.log files older than keep_days under
  given root dirs (e.g., bus/state, project root for daemon.log).
- ``prune_episodes_md``: truncate per-agent memory/<agent>/episodes.md
  to most-recent keep_entries items (entry delimiter is the ``---``
  separator written by ``BaseAgent._save_episode``).

Run via: ``uv run python -m bus.retention prune [--keep-events-days N]``

Companion to ``bus/backup.py``: the launchd plist runs
``bus.backup && bus.retention prune`` so the daily backup completes
before any DELETE/unlink fires.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from config import BASE_DIR

log = logging.getLogger(__name__)

DEFAULT_EVENTS_DB = BASE_DIR / "bus" / "state" / "events.db"
DEFAULT_AUTH_DB = BASE_DIR / "bus" / "state" / "auth.db"
DEFAULT_CROSS_COMMS = BASE_DIR / ".cross_comms"
DEFAULT_MEMORY = BASE_DIR / "memory"
DEFAULT_LOG_ROOTS = (BASE_DIR / "bus" / "state", BASE_DIR / "state", BASE_DIR)

DEFAULT_KEEP_EVENTS_DAYS = 90
DEFAULT_KEEP_AUTH_DAYS = 90
DEFAULT_KEEP_FILES_DAYS = 30
DEFAULT_KEEP_EPISODES = 500


def prune_events_db(
    *,
    keep_days: int = DEFAULT_KEEP_EVENTS_DAYS,
    db_path: Path = DEFAULT_EVENTS_DB,
) -> dict[str, int]:
    """DELETE rows from ``events`` where ts_utc < now - keep_days.

    Idempotent — second call deletes nothing further. Missing DB returns
    zero counts (caller can chain helpers without pre-checks).
    """
    if not db_path.exists():
        return {"deleted": 0, "remaining": 0}
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("DELETE FROM events WHERE ts_utc < ?", (cutoff,))
        deleted = cur.rowcount
        conn.commit()
        cur = conn.execute("SELECT COUNT(*) FROM events")
        remaining = int(cur.fetchone()[0])
    finally:
        conn.close()
    return {"deleted": deleted, "remaining": remaining}


def prune_sent_dirs(
    *,
    keep_days: int = DEFAULT_KEEP_FILES_DAYS,
    root: Path = DEFAULT_CROSS_COMMS,
) -> dict[str, int]:
    """Unlink *.json files under .sent/ + outbox_*/.sent/ + outbox_*/.failed/
    older than keep_days (mtime).

    NEVER touches ``inbox/`` — those are unprocessed live events.
    """
    if not root.exists():
        return {"scanned": 0, "deleted": 0}
    cutoff = time.time() - (keep_days * 86400)
    scanned = 0
    deleted = 0

    targets: list[Path] = []
    sent = root / ".sent"
    if sent.exists():
        targets.append(sent)
    for outbox in root.glob("outbox_*"):
        for sub in (".sent", ".failed"):
            p = outbox / sub
            if p.exists():
                targets.append(p)

    for d in targets:
        for f in d.glob("*.json"):
            scanned += 1
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except OSError as e:
                log.warning("retention: could not unlink %s: %s", f, e)
    return {"scanned": scanned, "deleted": deleted}


def prune_auth_tokens(
    *,
    keep_days: int = DEFAULT_KEEP_AUTH_DAYS,
    db_path: Path = DEFAULT_AUTH_DB,
) -> dict[str, int]:
    """DELETE rows from ``tokens`` where revoked_at < now - keep_days.

    Active tokens (``revoked_at IS NULL``) are NEVER deleted regardless
    of issue date — they're load-bearing for live peers.
    """
    if not db_path.exists():
        return {"deleted": 0, "remaining": 0}
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM tokens WHERE revoked_at IS NOT NULL AND revoked_at < ?",
            (cutoff,),
        )
        deleted = cur.rowcount
        conn.commit()
        cur = conn.execute("SELECT COUNT(*) FROM tokens")
        remaining = int(cur.fetchone()[0])
    finally:
        conn.close()
    return {"deleted": deleted, "remaining": remaining}


def prune_log_files(
    *,
    keep_days: int = DEFAULT_KEEP_FILES_DAYS,
    roots: list[Path] | tuple[Path, ...] = DEFAULT_LOG_ROOTS,
) -> dict[str, int]:
    """Unlink *.log files under given roots older than keep_days (mtime).

    Roots are scanned at top level only (no recursion) to avoid touching
    log files in subdirectories that may belong to other components.
    """
    cutoff = time.time() - (keep_days * 86400)
    deleted = 0
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for f in root.glob("*.log"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except OSError as e:
                log.warning("retention: could not unlink log %s: %s", f, e)
    return {"deleted": deleted}


def prune_episodes_md(
    *,
    keep_entries: int = DEFAULT_KEEP_EPISODES,
    memory_root: Path = DEFAULT_MEMORY,
) -> dict[str, int]:
    """Truncate each ``memory/<agent>/episodes.md`` to most-recent keep_entries entries.

    Entry delimiter is the ``---`` separator written by
    ``BaseAgent._save_episode``. Counts entries (separator occurrences);
    if file has ≤ keep_entries, no-op. Otherwise rewrites file with
    the tail-keep_entries entries only.
    """
    if not memory_root.exists():
        return {"agents_processed": 0, "entries_deleted": 0}

    agents_processed = 0
    entries_deleted = 0
    for agent_dir in memory_root.iterdir():
        if not agent_dir.is_dir():
            continue
        episodes = agent_dir / "episodes.md"
        if not episodes.exists():
            continue
        agents_processed += 1
        text = episodes.read_text()
        # Split on the entry delimiter. The first element before the
        # first "---" is preamble (typically empty); skip it.
        parts = text.split("---")
        # parts[0] is preamble; parts[1:] are entries (each preceded by ---).
        entries = parts[1:]
        if len(entries) <= keep_entries:
            continue
        kept = entries[-keep_entries:]
        entries_deleted += len(entries) - keep_entries
        new_text = parts[0] + "---" + "---".join(kept)
        episodes.write_text(new_text)
    return {"agents_processed": agents_processed, "entries_deleted": entries_deleted}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bus.retention", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    prune_p = sub.add_parser("prune", help="Run all retention helpers")
    prune_p.add_argument("--keep-events-days", type=int, default=DEFAULT_KEEP_EVENTS_DAYS)
    prune_p.add_argument("--keep-auth-days", type=int, default=DEFAULT_KEEP_AUTH_DAYS)
    prune_p.add_argument("--keep-files-days", type=int, default=DEFAULT_KEEP_FILES_DAYS)
    prune_p.add_argument("--keep-episodes", type=int, default=DEFAULT_KEEP_EPISODES)
    prune_p.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Override base dir for testing (default: vantage repo root)",
    )
    args = parser.parse_args(argv)

    if args.cmd != "prune":
        parser.error(f"unknown command: {args.cmd}")

    base = args.base_dir if args.base_dir is not None else BASE_DIR
    events_db = base / "bus" / "state" / "events.db"
    auth_db = base / "bus" / "state" / "auth.db"
    cross_comms = base / ".cross_comms"
    memory = base / "memory"
    log_roots = [base / "bus" / "state", base / "state", base]

    ev = prune_events_db(keep_days=args.keep_events_days, db_path=events_db)
    print(f"events.db: deleted={ev['deleted']} remaining={ev['remaining']}")

    au = prune_auth_tokens(keep_days=args.keep_auth_days, db_path=auth_db)
    print(f"auth.db: deleted={au['deleted']} remaining={au['remaining']}")

    sd = prune_sent_dirs(keep_days=args.keep_files_days, root=cross_comms)
    print(f"sent_dirs: scanned={sd['scanned']} deleted={sd['deleted']}")

    lf = prune_log_files(keep_days=args.keep_files_days, roots=log_roots)
    print(f"log files: deleted={lf['deleted']}")

    ep = prune_episodes_md(keep_entries=args.keep_episodes, memory_root=memory)
    print(
        f"episodes.md: agents_processed={ep['agents_processed']} "
        f"entries_deleted={ep['entries_deleted']}"
    )

    # Safety guard per Sprint 11 audit appendix: halt if events.db delete > 95%
    total = ev["deleted"] + ev["remaining"]
    if total > 100 and ev["deleted"] / total > 0.95:
        print(
            f"WARNING: deleted {ev['deleted']}/{total} = "
            f"{ev['deleted'] / total:.1%} of events. "
            "Investigate clock/keep_days config — likely misconfigured.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
