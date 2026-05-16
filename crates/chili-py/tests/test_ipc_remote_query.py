"""Sprint 19 (ADD-2) — acceptance test for the upstream IPC
remote-query feature (`606d1cc feat: add open_handle and sync for IPC
remote queries`), merged main → claude-2.

`606d1cc` adds `ChiliEngine.open_handle(socket)` and
`ChiliEngine.sync(handle_num, query)` plus the `fn_call` I64 dispatch
arm. claude-2's `sync()` Outgoing branch (Sprint 17 `publish_via_handle`
substrate) already does the synchronous send+receive, so the feature
works on the merged claude-2 — this test proves it end-to-end over TCP
loopback and guards it against regression in future upstream merges
(Sprint-19 audit ADD-2; the headline feature touches the diverged
`sync()` surface).
"""

import socket
import time

import pytest
from chili import ChiliEngine


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestIpcRemoteQuery:
    def test_sync_bytes_query_round_trip(self):
        """Client opens a `chili://` handle to a receiver engine and
        `sync`s a raw-string query; the receiver evaluates it and the
        result comes back. `engine.sync(h, b"1+1") == 2`."""
        port = _free_port()

        receiver = ChiliEngine(pepper=True)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        client = ChiliEngine(pepper=True)
        h = client.open_handle(f"chili://127.0.0.1:{port}")
        assert isinstance(h, int), f"open_handle must return an i64 id, got {type(h).__name__}"

        result = client.sync(h, b"1+1")
        assert result == 2, f"remote `1+1` must evaluate to 2, got {result!r}"

        client.shutdown()
        receiver.shutdown()

    def test_sync_list_query_is_function_call(self):
        """List form is sent as a function call `(func, args…)`:
        `sync(h, ["set", "a", 2])` then a follow-up query reads it back
        on the receiver — proving the list/MixedList dispatch path."""
        port = _free_port()

        receiver = ChiliEngine(pepper=True)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        client = ChiliEngine(pepper=True)
        h = client.open_handle(f"chili://127.0.0.1:{port}")
        client.sync(h, ["set", "a", 2])
        # Read it back through the same handle.
        got = client.sync(h, b"a")
        assert got == 2, f"remote var `a` set via list-form sync must read back 2, got {got!r}"

        client.shutdown()
        receiver.shutdown()
