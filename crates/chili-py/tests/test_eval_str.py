"""Sprint 22 W1 — `eval_str` pepper builtin end-to-end closure gate.

This is the MC-4 mandatory W1 closure gate per the Sprint 22 dispatch
brief audit appendix: W1 is not closed until `test_sync_eval_str_simple`
passes end-to-end. The Rust unit tests (`eval_str_test.rs`) cover the
chili-core side (parse + eval_ast + contract delta vs evalc); these
tests prove the full chain:

    engine.sync(h, (Symbol("eval_str"), "<pepper>"))
      → fn_call (Python wrapper)
      → fn_call I64-arm dispatch (engine_state.rs:1942-1951)
      → eval_call → state.sync(h, MixedList) over chili:// TCP
      → REMOTE engine's handle_chili_conn → eval_op → state.get_var("eval_str")
      → SIDE_EFFECT_FN["eval_str"] dispatch → state.parse + eval_ast
      → result serialized back over chili:// to the caller

The wishlist documents the use case at
`docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` — operator
qcon REPL across all mdata daemons.

NOTE: brief A.4 text says "file:// handle" but file:// is write-only
(tplog sequence-file) and cannot return query results. Using chili://
TCP per the existing `test_ipc_remote_query.TestIpcRemoteQuery` setup.
"""

import socket
import time

import pytest
from chili import ChiliEngine


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestEvalStrRemote:
    """End-to-end remote `eval_str` round-trip via chili:// TCP."""

    def test_sync_eval_str_simple(self):
        """MC-4 closure gate: `client.sync(h, ("eval_str", "1 + 2"))` must
        evaluate `1 + 2` on the receiver engine and return `3`. This is
        the W1 contract — chili-side dispatch into the new
        `SIDE_EFFECT_FN["eval_str"]` builtin is mandatory; if this test
        fails, W1's structural assumption (that adding a builtin to
        SIDE_EFFECT_FN is sufficient — no new dispatch wiring) is wrong."""
        port = _free_port()

        receiver = ChiliEngine(pepper=True)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        try:
            client = ChiliEngine(pepper=True)
            h = client.open_handle(f"chili://127.0.0.1:{port}")
            assert isinstance(h, int)

            # The mdata-wishlist API shape: tuple with bare-string fn name.
            # chili-py converts Python str → SpicyObj::Symbol when crossing
            # the FFI boundary (see crates/chili-py/src/lib.rs:111).
            result = client.sync(h, ("eval_str", "1 + 2"))
            assert result == 3, (
                f"remote eval_str('1 + 2') must return 3 (I64), got {result!r} "
                f"of type {type(result).__name__}"
            )

            # Contract delta vs evalc: result is the RAW I64, not a String.
            assert not isinstance(result, str), (
                f"eval_str must NOT stringify the result — that is evalc's job. "
                f"Got String: {result!r}"
            )

            client.shutdown()
        finally:
            receiver.shutdown()

    def test_sync_eval_str_assign_then_read(self):
        """Two-call round-trip via eval_str: first call binds a var on
        the receiver; second call reads it back. Proves remote state
        mutation persists across IPC calls."""
        port = _free_port()

        receiver = ChiliEngine(pepper=True)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        try:
            client = ChiliEngine(pepper=True)
            h = client.open_handle(f"chili://127.0.0.1:{port}")

            # Call 1 — assign `.test.x: 42` on the receiver.
            client.sync(h, ("eval_str", ".test.x: 42"))

            # Call 2 — read `.test.x` back via eval_str.
            result = client.sync(h, ("eval_str", ".test.x"))
            assert result == 42, (
                f"remote eval_str ('.test.x' lookup after assign) must return 42, "
                f"got {result!r}"
            )

            client.shutdown()
        finally:
            receiver.shutdown()

    def test_sync_eval_str_parse_error_propagates_not_panic(self):
        """Invalid pepper source must surface as a ChiliError on the
        client side, not crash the receiver (no Rust panic). Mirrors
        the Rust unit test `eval_str_errors_on_parse_failure`."""
        port = _free_port()

        receiver = ChiliEngine(pepper=True)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        try:
            client = ChiliEngine(pepper=True)
            h = client.open_handle(f"chili://127.0.0.1:{port}")

            with pytest.raises(Exception) as excinfo:
                client.sync(h, ("eval_str", "invalid )(*&"))
            # Just check we got some structured error, not a panic-shaped
            # message. The exact message wording is parser-implementation-
            # dependent; the load-bearing assertion is "no panic".
            assert "panic" not in str(excinfo.value).lower(), (
                f"eval_str parse error must NOT manifest as a Rust panic. "
                f"Got: {excinfo.value!r}"
            )

            client.shutdown()
        finally:
            receiver.shutdown()
