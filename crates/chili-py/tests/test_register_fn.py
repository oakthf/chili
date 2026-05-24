"""Sprint 23 W3 — `engine.register_fn` end-to-end Python-callable bridge tests.

These pytest exercise the full Python → chili-core → Python callback
chain via the new ExternalFnDispatcher (ADR-0007). The chili-core side
has its own 5 unit tests in `crates/chili-core/tests/external_fn_test.rs`
using a Rust-side stub dispatcher; these tests prove the actual Python
end of the bridge:

    engine.register_fn(".add_two", py_callable, arity=2)
      → PyExternalDispatcher.register(name, PyAny)
      → state.set_external_dispatcher(Arc<PyExternalDispatcher>)
      → state.set_var(name, SpicyObj::Fn(Func::new_external(name, 2)))

    client.sync(h, (".add_two", 3, 4))   # over chili:// TCP
      → REMOTE engine eval_op → eval_call → eval_fn_call
      → W3 branch → external_dispatcher().dispatch(name, args)
      → PyExternalDispatcher::dispatch
      → Python::attach + invoke_python
      → py_callable(3, 4) → 7
      → spicy_from_py_bound → SpicyObj::I64(7)
      → result over chili:// → 7

NOTE: chili-py converts Python str → SpicyObj::Symbol at the FFI
boundary (see crates/chili-py/src/lib.rs:111), which is why the
tuple-form `sync(h, (name, *args))` works with a bare Python str name.
"""

import socket
import threading
import time
import warnings

import pytest
from chili import ChiliEngine


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestRegisterFnLocal:
    """Local-only tests: engine.fn_call directly, no IPC. Cheaper to
    set up + faster to diagnose. The remote-via-IPC closure gate is in
    TestRegisterFnRemote below."""

    def test_register_and_invoke_local(self):
        """Happy path: register a callable, call via fn_call, get result back."""
        e = ChiliEngine()

        def add_two(a, b):
            return a + b

        e.engine.register_fn(".add_two", add_two, 2)
        assert e.engine.fn_call(".add_two", [3, 4]) == 7

    def test_callback_reentry(self):
        """The mdata-shape: a Python callback that re-enters the engine
        (calls engine.set_var + engine.fn_call from within the callback).
        Asserts no deadlock + that side effects persist. This is the
        load-bearing test for hazard #1 (re-entrancy)."""
        e = ChiliEngine()
        observed = {}

        def with_reentry(date):
            # Callback writes to engine var while pepper holds nothing
            # (the W3 branch ran outside the vars lock per ADR-0007 §3).
            e.engine.set_var("last_fire_date", date)
            return "ack-" + str(date)

        e.engine.register_fn(".eod.fire", with_reentry, 1)
        result = e.engine.fn_call(".eod.fire", ["2026-05-24"])
        assert result == "ack-2026-05-24"
        # The re-entered set_var must be visible after the callback.
        assert e.engine.get_var("last_fire_date") == "2026-05-24"

    def test_python_exception_propagates(self):
        """Callback raises ValueError → engine.fn_call raises ChiliError
        with the exception type + msg + traceback embedded."""
        from chili import ChiliError

        e = ChiliEngine()

        def bad_handler(arg):
            raise ValueError(f"bad date: {arg!r}")

        e.engine.register_fn(".bad_fire", bad_handler, 1)
        with pytest.raises(ChiliError) as excinfo:
            e.engine.fn_call(".bad_fire", ["2026-99-99"])

        msg = str(excinfo.value)
        assert "external fn '.bad_fire' raised" in msg, msg
        assert "ValueError" in msg, msg
        assert "bad date" in msg, msg
        # Traceback frames should be present; require at least the test
        # file in the formatted traceback. Best-effort: if the traceback
        # module can't format for any reason, the dispatcher falls back
        # to repr-only — so we don't require traceback presence as a
        # hard contract, only the type + msg.

    def test_arity_mismatch_projection(self):
        """Calling a registered fn with FEWER args than arity returns a
        partial-applied Fn (Func::project path). Matches existing pepper
        user-fn behavior."""
        e = ChiliEngine()

        def add_two(a, b):
            return a + b

        e.engine.register_fn(".add_two", add_two, 2)
        # Call with 1 arg of a 2-arg fn — projection.
        result = e.engine.fn_call(".add_two", [10])
        # The projection is returned as an opaque Func object from the
        # Python side. We can't directly poke at its internals from
        # Python (no PyFunc class — `spicy_to_py` for `SpicyObj::Fn`
        # produces a string repr). What matters is: this call does NOT
        # raise (i.e., we don't accidentally treat arity=1-of-2 as an
        # error or invoke the callable prematurely).
        # We test the no-invocation contract by side-effect: bump a
        # counter in the callable and assert it was NOT incremented.
        calls = {"n": 0}

        def add_with_counter(a, b):
            calls["n"] += 1
            return a + b

        e.engine.register_fn(".count_add", add_with_counter, 2)
        _ = e.engine.fn_call(".count_add", [5])  # projection — no invoke
        assert calls["n"] == 0, "projection must NOT invoke the callable"

        # Full-arity call now → invokes once.
        assert e.engine.fn_call(".count_add", [5, 6]) == 11
        assert calls["n"] == 1

    def test_unregister_happy_path(self):
        """Register, invoke (succeeds), unregister (returns True), invoke
        again (fails cleanly with a chili error)."""
        from chili import ChiliError

        e = ChiliEngine()

        def fn():
            return 42

        e.engine.register_fn(".my_fn", fn, 0)
        assert e.engine.fn_call(".my_fn", []) == 42

        assert e.engine.unregister_fn(".my_fn") is True
        assert e.engine.unregister_fn(".my_fn") is False  # already gone

        # After unregister, the Func placeholder is also gone — the
        # subsequent fn_call hits a name-not-found path.
        with pytest.raises(ChiliError):
            e.engine.fn_call(".my_fn", [])

    def test_unregister_warns_on_dangling_dispatcher(self):
        """If the user pre-deleted the Func placeholder (via del_var)
        before calling unregister_fn, unregister_fn still succeeds
        (callable removed from dispatcher) but emits a UserWarning so
        the inconsistency surfaces in logs. Per audit MC-13."""
        e = ChiliEngine()

        def fn():
            return 1

        e.engine.register_fn(".vanish", fn, 0)
        # User clears the Func placeholder out-of-band — chili can't
        # prevent this (set_var/del_var are public API). The internal
        # callable is still in the dispatcher.
        e.engine.del_var(".vanish")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            removed = e.engine.unregister_fn(".vanish")
        assert removed is True, "callable WAS in dispatcher — unregister returns True"
        warn_msgs = [str(warning.message) for warning in w]
        # Must be exactly one warning about the dangling-dispatcher state.
        assert any(
            "external Func placeholder '.vanish' was already cleared" in m
            for m in warn_msgs
        ), f"expected UserWarning, got warnings: {warn_msgs!r}"


class TestRegisterFnRemote:
    """End-to-end W3 closure-gate via chili:// TCP IPC.

    This is the structurally critical test — proves the full chain:
    Python register → mdata-shape tuple-form sync → chili-core IPC
    receiver → eval_fn_call W3 branch → PyExternalDispatcher →
    Python callable → result over wire.
    """

    def test_remote_register_and_invoke(self):
        """Mdata-shape end-to-end: register on the receiver, invoke from
        client via `sync(h, (name, *args))`."""
        port = _free_port()

        receiver = ChiliEngine()

        def remote_add(a, b):
            return a + b

        receiver.engine.register_fn(".remote_add", remote_add, 2)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        try:
            client = ChiliEngine()
            h = client.open_handle(f"chili://127.0.0.1:{port}")

            # The mdata-shape API: tuple with bare-string fn name +
            # positional args.
            result = client.sync(h, (".remote_add", 10, 32))
            assert result == 42, (
                f"remote W3 dispatch must return 42, got {result!r} "
                f"of type {type(result).__name__}"
            )
            client.shutdown()
        finally:
            receiver.shutdown()


class TestRegisterFnConcurrent:
    """Audit MC-3 — the `callables` RwLock on PyExternalDispatcher is a
    NEW lock introduced by this sprint. Asserts concurrent
    register/unregister + dispatch threads don't deadlock against the
    dispatch read-clone-out-of-lock path."""

    def test_concurrent_register_and_dispatch(self):
        e = ChiliEngine()
        # Pre-register a stable dispatch target.
        calls = {"n": 0}
        lock = threading.Lock()

        def add_two(a, b):
            with lock:
                calls["n"] += 1
            return a + b

        e.engine.register_fn(".stable", add_two, 2)

        DISPATCH_ITERS = 200
        REGISTER_ITERS = 50
        dispatch_done = threading.Event()
        register_done = threading.Event()
        errors: list[Exception] = []

        def dispatcher_thread():
            try:
                for i in range(DISPATCH_ITERS):
                    r = e.engine.fn_call(".stable", [i, 1])
                    assert r == i + 1
            except Exception as exc:
                errors.append(exc)
            finally:
                dispatch_done.set()

        def register_thread():
            try:
                for i in range(REGISTER_ITERS):
                    name = f".temp_{i}"

                    def temp_fn(x, n=i):
                        return x + n

                    e.engine.register_fn(name, temp_fn, 1)
                    _ = e.engine.unregister_fn(name)
            except Exception as exc:
                errors.append(exc)
            finally:
                register_done.set()

        t1 = threading.Thread(target=dispatcher_thread, name="dispatch")
        t2 = threading.Thread(target=register_thread, name="register")
        t1.start()
        t2.start()
        t1.join(timeout=15.0)
        t2.join(timeout=15.0)

        assert dispatch_done.is_set(), "dispatch thread did not complete (deadlock?)"
        assert register_done.is_set(), "register thread did not complete (deadlock?)"
        assert not errors, f"concurrent threads raised: {errors!r}"
        assert calls["n"] == DISPATCH_ITERS
