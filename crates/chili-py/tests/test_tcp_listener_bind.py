"""Tests for synchronous ``start_tcp_listener`` bind behavior."""

import socket

import pytest
from chili import ChiliEngine, ChiliError


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_bind_taken_port_raises_synchronously():
    port = _free_port()
    hog = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    hog.bind(("127.0.0.1", port))
    hog.listen(1)
    try:
        eng = ChiliEngine()
        with pytest.raises(ChiliError) as ei:
            eng.start_tcp_listener(port)
        assert "in use" in str(ei.value).lower() or "bind failed" in str(ei.value).lower()
    finally:
        hog.close()


def test_clean_bind_on_free_port_returns():
    port = _free_port()
    eng = ChiliEngine()
    eng.start_tcp_listener(port)


def test_second_live_bind_same_port_raises():
    port = _free_port()
    eng1 = ChiliEngine()
    eng1.start_tcp_listener(port)
    eng2 = ChiliEngine()
    with pytest.raises(ChiliError):
        eng2.start_tcp_listener(port)


def test_so_reuseaddr_rebind_after_close():
    port = _free_port()
    eng1 = ChiliEngine()
    eng1.start_tcp_listener(port)
    c = socket.create_connection(("127.0.0.1", port), timeout=2.0)
    c.close()
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    p2 = _free_port()
    probe.bind(("127.0.0.1", p2))
    probe.close()


def _port_bound(port: int) -> bool:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        return False
    except OSError:
        return True
    finally:
        s.close()


def test_shutdown_releases_listen_port():
    """shutdown() must free the port so a new engine can re-bind."""
    import time

    port = _free_port()
    e = ChiliEngine(pepper=True)
    e.start_tcp_listener(port)
    time.sleep(0.1)
    assert _port_bound(port)

    e.shutdown()
    # Accept loop polls every ~20ms; give it a moment to exit and drop its fd.
    deadline = time.time() + 2.0
    while time.time() < deadline and _port_bound(port):
        time.sleep(0.05)
    assert not _port_bound(port), "port still bound after shutdown"

    e2 = ChiliEngine(pepper=True)
    e2.start_tcp_listener(port)
    e2.shutdown()


def test_stop_tcp_listener_releases_port_without_full_shutdown():
    import time

    port = _free_port()
    e = ChiliEngine(pepper=True)
    e.set_var("x", 42)
    e.start_tcp_listener(port)
    time.sleep(0.1)
    e.stop_tcp_listener()
    deadline = time.time() + 2.0
    while time.time() < deadline and _port_bound(port):
        time.sleep(0.05)
    assert not _port_bound(port)
    assert e.get_var("x") == 42
    e.shutdown()


def test_shutdown_stops_serving():
    """After shutdown, the old listener must not accept/serve anymore."""
    import time

    port = _free_port()
    e = ChiliEngine(pepper=True)
    e.set_var("x", 42)
    e.start_tcp_listener(port)
    time.sleep(0.1)
    e.shutdown()
    deadline = time.time() + 2.0
    while time.time() < deadline and _port_bound(port):
        time.sleep(0.05)

    c = ChiliEngine(pepper=True)
    with pytest.raises(Exception):
        c.open_handle(f"chili://127.0.0.1:{port}")
    c.shutdown()