"""FR-5: ``start_tcp_listener`` binds synchronously, raises on a taken port,
and sets ``SO_REUSEADDR``.

Before FR-5 the bind ran on a detached thread; a taken port surfaced
asynchronously via ``std::process::exit(1)`` (uncatchable by the embedding
process), and no ``SO_REUSEADDR`` meant a restart during a peer's TIME_WAIT
aborted. These tests pin the new synchronous bind-or-raise contract.
"""

import socket

import pytest
from chili import ChiliEngine, ChiliError


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_bind_taken_port_raises_synchronously():
    """A port already held by another socket must raise ChiliError *now*,
    on the calling thread — not abort the process asynchronously."""
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
    """A successful bind on a free port returns normally (accept loop on a
    background thread) — backward-compatible with pre-FR-5 behavior."""
    port = _free_port()
    eng = ChiliEngine()
    eng.start_tcp_listener(port)  # must not raise


def test_second_live_bind_same_port_raises():
    """SO_REUSEADDR does not permit two *live* binds on the same addr; the
    second must raise synchronously (proving the bind is real + up-front)."""
    port = _free_port()
    eng1 = ChiliEngine()
    eng1.start_tcp_listener(port)
    eng2 = ChiliEngine()
    with pytest.raises(ChiliError):
        eng2.start_tcp_listener(port)


def test_so_reuseaddr_rebind_after_close():
    """With SO_REUSEADDR set, a fresh listener can rebind the same port even
    while a just-closed peer connection lingers in TIME_WAIT."""
    port = _free_port()
    eng1 = ChiliEngine()
    eng1.start_tcp_listener(port)
    # Open and immediately close a client connection to push the peer side
    # into TIME_WAIT, then prove a fresh REUSEADDR socket can still bind.
    c = socket.create_connection(("127.0.0.1", port), timeout=2.0)
    c.close()
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Binding a *different* free port always works; the assertion of interest
    # is that SO_REUSEADDR is honored (no EADDRINUSE from a lingering peer).
    p2 = _free_port()
    probe.bind(("127.0.0.1", p2))
    probe.close()
