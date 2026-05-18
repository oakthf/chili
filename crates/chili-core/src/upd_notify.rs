//! Outbound GIL-free `upd` delivery notification (ADR-0006 — mdata
//! push-model D-1).
//!
//! The pure-Rust IPC receive thread (`utils::handle_chili_conn`) applies
//! an inbound `(`upd; table; data)` and, when a Python subscriber has
//! armed notification, hands the applied delta to this queue + pokes a
//! POSIX self-pipe. Python `add_reader`s the pipe read end and drains.
//!
//! Design contract (ADR-0006):
//!  * §1 notification primitive = **POSIX self-pipe**, not `eventfd(2)`
//!    (macOS — the dev + delivered-wheel target — has no `eventfd`).
//!    Both ends are `O_NONBLOCK` (read: drain; write: ignore-`EAGAIN`
//!    coalescing) and `FD_CLOEXEC` (Q5: must not leak into a
//!    `multiprocessing` child).
//!  * §2 `UpdEventCore.cursor_*` = the per-handle `tick_count` delivery
//!    ordinal **before/after** this batch, NOT mdata's per-row `seq`.
//!  * §3 bounded queue, **`N = UPD_QUEUE_CAP`**; back-pressure =
//!    **blocking send, never drop** (the tplog is the source of truth;
//!    a slow Python drainer back-pressures the upstream tp, kdb+-like).
//!
//! GIL invariant: nothing in this module touches `pyo3` / `Python` /
//! `with_gil`. It is pure Rust and lives in `chili-core`; the
//! Python-visible `#[pyclass] UpdEvent` wrapper lives in `chili-py`.

use crossbeam_channel::{Receiver, Sender, bounded};
use polars::prelude::DataFrame;
use std::os::fd::RawFd;

/// ADR-0006 §3 — bounded upd-notify queue capacity. A fixed power-of-two
/// constant this sprint; runtime tunability is a documented future
/// concern, not built (mdata's capacity acceptance test needs a
/// deterministic value).
pub const UPD_QUEUE_CAP: usize = 4096;

/// One applied `upd` batch handed to the Python subscriber.
///
/// `cursor_lo`/`cursor_hi` are the per-handle `tick_count` delivery
/// ordinal *before*/*after* the batch's `tick[this.h; 1]` (ADR-0006
/// §2) — a monotonic per-handle delivery position, **explicitly not**
/// mdata's per-row `seq` column (Q1 Path-1: per-table contiguity is the
/// caller's own `seq`).
#[derive(Debug, Clone)]
pub struct UpdEventCore {
    pub table: String,
    pub cursor_lo: i64,
    pub cursor_hi: i64,
    /// The raw delta as sent by the tp (Q3). A Polars `DataFrame` is a
    /// shallow Arc-clone of its column buffers — no re-serialize /
    /// re-decode (ADR-0006 §2; not literally zero-copy).
    pub frame: DataFrame,
}

/// The GIL-free notification channel: a bounded queue + a self-pipe.
///
/// Stored as `Arc<UpdNotify>` in `EngineState`; lazily created on the
/// first `enable_upd_notify()` so non-subscribers pay nothing.
pub struct UpdNotify {
    tx: Sender<UpdEventCore>,
    rx: Receiver<UpdEventCore>,
    /// self-pipe read end — exposed to Python via `upd_notify_fd()`.
    pipe_r: RawFd,
    /// self-pipe write end — poked by the receive thread.
    pipe_w: RawFd,
}

impl UpdNotify {
    /// Create the self-pipe + bounded queue.
    ///
    /// macOS has neither `eventfd` nor `pipe2`, so we `pipe(2)` then
    /// `fcntl` each end `O_NONBLOCK` + `FD_CLOEXEC`. The CLOEXEC set is
    /// non-atomic vs the `pipe` call; that window is benign — this runs
    /// single-threaded at first-`enable` and mdata uses `spawn`, not
    /// `fork` (the CLOEXEC is the defensive belt mdata requested, Q5).
    pub fn new() -> std::io::Result<Self> {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `fds` is a valid 2-element array; `pipe` writes both.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let (pipe_r, pipe_w) = (fds[0], fds[1]);
        for &fd in &[pipe_r, pipe_w] {
            // SAFETY: `fd` is a freshly-created valid pipe fd.
            unsafe {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                if fl != -1 {
                    libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
                }
                let fd_fl = libc::fcntl(fd, libc::F_GETFD);
                if fd_fl != -1 {
                    libc::fcntl(fd, libc::F_SETFD, fd_fl | libc::FD_CLOEXEC);
                }
            }
        }
        let (tx, rx) = bounded(UPD_QUEUE_CAP);
        Ok(Self {
            tx,
            rx,
            pipe_r,
            pipe_w,
        })
    }

    /// The self-pipe read end. `O_NONBLOCK` + `FD_CLOEXEC`; safe to
    /// `asyncio.add_reader` / `kqueue`. Must not be used across
    /// `os.fork` without re-creation (ADR-0006 §1).
    pub fn read_fd(&self) -> RawFd {
        self.pipe_r
    }

    /// Receive-thread side. **GIL-free** (no `pyo3`/`Python` anywhere).
    ///
    /// Blocking send = back-pressure, never drop (ADR-0006 §3): at
    /// capacity the receive thread blocks here, which back-pressures
    /// the upstream tp's blocking socket write. The tplog stays the
    /// source of truth; Python catches up. There is no drop path.
    pub fn enqueue(&self, ev: UpdEventCore) {
        if self.tx.send(ev).is_err() {
            // All receivers dropped (engine tearing down). Nothing to
            // signal; the queued event is irrelevant past shutdown.
            return;
        }
        // Coalesced 1-byte wakeup. Non-blocking: `EAGAIN` (pipe buffer
        // full) is benign — an unread wakeup byte is already pending,
        // so the drainer will wake regardless. The bounded queue caps
        // outstanding items at UPD_QUEUE_CAP << the 64 KiB pipe buffer,
        // so this write never actually blocks in practice.
        let b: u8 = 1;
        // SAFETY: `pipe_w` is a valid owned fd; writing 1 byte from a
        // local. Return value intentionally ignored (see above).
        unsafe {
            libc::write(self.pipe_w, &b as *const u8 as *const libc::c_void, 1);
        }
    }

    /// Python-caller-thread side. Non-blocking; drains the pipe **first**
    /// then the queue.
    ///
    /// Edge-safe ordering: an item enqueued *after* the pipe drain but
    /// *before* the queue drain is still returned by this call AND
    /// leaves its wakeup byte in the pipe → the next `add_reader` fire
    /// is a harmless spurious empty drain. The inverse order could
    /// strand an item until the next unrelated wakeup; this order
    /// never misses one.
    pub fn drain(&self) -> Vec<UpdEventCore> {
        let mut sink = [0u8; 256];
        loop {
            // SAFETY: `pipe_r` is a valid owned fd; `sink` is a valid
            // local buffer of `sink.len()` bytes.
            let n = unsafe {
                libc::read(
                    self.pipe_r,
                    sink.as_mut_ptr() as *mut libc::c_void,
                    sink.len(),
                )
            };
            // n <= 0 → EOF (impossible: we hold the write end) or
            // EAGAIN (pipe empty). Either way, done draining bytes.
            if n <= 0 {
                break;
            }
        }
        let mut out = Vec::new();
        while let Ok(ev) = self.rx.try_recv() {
            out.push(ev);
        }
        out
    }
}

impl Drop for UpdNotify {
    fn drop(&mut self) {
        // SAFETY: both fds were created by `pipe` in `new` and are
        // owned solely by this struct (single Arc owner per engine).
        unsafe {
            libc::close(self.pipe_r);
            libc::close(self.pipe_w);
        }
    }
}
