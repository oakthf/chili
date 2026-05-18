//! Sprint 21 / ADR-0006 — committed guards for the D-1 GIL-free upd
//! notification primitive (`chili_core::UpdNotify`).
//!
//! GIL-free invariant (structural, not runtime-assertable): this whole
//! path lives in `chili-core`, which has **zero** `pyo3` dependency
//! (`grep pyo3 crates/chili-core/Cargo.toml` is empty). The enqueue +
//! self-pipe `write` therefore *cannot* touch the GIL — it has no
//! `Python` symbol in scope to touch. The Python-visible `#[pyclass]`
//! wrapper lives in `chili-py`; `drain_upds` takes the GIL only on the
//! distinct Python caller thread. These tests cover the back-pressure
//! contract (ADR-0006 §3), the self-pipe wakeup, and the Q5
//! close-on-exec + non-blocking fd flags (ADR-0006 §1).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use chili_core::{UPD_QUEUE_CAP, UpdEventCore, UpdNotify};
use polars::prelude::*;

fn ev(n: i64) -> UpdEventCore {
    let frame = df!["seq" => [n]].unwrap();
    UpdEventCore {
        table: "trade".to_string(),
        cursor_lo: n - 1,
        cursor_hi: n,
        frame,
    }
}

/// ADR-0006 §3 — at capacity the sender **blocks** (back-pressure) and
/// **never drops**: every enqueued item is eventually drained, in FIFO
/// order, including the one that blocked.
#[test]
fn back_pressure_blocks_and_never_drops() {
    let n = UpdNotify::new().expect("self-pipe");
    let n = Arc::new(n);

    // Fill the bounded queue exactly to capacity (non-blocking — there
    // is room for precisely UPD_QUEUE_CAP items).
    for i in 0..UPD_QUEUE_CAP as i64 {
        n.enqueue(ev(i));
    }

    // The (CAP+1)-th enqueue must block until a slot frees.
    let completed = Arc::new(AtomicBool::new(false));
    let n2 = Arc::clone(&n);
    let done = Arc::clone(&completed);
    let blocker = thread::spawn(move || {
        n2.enqueue(ev(UPD_QUEUE_CAP as i64));
        done.store(true, Ordering::SeqCst);
    });

    // Give the blocker time to reach the blocking send. It must NOT
    // have completed (queue is full → blocked, not dropped).
    thread::sleep(Duration::from_millis(150));
    assert!(
        !completed.load(Ordering::SeqCst),
        "enqueue at capacity must block, not drop or return"
    );

    // Draining frees slots → the blocked enqueue completes.
    let drained_first = n.drain();
    assert!(
        !drained_first.is_empty(),
        "drain at capacity must yield items"
    );
    blocker.join().expect("blocked enqueue thread");
    assert!(
        completed.load(Ordering::SeqCst),
        "blocked enqueue must complete once a slot frees"
    );

    // Drain the rest; total delivered == CAP+1 (nothing dropped) and
    // strictly FIFO by cursor_hi.
    let mut all: Vec<i64> = drained_first.iter().map(|e| e.cursor_hi).collect();
    // The blocked item + any not yet drained may still be queued.
    loop {
        let more = n.drain();
        if more.is_empty() {
            // One short retry window for the just-unblocked send.
            thread::sleep(Duration::from_millis(50));
            let last = n.drain();
            if last.is_empty() {
                break;
            }
            all.extend(last.iter().map(|e| e.cursor_hi));
            continue;
        }
        all.extend(more.iter().map(|e| e.cursor_hi));
    }
    assert_eq!(
        all.len(),
        UPD_QUEUE_CAP + 1,
        "every enqueued item must be delivered — no drop path"
    );
    let mut sorted = all.clone();
    sorted.sort_unstable();
    assert_eq!(all, sorted, "delivery order must be FIFO (monotone cursor)");
    assert_eq!(*all.first().unwrap(), 0);
    assert_eq!(*all.last().unwrap(), UPD_QUEUE_CAP as i64);
}

/// The self-pipe fd is readable after an enqueue and quiet again after
/// a drain (so `add_reader` re-arms only on genuinely-new data). Also
/// asserts the drained event round-trips intact.
#[test]
fn fd_signals_then_quiesces_after_drain() {
    let n = UpdNotify::new().expect("self-pipe");
    let fd = n.read_fd();

    assert!(!fd_readable(fd), "fresh self-pipe must not be readable");

    n.enqueue(ev(42));
    assert!(
        fd_readable(fd),
        "fd must be readable after an enqueue (wakeup byte written)"
    );

    let got = n.drain();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].table, "trade");
    assert_eq!(got[0].cursor_lo, 41);
    assert_eq!(got[0].cursor_hi, 42);
    assert_eq!(got[0].frame.height(), 1);

    assert!(
        !fd_readable(fd),
        "fd must quiesce after drain — no spurious re-arm"
    );
}

/// ADR-0006 §1 — both pipe ends are `FD_CLOEXEC` (Q5: must not leak
/// into a `multiprocessing` child across exec) and the read end is
/// `O_NONBLOCK` (drain must never block the Python caller thread).
#[test]
fn fd_has_cloexec_and_nonblock() {
    let n = UpdNotify::new().expect("self-pipe");
    let fd = n.read_fd();

    // SAFETY: `fd` is a live owned pipe fd for the lifetime of `n`.
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(fd_flags >= 0, "F_GETFD failed");
    assert!(
        fd_flags & libc::FD_CLOEXEC != 0,
        "self-pipe read end must be FD_CLOEXEC (Q5 close-on-exec)"
    );

    let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(fl >= 0, "F_GETFL failed");
    assert!(
        fl & libc::O_NONBLOCK != 0,
        "self-pipe read end must be O_NONBLOCK (non-blocking drain)"
    );
}

/// `poll(2)` the fd for read-readiness with a zero timeout.
fn fd_readable(fd: libc::c_int) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: single valid pollfd, zero timeout (non-blocking probe).
    let rc = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, 0) };
    rc > 0 && (pfd.revents & libc::POLLIN) != 0
}
