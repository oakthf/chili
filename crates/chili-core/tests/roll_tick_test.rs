//! Sprint 18 — `roll_tick` tplog segment-rollover correctness harness.
//!
//! RED-FIRST (audit MAJOR-3): this file is built BEFORE `roll_tick`
//! exists. It proves the harness has TEETH by demonstrating the
//! pre-fix close-then-reopen roll (`close_handle` + `open_handle`, two
//! separate `handle.write()` acquisitions — exactly what `tick.pep:9-10`
//! `.tick.createLog` lowers to) LOSES a concurrent inbound write.
//!
//! After Part A lands `roll_tick`, `roll_tick_zero_loss_*` tests (added
//! in Part B.1) drive the SAME `run_roll_scenario` harness with the
//! `roll_tick` roll impl and assert zero loss / crisp boundary.
//!
//! Independent oracle (audit principle 4): `read_tplog_ints` parses the
//! on-disk frame stream by hand — 8-byte `[255,0,0,0,…]` file magic,
//! then repeating `[len:u64 LE | ts:u64 LE | payload]` (verified against
//! `engine_state.rs:1044-1066` New→Sequence and `:1085-1108` Sequence,
//! and `broker.rs:86-108` `validate_seq`). Payload is decoded with
//! `serde9::deserialize` (the READ path — structurally independent of
//! `sync()`'s `serde9::serialize` + framing WRITE path that the bug
//! lives in). The harness never trusts chili's writer to verify itself.

use std::sync::{Arc, Barrier, mpsc};
use std::thread;

use chili_core::{EngineState, SpicyError, SpicyObj, serde9};

/// One inbound `.tick.upd`-shaped message carrying a recoverable int.
/// `(`upd; `trade; i)` — `sync()`'s New/Sequence branch serializes any
/// `MixedList`; the payload contents are irrelevant to the handle race,
/// so we use a trailing `I64` instead of a Polars df to keep the test
/// focused on the framing/handle surface (not df marshalling).
fn msg(i: i64) -> SpicyObj {
    SpicyObj::MixedList(vec![
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol("trade".into()),
        SpicyObj::I64(i),
    ])
}

/// Independent raw-frame oracle. Returns the ordered payload ints found
/// in the tplog at `path`. A torn/partial trailing frame is ignored
/// (matches `validate_seq`'s truncation semantics). Empty/missing file
/// → empty vec.
fn read_tplog_ints(path: &str) -> Vec<i64> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(_) => return vec![],
    };
    if bytes.len() < 8 {
        return vec![];
    }
    assert_eq!(
        &bytes[0..4],
        &[255, 0, 0, 0],
        "tplog {path} missing the 8-byte sequence magic header"
    );
    let mut pos = 8usize;
    let mut out = Vec::new();
    while pos + 16 <= bytes.len() {
        let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        // bytes[pos+8 .. pos+16] = timestamp (skipped by the oracle)
        pos += 16;
        if len == 0 || pos + len > bytes.len() {
            break; // torn tail — ignored, as validate_seq does
        }
        let mut dp = 0usize;
        let obj = serde9::deserialize(&bytes[pos..pos + len], &mut dp)
            .expect("oracle: serde9 read-path decode of a complete frame");
        let v = obj.as_vec().expect("oracle: frame payload is a MixedList");
        let i = v
            .last()
            .expect("oracle: non-empty MixedList")
            .to_i64()
            .expect("oracle: trailing element is the I64 key");
        out.push(i);
        pos += len;
    }
    out
}

fn new_engine() -> Arc<EngineState> {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    Arc::new(state)
}

fn open_seg(engine: &Arc<EngineState>, path: &str) -> i64 {
    match engine.open_handle(&format!("file://{path}"), 0).unwrap() {
        SpicyObj::I64(h) => h,
        other => panic!("open_handle returned non-i64: {other:?}"),
    }
}

/// The pre-fix roll: close the live handle then reopen the next segment
/// as a NEW handle id — two separate `handle.write()` critical sections
/// with the old id `shift_remove`d in between. This is exactly what
/// `.tick.createLog` (`tick.pep:9` then `:10`) does today. Returns the
/// new segment's handle id (the `.tick.msgHandle` reassignment).
fn legacy_roll(engine: &Arc<EngineState>, old_h: i64, next_path: &str) -> i64 {
    engine.close_handle(&old_h).unwrap(); // tick.pep:9  — handle.write() #1
    open_seg(engine, next_path) //          tick.pep:10 — handle.write() #2
}

/// Deterministic teeth — FAILURE MODE 1 (mid-gap loss). The writer
/// resolves the live tplog id (== `.tick.upd`'s `get[`.tick.msgHandle]`)
/// and fires its `sync()` precisely in the window AFTER `close_handle`
/// but BEFORE `open_handle` — the 2-statement gap `tick.pep:9`→`:10`.
/// The id is absent → `InvalidHandleErr` → the message is LOST (in
/// neither segment). This is mdata's verdict (b). MUST hold or the
/// harness is blind. `roll_tick` has no such gap (Part B.1 mirror).
#[test]
fn teeth_legacy_gap_loses_a_concurrent_write() {
    let dir = tempfile::tempdir().unwrap();
    let seg0 = dir.path().join("seg_0000");
    let seg1 = dir.path().join("seg_0001");
    let seg0s = seg0.to_str().unwrap().to_owned();
    let seg1s = seg1.to_str().unwrap().to_owned();

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);

    const P: i64 = 5;
    for i in 0..P {
        engine.sync(&h0, &msg(i)).unwrap(); // pre-roll → seg0 (+ magic)
    }

    let gate = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();

    let w_engine = Arc::clone(&engine);
    let w_gate = Arc::clone(&gate);
    let writer = thread::spawn(move || {
        let h = h0; // resolved `.tick.msgHandle`
        w_gate.wait(); // A: close() has happened, open() has NOT
        let res = w_engine.sync(&h, &msg(P)); // fires inside the gap
        tx.send(res).unwrap();
        w_gate.wait(); // B: release main to open() only after the sync
    });

    engine.close_handle(&h0).unwrap(); // tick.pep:9  (handle.write() #1)
    gate.wait(); // A — writer now syncs into the close→open gap
    gate.wait(); // B — writer's gap sync has completed
    let h1 = open_seg(&engine, &seg1s); // tick.pep:10 (handle.write() #2)
    engine.sync(&h1, &msg(9_999)).unwrap(); // post-roll marker → seg1

    let racing = rx.recv().unwrap();
    writer.join().unwrap();
    let in_seg0 = read_tplog_ints(&seg0s);
    let in_seg1 = read_tplog_ints(&seg1s);

    assert!(
        matches!(racing, Err(SpicyError::InvalidHandleErr(_))),
        "legacy gap: stale-id sync must hit InvalidHandleErr, got {racing:?}"
    );
    assert!(
        !in_seg0.contains(&P) && !in_seg1.contains(&P),
        "TEETH: message {P} must be LOST in the close→open gap \
         (seg0={in_seg0:?}, seg1={in_seg1:?})"
    );
    assert_eq!(in_seg0, (0..P).collect::<Vec<_>>(), "pre-roll seg0 intact");
    assert_eq!(in_seg1, vec![9_999], "post-roll marker in seg1");
}

/// Deterministic teeth — FAILURE MODE 2 (post-roll id-reuse →
/// wrong-segment misplacement). `set_handle` (`engine_state.rs:874-878`)
/// allocates `1 + max(keys)`; for a single-tplog-handle tickerplant
/// (mdata's exact topology) `close_handle` empties the map so the
/// re-opened segment re-derives the SAME id. A writer that resolved the
/// pre-roll id, parked across the FULL legacy roll, then syncs, writes
/// SUCCESSFULLY (no error) but into the NEW segment — a row that
/// belongs in seg0 lands in seg1. Silent partition/SEQ-invariant
/// corruption, no crisp boundary. `roll_tick`'s atomic same-id swap
/// makes the swap point the exact boundary (Part B.1 asserts the fix).
#[test]
fn teeth_legacy_idreuse_misplaces_a_concurrent_write() {
    let dir = tempfile::tempdir().unwrap();
    let seg0 = dir.path().join("seg_0000");
    let seg1 = dir.path().join("seg_0001");
    let seg0s = seg0.to_str().unwrap().to_owned();
    let seg1s = seg1.to_str().unwrap().to_owned();

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    const P: i64 = 5;
    for i in 0..P {
        engine.sync(&h0, &msg(i)).unwrap();
    }

    let gate = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();
    let w_engine = Arc::clone(&engine);
    let w_gate = Arc::clone(&gate);
    let writer = thread::spawn(move || {
        let h = h0;
        w_gate.wait(); // A: full legacy roll completed
        let res = w_engine.sync(&h, &msg(P)); // stale id, now == reused seg1 id
        tx.send(res).unwrap();
    });

    let h1 = legacy_roll(&engine, h0, &seg1s); // close(h0) then open(seg1)
    assert_eq!(
        h1, h0,
        "single-handle topology: open re-derives the freed id"
    );
    engine.sync(&h1, &msg(9_999)).unwrap();
    gate.wait(); // A — release writer to sync against the (reused) id

    let racing = rx.recv().unwrap();
    writer.join().unwrap();
    let in_seg0 = read_tplog_ints(&seg0s);
    let in_seg1 = read_tplog_ints(&seg1s);

    assert!(
        racing.is_ok(),
        "id-reuse: stale-id sync SUCCEEDS, got {racing:?}"
    );
    assert!(
        !in_seg0.contains(&P) && in_seg1.contains(&P),
        "TEETH: message {P} resolved the pre-roll handle but was MISPLACED \
         into seg1 (seg0={in_seg0:?}, seg1={in_seg1:?}) — crisp boundary \
         violated with NO error raised"
    );
}

/// Randomized realistic teeth check: K writer threads hammer
/// `sync(resolve-current-handle)` (modelling `.tick.upd` re-reading
/// `.tick.msgHandle` each call) while the main thread performs a legacy
/// roll mid-stream. Across the run, at least one concurrent write must
/// be lost (InvalidHandleErr against the just-removed id). Proves the
/// bug reproduces under unstructured timing, not only the orchestrated
/// interleaving.
#[test]
fn teeth_legacy_roll_loses_under_concurrent_load() {
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let seg0 = dir.path().join("seg_0000");
    let seg1 = dir.path().join("seg_0001");
    let seg0s = seg0.to_str().unwrap().to_owned();
    let seg1s = seg1.to_str().unwrap().to_owned();

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    engine.sync(&h0, &msg(-1)).unwrap(); // seed magic header in seg0

    // Models the `.tick.msgHandle` pepper var the writers re-resolve.
    let msg_handle = Arc::new(RwLock::new(h0));
    let stop = Arc::new(AtomicBool::new(false));
    let next_int = Arc::new(AtomicI64::new(0));
    let lost = Arc::new(AtomicI64::new(0));
    let sent = Arc::new(AtomicI64::new(0));

    let mut writers = Vec::new();
    for _ in 0..4 {
        let e = Arc::clone(&engine);
        let mh = Arc::clone(&msg_handle);
        let st = Arc::clone(&stop);
        let ni = Arc::clone(&next_int);
        let ls = Arc::clone(&lost);
        let sn = Arc::clone(&sent);
        writers.push(thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                let i = ni.fetch_add(1, Ordering::Relaxed);
                let h = *mh.read().unwrap(); // resolve `.tick.msgHandle`
                sn.fetch_add(1, Ordering::Relaxed);
                match e.sync(&h, &msg(i)) {
                    Ok(_) => {}
                    Err(SpicyError::InvalidHandleErr(_)) => {
                        ls.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(other) => panic!("unexpected sync error: {other:?}"),
                }
            }
        }));
    }

    // Let writers ramp, then roll mid-stream (legacy: id changes).
    thread::sleep(std::time::Duration::from_millis(20));
    let h1 = legacy_roll(&engine, h0, &seg1s);
    engine.sync(&h1, &msg(-2)).unwrap(); // seed magic header in seg1
    *msg_handle.write().unwrap() = h1;
    thread::sleep(std::time::Duration::from_millis(20));

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    let lost_n = lost.load(Ordering::Relaxed);
    assert!(
        lost_n > 0,
        "TEETH: legacy roll under {} concurrent writes must drop ≥1 \
         (InvalidHandleErr against the removed id); lost={lost_n}. A green \
         here means the harness can't see the race.",
        sent.load(Ordering::Relaxed)
    );
}

// ─────────────────────────────────────────────────────────────────────
// Part B.1 — the SAME harness, now driving the real `roll_tick` impl.
// Every test below is the green mirror of a teeth test above.
// ─────────────────────────────────────────────────────────────────────

/// Set `.tick.msgHandle` so `roll_tick` can resolve the live handle
/// (models what `init_tick`/`createLog` leave behind).
fn set_msg_handle(engine: &Arc<EngineState>, h: i64) {
    engine.set_var(".tick.msgHandle", SpicyObj::I64(h)).unwrap();
}

/// Deterministic crisp-boundary proof. Same orchestration as
/// `teeth_legacy_idreuse_misplaces` (writer resolves the live id, parks
/// across the FULL roll, then syncs) — but with `roll_tick` the write
/// is NOT misplaced: it lands in the NEW segment because the atomic
/// same-id swap completed before the writer's `sync()` acquired the
/// lock. The swap point is the exact boundary: {0..P} in seg0, P in
/// seg1, zero loss, zero dup.
#[test]
fn roll_tick_deterministic_crisp_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let log_dir = format!("{base}/");
    let seg0s = format!("{base}/0000");
    let seg1s = format!("{base}/0001");

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    set_msg_handle(&engine, h0);
    const P: i64 = 5;
    for i in 0..P {
        engine.sync(&h0, &msg(i)).unwrap();
    }

    let gate = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();
    let w_engine = Arc::clone(&engine);
    let w_gate = Arc::clone(&gate);
    let writer = thread::spawn(move || {
        let h = h0; // resolved `.tick.msgHandle` (unchanged by roll_tick)
        w_gate.wait(); // A: roll_tick has fully completed
        let res = w_engine.sync(&h, &msg(P)); // same id — still valid
        tx.send(res).unwrap();
    });

    engine.roll_tick(&log_dir, "0001").unwrap(); // atomic, same id
    gate.wait(); // A — release writer

    let racing = rx.recv().unwrap();
    writer.join().unwrap();
    let in_seg0 = read_tplog_ints(&seg0s);
    let in_seg1 = read_tplog_ints(&seg1s);

    assert!(
        racing.is_ok(),
        "roll_tick: stale-resolved id stays valid, sync must succeed, got {racing:?}"
    );
    assert_eq!(
        in_seg0,
        (0..P).collect::<Vec<_>>(),
        "pre-roll wholly in seg0"
    );
    assert_eq!(in_seg1, vec![P], "post-roll write wholly in seg1");
    assert!(
        !in_seg0.contains(&P) && !in_seg1.iter().any(|&i| i < P),
        "crisp boundary: no message straddles or is misplaced"
    );
}

/// THE proof — same scenario as `teeth_legacy_roll_loses_under_concurrent_load`
/// but rolled via `roll_tick`: zero loss, zero dup, conservation, and
/// per-publisher monotone + clean prefix/suffix split (mdata SEQ-MONO).
#[test]
fn roll_tick_zero_loss_under_concurrent_load() {
    use std::sync::atomic::{AtomicBool, Ordering};

    const K: i64 = 4;
    const NS: i64 = 1_000_000; // per-writer int namespace stride

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let log_dir = format!("{base}/");
    let seg0s = format!("{base}/0000");
    let seg1s = format!("{base}/0001");

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    set_msg_handle(&engine, h0);

    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..K {
        let e = Arc::clone(&engine);
        let st = Arc::clone(&stop);
        writers.push(thread::spawn(move || {
            // Each writer owns int namespace [w*NS, (w+1)*NS).
            // h0 stays the live id across roll_tick (same-id swap), so
            // writers need not re-resolve `.tick.msgHandle` each call —
            // the re-resolve path is covered by the legacy teeth load
            // test (RwLock msg var) and the Tier-2 TCP suite.
            let mut sent_ok = Vec::new();
            let mut local = 0i64;
            let mut invalid = 0i64;
            while !st.load(Ordering::Relaxed) {
                let v = w * NS + local;
                match e.sync(&h0, &msg(v)) {
                    Ok(_) => sent_ok.push(v),
                    Err(SpicyError::InvalidHandleErr(_)) => invalid += 1,
                    Err(other) => panic!("unexpected sync error: {other:?}"),
                }
                local += 1;
            }
            (sent_ok, invalid)
        }));
    }

    thread::sleep(std::time::Duration::from_millis(20));
    engine.roll_tick(&log_dir, "0001").unwrap(); // roll mid-stream
    thread::sleep(std::time::Duration::from_millis(20));
    stop.store(true, Ordering::Relaxed);

    let mut per_writer: Vec<Vec<i64>> = vec![Vec::new(); K as usize];
    let mut total_invalid = 0i64;
    for (w, jh) in writers.into_iter().enumerate() {
        let (ok, invalid) = jh.join().unwrap();
        per_writer[w] = ok;
        total_invalid += invalid;
    }

    let seg0 = read_tplog_ints(&seg0s);
    let seg1 = read_tplog_ints(&seg1s);

    assert_eq!(
        total_invalid, 0,
        "roll_tick: ZERO InvalidHandleErr expected (same-id swap), got {total_invalid}"
    );

    use std::collections::HashSet;
    let s0: HashSet<i64> = seg0.iter().copied().collect();
    let s1: HashSet<i64> = seg1.iter().copied().collect();
    assert!(
        s0.is_disjoint(&s1),
        "no duplication across the boundary (s0∩s1 must be ∅)"
    );
    let expected: HashSet<i64> = per_writer.iter().flatten().copied().collect();
    let union: HashSet<i64> = s0.union(&s1).copied().collect();
    assert_eq!(
        union, expected,
        "zero loss + full coverage: seg0∪seg1 == every successfully-sync'd int"
    );
    assert_eq!(
        seg0.len() + seg1.len(),
        expected.len(),
        "conservation: total frames == total successful syncs (no dup-with-loss)"
    );

    // Per-publisher SEQ-MONO + crisp split: writer w's frames, read in
    // file order (seg0 then seg1), must equal exactly the order it sent
    // them — proving no loss, no reorder, and a single clean cut point.
    for (w, sent) in per_writer.iter().enumerate() {
        let lo = w as i64 * NS;
        let hi = lo + NS;
        let in0: Vec<i64> = seg0
            .iter()
            .copied()
            .filter(|&i| i >= lo && i < hi)
            .collect();
        let in1: Vec<i64> = seg1
            .iter()
            .copied()
            .filter(|&i| i >= lo && i < hi)
            .collect();
        let mut joined = in0.clone();
        joined.extend(in1.iter().copied());
        assert_eq!(
            &joined, sent,
            "writer {w}: seg0-part ++ seg1-part must equal its send order \
             (SEQ-MONO + single boundary; seg0={in0:?} seg1={in1:?})"
        );
    }
}

/// Failure-atomicity: a next segment that cannot be opened (parent dir
/// absent) must leave the live segment fully writable — no half-roll.
#[test]
fn roll_tick_failure_atomicity_old_segment_survives() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let seg0s = format!("{base}/0000");

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    set_msg_handle(&engine, h0);
    engine.sync(&h0, &msg(1)).unwrap();

    // Parent directory does not exist → open/create fails pre-lock.
    let bad = engine.roll_tick("/no/such/dir_xyz/", "seg");
    assert!(
        bad.is_err(),
        "roll_tick to an unopenable path must Err, got {bad:?}"
    );

    // The old segment is untouched and still the live writer.
    engine.sync(&h0, &msg(2)).unwrap();
    assert_eq!(
        read_tplog_ints(&seg0s),
        vec![1, 2],
        "old segment must remain writable after a failed roll"
    );
}

/// Idempotent retry: a second `roll_tick` to the same label is a safe
/// no-op (EodScheduler retry path) — content stable, handle still live.
#[test]
fn roll_tick_idempotent_repeat_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let log_dir = format!("{base}/");
    let seg0s = format!("{base}/0000");
    let seg1s = format!("{base}/0001");

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    set_msg_handle(&engine, h0);
    engine.sync(&h0, &msg(1)).unwrap();

    engine.roll_tick(&log_dir, "0001").unwrap();
    engine.sync(&h0, &msg(2)).unwrap();
    engine.roll_tick(&log_dir, "0001").unwrap(); // idempotent no-op
    engine.sync(&h0, &msg(3)).unwrap();

    assert_eq!(read_tplog_ints(&seg0s), vec![1], "seg0 closed at the roll");
    assert_eq!(
        read_tplog_ints(&seg1s),
        vec![2, 3],
        "seg1 keeps accumulating across the idempotent repeat (no truncation/reopen)"
    );
}

#[test]
fn roll_tick_empty_label_errs() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let engine = new_engine();
    let h0 = open_seg(&engine, &format!("{base}/0000"));
    set_msg_handle(&engine, h0);
    assert!(
        engine.roll_tick(&format!("{base}/"), "").is_err(),
        "empty segment_label must Err"
    );
}

#[test]
fn roll_tick_unset_handle_errs_without_panic() {
    let engine = new_engine();
    // `.tick.msgHandle` never set.
    let r = engine.roll_tick("/tmp/", "0001");
    assert!(
        r.is_err(),
        "unset .tick.msgHandle must Err (not panic), got {r:?}"
    );
}

/// Concurrent double-roll to the SAME label (e.g. an EodScheduler that
/// double-fired): both calls return Ok, exactly one performs the swap,
/// pre-roll data is intact and not duplicated. (Pre-roll-only writers;
/// callers must single-flight rolls vs. live writes — see roll_tick
/// doc — so this asserts the idempotent-under-concurrency contract.)
#[test]
fn roll_tick_concurrent_double_same_label() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_owned();
    let log_dir = format!("{base}/");
    let seg0s = format!("{base}/0000");
    let seg1s = format!("{base}/0001");

    let engine = new_engine();
    let h0 = open_seg(&engine, &seg0s);
    set_msg_handle(&engine, h0);
    for i in 0..5 {
        engine.sync(&h0, &msg(i)).unwrap();
    }

    let start = Arc::new(Barrier::new(2));
    let mut rollers = Vec::new();
    for _ in 0..2 {
        let e = Arc::clone(&engine);
        let b = Arc::clone(&start);
        let ld = log_dir.clone();
        rollers.push(thread::spawn(move || {
            b.wait();
            e.roll_tick(&ld, "0001")
        }));
    }
    let results: Vec<_> = rollers.into_iter().map(|r| r.join().unwrap()).collect();
    engine.sync(&h0, &msg(99)).unwrap(); // post-roll write via same id

    assert!(
        results.iter().all(|r| r.is_ok()),
        "both concurrent roll_tick(same label) calls must be Ok, got {results:?}"
    );
    assert_eq!(
        read_tplog_ints(&seg0s),
        (0..5).collect::<Vec<_>>(),
        "pre-roll data intact in seg0, exactly once"
    );
    assert_eq!(
        read_tplog_ints(&seg1s),
        vec![99],
        "exactly one cutover happened; post-roll write in seg1"
    );
}
