//! M-2 Stage 2 — slow-subscriber shed via write-timeout + socket shutdown.
//!
//! Stage 1 moved blocking socket I/O off the global handle lock (per-handle
//! mutex). Stage 2 adds an OPT-IN per-write timeout on INCOMING (subscriber)
//! sockets: a subscriber that stops reading fills its TCP send buffer, so the
//! tp's blocking `write_all` would hang the publish loop forever. With the
//! timeout set, that write times out, the handle is marked `Disconnected`, AND
//! its socket is `shutdown(Both)` so the peer gets a reset (not a stuck read)
//! and its reconnect/replay logic re-subscribes.
//!
//! This is an end-to-end networked test: a real listener accepts a real
//! subscriber socket; we register that handle as `Publishing` on a topic and
//! publish large frames until the write times out, then assert the shed
//! (handle `Disconnected` + the subscriber socket is torn down → a peer read
//! returns EOF/error). Determinism: the subscriber's `SO_RCVBUF` is shrunk so
//! the kernel buffers fill in a couple of large frames, and the publish loop is
//! bounded with a wall-clock guard so the test can never hang.

use std::{
    io::Read,
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use chili_core::{EngineState, SpicyObj, utils::send_auth};

/// Bind a listener on an ephemeral port, run the accept loop on a background
/// thread, and return the engine handle + the port it is listening on.
fn start_server(write_timeout_ms: i64) -> (Arc<EngineState>, u16) {
    let engine = Arc::new(EngineState::initialize());
    engine.set_arc_self(Arc::clone(&engine)).unwrap();
    engine.set_write_timeout_ms(write_timeout_ms);

    // Bind on port 0 → the OS picks a free port; read it back before we move the
    // listener into the accept loop.
    let listener = EngineState::bind_tcp_listener(0, false).expect("bind on ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();

    let srv = Arc::clone(&engine);
    std::thread::spawn(move || {
        srv.run_accept_loop(listener, vec![]);
    });
    (engine, port)
}

/// Connect a chili (v9) subscriber socket to `port`, completing the auth
/// handshake. Shrinks `SO_RCVBUF` so the kernel receive buffer fills fast and a
/// non-reading subscriber blocks the server's write quickly + deterministically.
fn connect_subscriber(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to listener");
    // Shrink the receive buffer so the server-side write fills + blocks fast.
    let sref = socket2::SockRef::from(&stream);
    let _ = sref.set_recv_buffer_size(2048);

    let mut stream = stream;
    // v9 = chili IPC; empty user/password (server has no user allow-list).
    let remote_version = send_auth(&mut stream, "", "", 9).expect("auth handshake");
    assert_eq!(remote_version, 9, "server must negotiate chili v9");
    stream
}

/// Poll `list_handle()` until a single chili Incoming handle (the subscriber we
/// just connected) shows up, returning its handle number. Bounded so a missed
/// accept can't hang the test.
fn await_incoming_handle(engine: &Arc<EngineState>) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let df = engine.list_handle().expect("list_handle");
        if df.height() > 0 {
            let nums = df.column("num").unwrap().i64().unwrap();
            let conn = df.column("conn_type").unwrap().str().unwrap();
            for i in 0..df.height() {
                if conn.get(i) == Some("Incoming") {
                    return nums.get(i).unwrap();
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server never registered the incoming subscriber handle"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Return the rendered `conn_type` of handle `h`, or None if it's gone.
fn conn_type_of(engine: &Arc<EngineState>, h: i64) -> Option<String> {
    let df = engine.list_handle().ok()?;
    let nums = df.column("num").ok()?.i64().ok()?;
    let conn = df.column("conn_type").ok()?.str().ok()?;
    for i in 0..df.height() {
        if nums.get(i) == Some(h) {
            return conn.get(i).map(|s| s.to_owned());
        }
    }
    None
}

/// A large publish frame whose serialized form dwarfs the shrunken socket
/// buffers, so a non-reading subscriber blocks the server write in a frame or
/// two.
fn big_message() -> (SpicyObj, SpicyObj, SpicyObj) {
    let payload = "x".repeat(512 * 1024); // 512 KiB
    (
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol("trade".into()),
        SpicyObj::String(payload),
    )
}

#[test]
fn slow_subscriber_is_shed_on_write_timeout() {
    // 200 ms write timeout — small enough to keep the test fast, large enough
    // that a healthy localhost write never trips it spuriously.
    let (engine, port) = start_server(200);

    // Connect a subscriber that NEVER reads from its socket.
    let mut peer = connect_subscriber(port);

    // The server accepts + registers it as an Incoming handle.
    let h = await_incoming_handle(&engine);

    // Promote it to a Publishing subscriber on the "trade" topic, exactly as the
    // `.sub`/broker path would, then publish large frames to it.
    engine.handle_subscriber(&h).expect("promote to Publishing");
    engine
        .add_subscriber("trade", h)
        .expect("register subscriber on topic");

    let (upd, table, _msg) = big_message();

    // Publish in a bounded loop until the handle is shed (Disconnected) or a
    // wall-clock guard fires. Each publish that times out marks the handle
    // Disconnected; once that happens further publishes are skipped for it.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut shed = false;
    for _ in 0..200 {
        // Rebuild the payload each iteration (publish takes &SpicyObj).
        let payload = SpicyObj::String("x".repeat(512 * 1024));
        // publish itself never errors on a per-handle write failure — it marks
        // the handle Disconnected internally — so we drive it and then inspect.
        let _ = engine.publish(&upd, &table, "trade", &payload);

        if conn_type_of(&engine, h).as_deref() == Some("Disconnected") {
            shed = true;
            break;
        }
        assert!(
            Instant::now() < deadline,
            "write-timeout shed never fired within the guard window"
        );
    }

    assert!(
        shed,
        "subscriber that stopped reading must be shed (handle marked Disconnected) on write timeout"
    );

    // The shed must also TEAR DOWN the socket (shutdown(Both)): a subsequent read
    // on the peer returns EOF (0 bytes) or an error — never blocks indefinitely.
    // A short read timeout guards against a half-open socket hanging the test.
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = [0u8; 64];
    loop {
        match peer.read(&mut buf) {
            Ok(0) => break,    // clean EOF — socket shut
            Ok(_) => continue, // drain buffered bytes, keep reading to EOF
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                panic!("peer socket was not shut down — read blocked past the shed")
            }
            Err(_) => break, // reset/other error — socket is torn down, acceptable
        }
    }
}

/// Control: with the write-timeout OFF (the default, 0), an Incoming handle
/// gets NO shutdown dup, so the shed path is inert. (We don't try to wedge it —
/// that would hang by design without the timeout; this just asserts the opt-in
/// is genuinely opt-in: a fresh handle is Incoming, not pre-shed.)
#[test]
fn write_timeout_off_by_default_leaves_handle_live() {
    let (engine, port) = start_server(0);
    let _peer = connect_subscriber(port);
    let h = await_incoming_handle(&engine);
    assert_eq!(
        conn_type_of(&engine, h).as_deref(),
        Some("Incoming"),
        "with the timeout off, the accepted handle stays a live Incoming handle"
    );
}
