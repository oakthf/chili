//! Bad-message hook + continue-on-corrupt-frame for tplog replay.

use std::io::Write;
use std::sync::Arc;

use chili_core::{EngineState, SpicyObj, Stack};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> Arc<EngineState> {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    Arc::new(state)
}

fn write_valid_tplog(dir: &tempfile::TempDir, name: &str, count: usize) -> String {
    let path = dir.path().join(name);
    let paths = path.to_str().unwrap().to_owned();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.write_all(&[255, 0, 0, 0, 0, 0, 0, 0]).unwrap();
    for i in 0..count {
        let msg = SpicyObj::MixedList(vec![
            SpicyObj::Symbol("upd".into()),
            SpicyObj::Symbol("trade".into()),
            SpicyObj::I64(i as i64),
        ]);
        let payload = chili_core::serde9::serialize(&msg, false).unwrap();
        let payload_bytes: Vec<u8> = payload.iter().flat_map(|v| v.iter().copied()).collect();
        let size = payload_bytes.len() as u64;
        let utc_time = 1000u64 + i as u64;
        file.write_all(&size.to_le_bytes()).unwrap();
        file.write_all(&utc_time.to_le_bytes()).unwrap();
        file.write_all(&payload_bytes).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    paths
}

fn append_garbage_frame(path: &str) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    let garbage = vec![0xFEu8; 3];
    let size = garbage.len() as u64;
    file.write_all(&size.to_le_bytes()).unwrap();
    file.write_all(&9999u64.to_le_bytes()).unwrap();
    file.write_all(&garbage).unwrap();
    file.flush().unwrap();
}

fn append_valid_frame(path: &str, i: i64) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    let msg = SpicyObj::MixedList(vec![
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol("trade".into()),
        SpicyObj::I64(i),
    ]);
    let payload = chili_core::serde9::serialize(&msg, false).unwrap();
    let payload_bytes: Vec<u8> = payload.iter().flat_map(|v| v.iter().copied()).collect();
    file.write_all(&(payload_bytes.len() as u64).to_le_bytes())
        .unwrap();
    file.write_all(&(1000u64 + i as u64).to_le_bytes()).unwrap();
    file.write_all(&payload_bytes).unwrap();
    file.flush().unwrap();
}

#[test]
fn replay_continues_past_corrupt_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_valid_tplog(&dir, "mid_corrupt.seq", 2);
    append_garbage_frame(&path);
    append_valid_frame(&path, 99);

    let engine = new_engine();
    let result = engine
        .replay_chili_msgs_log(&path, 0, 100, 0, &vec![], false, 0)
        .unwrap();
    match result {
        SpicyObj::MixedList(list) => {
            assert_eq!(list.len(), 3, "must skip corrupt frame and keep replaying");
            let last = list[2].as_vec().unwrap();
            assert_eq!(last[2].to_i64().unwrap(), 99);
        }
        other => panic!("expected MixedList, got {other:?}"),
    }
}

#[test]
fn bad_msg_hook_fires_on_corrupt_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_valid_tplog(&dir, "hook.seq", 1);
    append_garbage_frame(&path);
    append_valid_frame(&path, 7);

    let engine = new_engine();
    let mut s = Stack::new(None, 0, 0, "");
    engine
        .eval(
            &mut s,
            &SpicyObj::String(
                ".bad.msg: {[i; e; b]
                    `bad_idx set i;
                    `bad_err set e;
                    `bad_n set count b; };"
                    .into(),
            ),
            "t.pep",
        )
        .unwrap();
    engine.set_on_bad_msg_hook(Some(".bad.msg".into()));

    let result = engine
        .replay_chili_msgs_log(&path, 0, 100, 0, &vec![], false, 0)
        .unwrap();
    match result {
        SpicyObj::MixedList(list) => assert_eq!(list.len(), 2),
        other => panic!("expected MixedList, got {other:?}"),
    }
    assert_eq!(engine.get_var("bad_idx").unwrap().to_i64().unwrap(), 1);
    assert!(engine.get_var("bad_err").unwrap().str().is_ok());
    assert_eq!(engine.get_var("bad_n").unwrap().to_i64().unwrap(), 3);
}
