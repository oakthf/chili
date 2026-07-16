//! Gzip sequence validation — `.broker.validateSeq` and `init_tick` recovery path.

use std::io::Write;

use chili_core::{EngineState, SpicyObj, Stack};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn write_valid_seq(dir: &tempfile::TempDir, name: &str, count: usize) -> String {
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

fn gzip_file(plain_path: &str, gz_name: &str) -> String {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let plain = std::path::Path::new(plain_path);
    let gz_path = plain.with_file_name(gz_name);
    let plain_bytes = std::fs::read(plain_path).unwrap();
    let out = std::fs::File::create(&gz_path).unwrap();
    let mut enc = GzEncoder::new(out, Compression::default());
    enc.write_all(&plain_bytes).unwrap();
    enc.finish().unwrap();
    gz_path.to_str().unwrap().to_owned()
}

fn new_engine() -> EngineState {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    state
}

fn validate_seq(state: &EngineState, path: &str) -> i64 {
    let mut stack = Stack::new(None, 0, 0, "");
    let call = SpicyObj::MixedList(vec![
        SpicyObj::Symbol(".broker.validateSeq".into()),
        SpicyObj::String(path.to_string()),
        SpicyObj::Boolean(false),
    ]);
    state
        .eval(&mut stack, &call, "t.pep")
        .unwrap()
        .to_i64()
        .unwrap()
}

#[test]
fn validate_seq_counts_gzip_tplog() {
    let dir = tempfile::tempdir().unwrap();
    let plain = write_valid_seq(&dir, "plain.seq", 7);
    let gz_path = gzip_file(&plain, "plain.seq.gz");

    let engine = new_engine();
    assert_eq!(validate_seq(&engine, &gz_path), 7);
}

#[test]
fn open_handle_on_gzip_seq_returns_message_count() {
    let dir = tempfile::tempdir().unwrap();
    let plain = write_valid_seq(&dir, "plain.seq", 4);
    let gz_path = gzip_file(&plain, "archive.seq.gz");

    let engine = new_engine();
    assert_eq!(validate_seq(&engine, &gz_path), 4);

    let h = engine
        .open_handle(&format!("file://{gz_path}"), 0)
        .unwrap()
        .to_i64()
        .unwrap();
    assert_eq!(engine.get_tick_count(h as usize).unwrap(), 4);
}
