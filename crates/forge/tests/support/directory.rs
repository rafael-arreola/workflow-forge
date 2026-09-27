use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

/// Reserve atomically: parallel tests can share a clock tick, even in nanoseconds.
/// Skip leftovers from an earlier process that reused the PID.
pub fn create(prefix: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{sequence}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("Cannot create isolated test directory: {error}"),
        }
    }
}
