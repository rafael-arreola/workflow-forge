//! Read-only allocator diagnostics for the capacity benchmark, not an engine port.
use serde_json::Value;

/// macOS exposes live allocation and reserved-memory counters separately from RSS.
/// Other platforms report null; a missing heap probe must not be interpreted as zero.
#[cfg(target_os = "macos")]
pub fn statistics() -> Value {
    let mut stats = libc::malloc_statistics_t {
        blocks_in_use: 0,
        size_in_use: 0,
        max_size_in_use: 0,
        size_allocated: 0,
    };
    // SAFETY: Apple's public malloc/malloc.h specifies NULL to sum all zones.
    // libc supplies the platform ABI, and stats is a valid, initialized output
    // buffer for this synchronous call; neither pointer escapes this function.
    // This reads counters and never requests pressure relief or changes allocators.
    unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), &mut stats) };
    serde_json::json!({
        "blocks_in_use":stats.blocks_in_use,
        "size_in_use":stats.size_in_use,
        "max_size_in_use":stats.max_size_in_use,
        "size_allocated":stats.size_allocated,
    })
}

#[cfg(not(target_os = "macos"))]
pub fn statistics() -> Value {
    Value::Null
}
