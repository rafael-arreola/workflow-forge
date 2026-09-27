use super::*;
pub(super) const APPLICATION_ID: i32 = 0x57464f52;
const SCHEMA_VERSION: u32 = 3;

pub(super) fn initialize(conn: &mut Connection, options: &SqliteOptions) -> Result<(), ForgeError> {
    let app: i32 = conn
        .pragma_query_value(None, "application_id", |r| r.get(0))
        .map_err(db_error)?;
    let version: u32 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .map_err(db_error)?;
    if (app != 0 && app != APPLICATION_ID) || version > SCHEMA_VERSION || (app == 0 && version != 0)
    {
        return Err(unsupported());
    }
    if app == 0 {
        let count: usize = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .map_err(db_error)?;
        if count != 0 {
            return Err(unsupported());
        }
    } else if version == 0 {
        return Err(unsupported());
    }

    conn.busy_timeout(std::time::Duration::from_millis(options.busy_timeout_ms))
        .map_err(db_error)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(db_error)?;
    conn.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA wal_autocheckpoint=1000;").map_err(db_error)?;
    let wal: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .map_err(db_error)?;
    let sync: u32 = conn
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .map_err(db_error)?;
    let foreign: bool = conn
        .pragma_query_value(None, "foreign_keys", |r| r.get(0))
        .map_err(db_error)?;
    if wal != "wal" || sync != 2 || !foreign {
        return Err(unsupported());
    }
    let page_size: u64 = conn
        .pragma_query_value(None, "page_size", |r| r.get(0))
        .map_err(db_error)?;
    let pages = options.max_database_bytes / page_size;
    conn.pragma_update(None, "max_page_count", pages)
        .map_err(db_error)?;
    let actual: u64 = conn
        .pragma_query_value(None, "max_page_count", |r| r.get(0))
        .map_err(db_error)?;
    if actual > pages {
        return Err(ForgeError::new(
            "resource.limit",
            "Existing database exceeds its page budget",
        ));
    }
    conn.set_limit(
        rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
        options
            .max_record_bytes
            .saturating_add(1024)
            .min(i32::MAX as usize) as i32,
    )
    .map_err(db_error)?;

    if version < SCHEMA_VERSION {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        if version == 0 {
            tx.execute_batch(include_str!("v1.sql")).map_err(db_error)?;
        }
        // Format-2 JSON-only checkpoints retain their representation and
        // counters. New attachment declarations default to an empty collection.
        if version < 2 {
            tx.execute_batch(include_str!("v2.sql")).map_err(db_error)?;
        }
        tx.execute_batch(include_str!("v3.sql")).map_err(db_error)?;
        super::migration::checkpoint_three(&tx, options.max_record_bytes)?;
        tx.pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(db_error)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
    }
    let check: String = conn
        .query_row("PRAGMA quick_check(1)", [], |r| r.get(0))
        .map_err(db_error)?;
    if check != "ok" {
        return Err(corrupt());
    }
    Ok(())
}
