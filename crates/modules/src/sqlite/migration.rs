use super::*;
use serde::{Serialize, de::DeserializeOwned};

/// Run under the schema transaction and exclusive owner lock. Process one
/// bounded record at a time, including hash/FK replacement for frozen packages.
pub(super) fn checkpoint_three(tx: &Transaction<'_>, cap: usize) -> Result<(), ForgeError> {
    let maximum: i64 = tx
        .query_row("SELECT coalesce(max(rowid),0) FROM wf_packages", [], |r| {
            r.get(0)
        })
        .map_err(db_error)?;
    let mut rowid = 0_i64;
    loop {
        let row:Option<(i64,String,Vec<u8>)>=tx.query_row("SELECT rowid,id,body FROM wf_packages WHERE rowid>?1 AND rowid<=?2 ORDER BY rowid LIMIT 1",params![rowid,maximum],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(db_error)?;
        let Some((next, id, body)) = row else {
            break;
        };
        rowid = next;
        if codec::hash(&body) != id {
            return Err(corrupt());
        }
        match codec::version(&body, cap)? {
            CHECKPOINT_FORMAT => continue,
            2 => (),
            _ => return Err(unsupported()),
        }
        let package: ResolvedPackage = codec::decode_version(&body, cap, 2)?;
        let upgraded = codec::encode(&package, cap)?;
        let new_id = codec::hash(&upgraded);
        tx.execute(
            "INSERT INTO wf_packages(id,body) VALUES(?1,?2) ON CONFLICT(id) DO NOTHING",
            params![new_id, upgraded],
        )
        .map_err(db_error)?;
        let stored: Vec<u8> = tx
            .query_row("SELECT body FROM wf_packages WHERE id=?1", [&new_id], |r| {
                r.get(0)
            })
            .map_err(db_error)?;
        if stored != upgraded {
            return Err(corrupt());
        }
        tx.execute(
            "UPDATE wf_runs SET package_id=?1 WHERE package_id=?2",
            params![new_id, id],
        )
        .map_err(db_error)?;
        tx.execute("DELETE FROM wf_packages WHERE id=?1", [id])
            .map_err(db_error)?;
    }
    upgrade_rows::<RunSnapshot>(tx, "wf_runs", cap, |run| {
        if run.checkpoint_format != 2 || !run.waits.is_empty() {
            return Err(unsupported());
        }
        run.checkpoint_format = CHECKPOINT_FORMAT;
        Ok(())
    })?;
    upgrade_rows::<InvocationRecord>(tx, "wf_invocations", cap, |_| Ok(()))?;
    upgrade_rows::<serde_json::Value>(tx, "wf_receipts", cap, |_| Ok(()))?;
    tx.execute(
        "UPDATE wf_runs SET next_wakeup_at_ms=deadline_at_ms WHERE state='waiting'",
        [],
    )
    .map_err(db_error)?;
    Ok(())
}

fn upgrade_rows<T: Serialize + DeserializeOwned>(
    tx: &Transaction<'_>,
    table: &str,
    cap: usize,
    edit: impl Fn(&mut T) -> Result<(), ForgeError>,
) -> Result<(), ForgeError> {
    // Table/column names are private migration constants, never caller input.
    let column = if table == "wf_receipts" {
        "request"
    } else {
        "body"
    };
    let query = format!("SELECT rowid,{column} FROM {table} WHERE rowid>?1 ORDER BY rowid LIMIT 1");
    let update = format!("UPDATE {table} SET {column}=?1 WHERE rowid=?2");
    let mut rowid = 0_i64;
    loop {
        let row: Option<(i64, Vec<u8>)> = tx
            .query_row(&query, [rowid], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .map_err(db_error)?;
        let Some((next, body)) = row else {
            break;
        };
        rowid = next;
        match codec::version(&body, cap)? {
            CHECKPOINT_FORMAT => continue,
            2 => (),
            _ => return Err(unsupported()),
        }
        let mut value: T = codec::decode_version(&body, cap, 2)?;
        edit(&mut value)?;
        tx.execute(&update, params![codec::encode(&value, cap)?, rowid])
            .map_err(db_error)?;
    }
    Ok(())
}
