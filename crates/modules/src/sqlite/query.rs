use super::*;

pub(super) fn head(tx: &Transaction<'_>, id: &RunId) -> Result<Option<RunHead>, ForgeError> {
    let value = tx.query_row(
        "SELECT scope,revision,state,cancel_requested,deadline_at_ms,invocation_count,retained_bytes,unresolved_count,next_wakeup_at_ms FROM wf_runs WHERE id=?1",
        [&id.0], |r| Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?,r.get::<_,bool>(3)?,r.get::<_,u64>(4)?,r.get::<_,usize>(5)?,r.get::<_,usize>(6)?,r.get::<_,usize>(7)?,r.get::<_,Option<u64>>(8)?)),
    ).optional().map_err(db_error)?;
    value
        .map(
            |(
                scope,
                revision,
                state,
                cancel_requested,
                deadline_at_ms,
                invocation_count,
                retained_data_bytes,
                unresolved_invocations,
                next_wakeup_at_ms,
            )| {
                Ok(RunHead {
                    next_wakeup_at_ms,
                    id: id.clone(),
                    scope,
                    revision,
                    state: codec::parse_state(&state)?,
                    cancel_requested,
                    deadline_at_ms,
                    invocation_count,
                    retained_data_bytes,
                    unresolved_invocations,
                })
            },
        )
        .transpose()
}

pub(super) fn node(
    tx: &Transaction<'_>,
    id: &RunId,
    path: &str,
    limit: usize,
) -> Result<Option<InvocationRecord>, ForgeError> {
    let value = tx
        .query_row(
            "SELECT body,retained_bytes,unresolved FROM wf_invocations WHERE run_id=?1 AND path=?2",
            params![id.0, path],
            |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, usize>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            },
        )
        .optional()
        .map_err(db_error)?;
    value
        .map(|(body, bytes, unresolved)| {
            let record: InvocationRecord = codec::decode(&body, limit)?;
            if record.retained_data_bytes() != bytes || record.is_unresolved() != unresolved {
                return Err(corrupt());
            }
            Ok(record)
        })
        .transpose()
}

pub(super) fn view(
    tx: &Transaction<'_>,
    id: &RunId,
    path: Option<&str>,
    limit: usize,
) -> Result<Option<ExecutionView>, ForgeError> {
    let Some(head) = head(tx, id)? else {
        return Ok(None);
    };
    let invocation = match path {
        Some(path) => node(tx, id, path, limit)?,
        None => None,
    };
    let unresolved_descendants = match path {
        // Paths use binary collation. Every descendant starts with the slash,
        // whose immediate ASCII successor is '0'; wildcard IDs stay literal.
        Some(path) => tx.query_row("SELECT EXISTS(SELECT 1 FROM wf_invocations WHERE run_id=?1 AND unresolved=1 AND path>=?2 AND path<?3)",params![id.0,format!("{path}/"),format!("{path}0")],|r|r.get(0)).map_err(db_error)?,
        None => head.unresolved_invocations != 0,
    };
    Ok(Some(ExecutionView {
        head,
        invocation,
        unresolved_descendants,
    }))
}

pub(super) fn run(
    tx: &Transaction<'_>,
    id: &RunId,
    limit: usize,
) -> Result<Option<RunSnapshot>, ForgeError> {
    let Some(head) = head(tx, id)? else {
        return Ok(None);
    };
    let (body, package_id, finished): (Vec<u8>, String, Option<u64>) = tx
        .query_row(
            "SELECT body,package_id,finished_at_ms FROM wf_runs WHERE id=?1",
            [&id.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(db_error)?;
    let mut run: RunSnapshot = codec::decode(&body, limit)?;
    if run.checkpoint_format != CHECKPOINT_FORMAT {
        return Err(unsupported());
    }
    if run.id != *id
        || run.scope != head.scope
        || run.state != head.state
        || run.cancel_requested != head.cancel_requested
        || run.deadline_at_ms != head.deadline_at_ms
        || run.finished_at_ms != finished
        || run.revision > head.revision
        || !run.invocations.is_empty()
        || run.package != ResolvedPackage::default()
    {
        return Err(corrupt());
    }
    let package: Vec<u8> = tx
        .query_row(
            "SELECT body FROM wf_packages WHERE id=?1",
            [&package_id],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    if codec::hash(&package) != package_id {
        return Err(corrupt());
    }
    run.package = codec::decode(&package, limit)?;
    run.revision = head.revision;
    let mut statement = tx.prepare("SELECT path,body,retained_bytes,unresolved FROM wf_invocations WHERE run_id=?1 ORDER BY path").map_err(db_error)?;
    let mut rows = statement.query([&id.0]).map_err(db_error)?;
    while let Some(row) = rows.next().map_err(db_error)? {
        let path: String = row.get(0).map_err(db_error)?;
        let bytes: Vec<u8> = row.get(1).map_err(db_error)?;
        let retained: usize = row.get(2).map_err(db_error)?;
        let unresolved: bool = row.get(3).map_err(db_error)?;
        let record: InvocationRecord = codec::decode(&bytes, limit)?;
        if record.retained_data_bytes() != retained || record.is_unresolved() != unresolved {
            return Err(corrupt());
        }
        run.invocations.insert(path, record);
    }
    if run.head() != head {
        return Err(corrupt());
    }
    Ok(Some(run))
}

pub(super) fn unfinished(tx: &Transaction<'_>) -> Result<Vec<RunId>, ForgeError> {
    let mut statement = tx.prepare("SELECT id FROM wf_runs WHERE state NOT IN ('succeeded','failed','cancelled') ORDER BY id").map_err(db_error)?;
    statement
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(db_error)?
        .map(|r| r.map(RunId).map_err(db_error))
        .collect()
}

pub(super) fn header_body(run: &RunSnapshot, limit: usize) -> Result<Vec<u8>, ForgeError> {
    let mut header = run.clone();
    header.invocations.clear();
    header.package = ResolvedPackage::default();
    codec::encode(&header, limit)
}

pub(super) fn write_node(
    tx: &Transaction<'_>,
    id: &RunId,
    path: &str,
    record: &InvocationRecord,
    limit: usize,
) -> Result<(), ForgeError> {
    tx.execute("INSERT INTO wf_invocations(run_id,path,retained_bytes,unresolved,body) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(run_id,path) DO UPDATE SET retained_bytes=excluded.retained_bytes,unresolved=excluded.unresolved,body=excluded.body",params![id.0,path,record.retained_data_bytes(),record.is_unresolved(),codec::encode(record,limit)?]).map_err(db_error)?;
    Ok(())
}
