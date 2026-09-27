use super::*;

impl ExecutionStore for SqliteExecutionStore {
    fn artifact_domain(&self) -> Option<&str> {
        Some(&self.inner.domain)
    }
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            durable: true,
            checkpoint_format: CHECKPOINT_FORMAT,
        }
    }
    fn claim<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        let owner = owner.to_owned();
        Box::pin(async move {
            let (send, receive) = oneshot::channel();
            self.sender()?
                .send(Box::new(move |worker| {
                    let result = worker.claim(&owner);
                    let claimed = result.is_ok();
                    if send.send(result).is_err() && claimed {
                        let _ = worker.release(&owner);
                    }
                }))
                .await
                .map_err(|_| failed())?;
            receive.await.map_err(|_| failed())?
        })
    }
    fn release<'a>(&'a self, owner: &'a str) -> PortFuture<'a, ()> {
        let owner = owner.to_owned();
        Box::pin(async move { self.call(move |s| s.release(&owner)).await })
    }
    fn create<'a>(
        &'a self,
        owner: &'a str,
        run: RunSnapshot,
        receipt: Option<ReceiptReservation>,
        limits: &'a Limits,
    ) -> PortFuture<'a, CreateOutcome> {
        let owner = owner.to_owned();
        let limits = limits.clone();
        Box::pin(async move {
            self.call(move |s| {
                let cap = s.options.max_record_bytes;
                s.write(&owner, |tx| create(tx, run, receipt, &limits, cap))
            })
            .await
        })
    }
    fn get<'a>(&'a self, id: &'a RunId) -> PortFuture<'a, Option<RunSnapshot>> {
        let id = id.clone();
        Box::pin(async move {
            self.call(move |s| {
                let cap = s.options.max_record_bytes;
                s.read(|tx| query::run(tx, &id, cap))
            })
            .await
        })
    }
    fn view<'a>(
        &'a self,
        id: &'a RunId,
        node: Option<&'a str>,
    ) -> PortFuture<'a, Option<ExecutionView>> {
        let id = id.clone();
        let node = node.map(str::to_owned);
        Box::pin(async move {
            self.call(move |s| {
                let cap = s.options.max_record_bytes;
                s.read(|tx| query::view(tx, &id, node.as_deref(), cap))
            })
            .await
        })
    }
    fn unfinished(&self) -> PortFuture<'_, Vec<RunSnapshot>> {
        Box::pin(async move {
            self.call(|s| {
                let cap = s.options.max_record_bytes;
                s.read(|tx| {
                    query::unfinished(tx)?
                        .iter()
                        .map(|id| query::run(tx, id, cap)?.ok_or_else(corrupt))
                        .collect()
                })
            })
            .await
        })
    }
    fn unfinished_heads(&self) -> PortFuture<'_, Vec<RunHead>> {
        Box::pin(async move {
            self.call(|s| {
                s.read(|tx| {
                    query::unfinished(tx)?
                        .iter()
                        .map(|id| query::head(tx, id)?.ok_or_else(corrupt))
                        .collect()
                })
            })
            .await
        })
    }
    fn commit<'a>(
        &'a self,
        owner: &'a str,
        expected: u64,
        next: RunSnapshot,
    ) -> PortFuture<'a, ()> {
        let owner = owner.to_owned();
        Box::pin(async move {
            self.call(move |s| {
                let cap = s.options.max_record_bytes;
                s.write(&owner, |tx| commit(tx, expected, next, cap))
            })
            .await
        })
    }
    fn commit_invocation<'a>(
        &'a self,
        owner: &'a str,
        id: &'a RunId,
        expected: u64,
        node: &'a str,
        record: InvocationRecord,
    ) -> PortFuture<'a, ExecutionView> {
        let owner = owner.to_owned();
        let id = id.clone();
        let node = node.to_owned();
        Box::pin(async move {
            self.call(move |s| {
                let cap = s.options.max_record_bytes;
                s.write(&owner, |tx| {
                    commit_node(tx, &id, expected, &node, record, cap)
                })
            })
            .await
        })
    }
    fn collect<'a>(&'a self, owner: &'a str, now: u64, limits: &'a Limits) -> PortFuture<'a, ()> {
        let owner = owner.to_owned();
        let limits = limits.clone();
        Box::pin(async move {
            self.call(move |s| s.write(&owner, |tx| collect(tx, now, &limits)))
                .await
        })
    }
}

fn create(
    tx: &Transaction<'_>,
    run: RunSnapshot,
    receipt: Option<ReceiptReservation>,
    limits: &Limits,
    cap: usize,
) -> Result<CreateOutcome, ForgeError> {
    tx.execute(
        "DELETE FROM wf_receipts WHERE expires_at_ms<=?1",
        [run.created_at_ms],
    )
    .map_err(db_error)?;
    if let Some(reservation) = &receipt {
        let previous: Option<(String,u64,Vec<u8>)> = tx.query_row("SELECT run_id,expires_at_ms,request FROM wf_receipts WHERE scope=?1 AND receipt_key=?2",params![run.scope,reservation.key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(db_error)?;
        if let Some((id, expiry, request)) = previous {
            let value: serde_json::Value = codec::decode(&request, cap)?;
            if value != reservation.request {
                return Err(conflict());
            }
            return Ok(CreateOutcome::Duplicate {
                run_id: RunId(id),
                expires_at_ms: expiry,
            });
        }
        let count: usize = tx
            .query_row("SELECT count(*) FROM wf_receipts", [], |r| r.get(0))
            .map_err(db_error)?;
        if count >= limits.receipt_count {
            return Err(ForgeError::new(
                "admission.full",
                "Receipt retention is full",
            ));
        }
    }
    let count: usize = tx
        .query_row(
            "SELECT count(*) FROM wf_runs WHERE state NOT IN ('succeeded','failed','cancelled')",
            [],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    if count >= limits.active_runs.saturating_add(limits.pending_runs) {
        return Err(ForgeError::new("admission.full", "Run capacity is full"));
    }
    run.validate_initial()?;
    if query::head(tx, &run.id)?.is_some() {
        return Err(conflict());
    }
    let package = codec::encode(&run.package, cap)?;
    let package_id = codec::hash(&package);
    tx.execute(
        "INSERT INTO wf_packages(id,body) VALUES(?1,?2) ON CONFLICT(id) DO NOTHING",
        params![package_id, package],
    )
    .map_err(db_error)?;
    let stored: Vec<u8> = tx
        .query_row(
            "SELECT body FROM wf_packages WHERE id=?1",
            [&package_id],
            |r| r.get(0),
        )
        .map_err(db_error)?;
    if stored != package {
        return Err(corrupt());
    }
    let head = run.head();
    tx.execute("INSERT INTO wf_runs(id,scope,revision,state,cancel_requested,deadline_at_ms,finished_at_ms,retained_bytes,invocation_count,unresolved_count,package_id,body,next_wakeup_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![run.id.0,run.scope,run.revision,codec::state(run.state),run.cancel_requested,run.deadline_at_ms,run.finished_at_ms,head.retained_data_bytes,head.invocation_count,head.unresolved_invocations,package_id,query::header_body(&run,cap)?,head.next_wakeup_at_ms]).map_err(db_error)?;
    artifacts::pin_inputs(tx, &run)?;
    if let Some(reservation) = receipt {
        tx.execute("INSERT INTO wf_receipts(scope,receipt_key,run_id,expires_at_ms,request) VALUES(?1,?2,?3,?4,?5)",params![run.scope,reservation.key,run.id.0,reservation.expires_at_ms,codec::encode(&reservation.request,cap)?]).map_err(db_error)?;
    }
    Ok(CreateOutcome::Created)
}

fn commit(
    tx: &Transaction<'_>,
    expected: u64,
    next: RunSnapshot,
    cap: usize,
) -> Result<(), ForgeError> {
    let current = query::run(tx, &next.id, cap)?
        .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
    current.validate_successor(expected, &next)?;
    for (id, wait) in &next.waits {
        if let Some(delivery) = &wait.delivery {
            if current
                .waits
                .get(id)
                .is_none_or(|old| old.delivery.is_none())
            {
                artifacts::pin_references(tx, &next.id, &next.scope, &delivery.command.artifacts)?;
            }
        }
    }
    let head = next.head();
    let changed=tx.execute("UPDATE wf_runs SET revision=?1,state=?2,cancel_requested=?3,finished_at_ms=?4,retained_bytes=?5,invocation_count=?6,unresolved_count=?7,body=?8,next_wakeup_at_ms=?11 WHERE id=?9 AND revision=?10",params![next.revision,codec::state(next.state),next.cancel_requested,next.finished_at_ms,head.retained_data_bytes,head.invocation_count,head.unresolved_invocations,query::header_body(&next,cap)?,next.id.0,expected,head.next_wakeup_at_ms]).map_err(db_error)?;
    if changed != 1 {
        return Err(conflict());
    }
    for (path, record) in &next.invocations {
        if current.invocations.get(path) != Some(record) {
            query::write_node(tx, &next.id, path, record, cap)?;
        }
    }
    for path in current
        .invocations
        .keys()
        .filter(|p| !next.invocations.contains_key(*p))
    {
        tx.execute(
            "DELETE FROM wf_invocations WHERE run_id=?1 AND path=?2",
            params![next.id.0, path],
        )
        .map_err(db_error)?;
    }
    Ok(())
}

fn commit_node(
    tx: &Transaction<'_>,
    id: &RunId,
    expected: u64,
    path: &str,
    record: InvocationRecord,
    cap: usize,
) -> Result<ExecutionView, ForgeError> {
    let head =
        query::head(tx, id)?.ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
    let old = query::node(tx, id, path, cap)?;
    if head.revision != expected
        || record.is_wait()
        || old.as_ref().is_some_and(InvocationRecord::is_wait)
        || head.state.is_terminal()
        || old
            .as_ref()
            .is_some_and(|r| r.state == InvocationState::Succeeded && r != &record)
    {
        return Err(conflict());
    }
    let revision = expected.checked_add(1).ok_or_else(conflict)?;
    let bytes = head
        .retained_data_bytes
        .saturating_sub(
            old.as_ref()
                .map_or(0, InvocationRecord::retained_data_bytes),
        )
        .saturating_add(record.retained_data_bytes());
    let count = head.invocation_count + usize::from(old.is_none());
    let unresolved = head.unresolved_invocations.saturating_sub(usize::from(
        old.as_ref().is_some_and(InvocationRecord::is_unresolved),
    )) + usize::from(record.is_unresolved());
    let changed=tx.execute("UPDATE wf_runs SET revision=?1,retained_bytes=?2,invocation_count=?3,unresolved_count=?4 WHERE id=?5 AND revision=?6",params![revision,bytes,count,unresolved,id.0,expected]).map_err(db_error)?;
    if changed != 1 {
        return Err(conflict());
    }
    query::write_node(tx, id, path, &record, cap)?;
    query::view(tx, id, Some(path), cap)?.ok_or_else(corrupt)
}

fn collect(tx: &Transaction<'_>, now: u64, limits: &Limits) -> Result<(), ForgeError> {
    tx.execute("DELETE FROM wf_receipts WHERE expires_at_ms<=?1", [now])
        .map_err(db_error)?;
    let mut statement=tx.prepare("SELECT finished_at_ms,id FROM wf_runs WHERE state IN ('succeeded','failed','cancelled') AND finished_at_ms IS NOT NULL ORDER BY finished_at_ms,id").map_err(db_error)?;
    let finished: Vec<(u64, String)> = statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db_error)?
        .collect::<Result<_, _>>()
        .map_err(db_error)?;
    let excess = finished.len().saturating_sub(limits.terminal_runs);
    for (index, (at, id)) in finished.into_iter().enumerate() {
        if index < excess || now.saturating_sub(at) >= limits.retention_ms {
            tx.execute("DELETE FROM wf_runs WHERE id=?1", [id])
                .map_err(db_error)?;
        }
    }
    tx.execute("DELETE FROM wf_packages WHERE NOT EXISTS(SELECT 1 FROM wf_runs WHERE package_id=wf_packages.id)",[]).map_err(db_error)?;
    artifacts::collect(tx, now)?;
    Ok(())
}
