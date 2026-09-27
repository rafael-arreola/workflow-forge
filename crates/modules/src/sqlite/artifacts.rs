use super::*;
use futures::{StreamExt, stream};

const CHUNK_BYTES: usize = 64 * 1024;

fn unavailable() -> ForgeError {
    ForgeError::new(
        "not_found",
        "Artifact is unavailable or its metadata does not match",
    )
}
fn denied() -> ForgeError {
    ForgeError::new("access.denied", "Artifact is not owned by this run")
}
fn exhausted() -> ForgeError {
    ForgeError::new("resource.limit", "Artifact storage budget is exhausted")
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as u64
}

impl ArtifactStore for SqliteExecutionStore {
    fn durable(&self) -> bool {
        true
    }
    fn host_access(&self) -> bool {
        true
    }
    fn artifact_domain(&self) -> Option<&str> {
        Some(&self.inner.domain)
    }
    fn write<'a>(
        &'a self,
        scope: &'a str,
        content: ByteStream,
        media_type: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(self.write_artifact(None, None, scope, content, media_type))
    }
    fn read<'a>(&'a self, reference: &'a ArtifactRef) -> PortFuture<'a, ByteStream> {
        Box::pin(self.read_artifact(None, None, reference))
    }
    fn write_for_host<'a>(
        &'a self,
        owner: &'a str,
        scope: &'a str,
        content: ByteStream,
        media_type: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(self.write_artifact(None, Some(owner.to_owned()), scope, content, media_type))
    }
    fn read_for_host<'a>(
        &'a self,
        owner: &'a str,
        reference: &'a ArtifactRef,
    ) -> PortFuture<'a, ByteStream> {
        Box::pin(self.read_artifact(None, Some(owner.to_owned()), reference))
    }
    fn write_for_run<'a>(
        &'a self,
        access: &'a ArtifactAccess,
        scope: &'a str,
        content: ByteStream,
        media_type: &'a str,
    ) -> PortFuture<'a, ArtifactRef> {
        Box::pin(self.write_artifact(Some(access.clone()), None, scope, content, media_type))
    }
    fn read_for_run<'a>(
        &'a self,
        access: &'a ArtifactAccess,
        reference: &'a ArtifactRef,
    ) -> PortFuture<'a, ByteStream> {
        Box::pin(self.read_artifact(Some(access.clone()), None, reference))
    }
}

impl SqliteExecutionStore {
    async fn write_artifact(
        &self,
        access: Option<ArtifactAccess>,
        host_owner: Option<String>,
        scope: &str,
        mut content: ByteStream,
        media_type: &str,
    ) -> Result<ArtifactRef, ForgeError> {
        if scope.is_empty() || scope.len() > 256 || media_type.is_empty() || media_type.len() > 256
        {
            return Err(ForgeError::new(
                "data.invalid",
                "Artifact scope and media type must contain 1 to 256 bytes",
            ));
        }
        let owner = match (host_owner, &access) {
            (Some(owner), _) => owner,
            (_, Some(access)) => access.runtime_owner.clone(),
            _ => self.call(|s| s.current_owner()).await?,
        };
        let mut reference = ArtifactRef {
            id: uuid::Uuid::now_v7().to_string(),
            scope: scope.into(),
            bytes: 0,
            media_type: media_type.into(),
        };
        // Installed before queueing creation: cancellation after a queued commit
        // also removes its unpublished row. A restart handles a lost cleanup.
        let mut staging = Staging {
            store: self.clone(),
            owner: owner.clone(),
            id: Some(reference.id.clone()),
        };
        let result = async {
            let item = reference.clone();
            let writer = owner.clone();
            let run = access.clone();
            self.call(move |s| {
                let options = s.options.clone();
                s.write(&writer, |tx| begin(tx, &item, run.as_ref(), &options))
            })
            .await?;
            let mut sequence = 0_u64;
            while let Some(chunk) = content.next().await {
                let chunk = chunk?;
                if chunk.len() as u64
                    > self
                        .inner
                        .options
                        .max_artifact_bytes
                        .saturating_sub(reference.bytes)
                {
                    return Err(exhausted());
                }
                // Ignore empty source chunks; storage and reads remain bounded.
                for bytes in chunk.chunks(CHUNK_BYTES.min(self.inner.options.max_record_bytes / 2))
                {
                    let data = bytes.to_vec();
                    let item = reference.clone();
                    let writer = owner.clone();
                    let run = access.clone();
                    self.call(move |s| {
                        let cap = s.options.max_artifact_bytes;
                        s.write(&writer, |tx| {
                            append(tx, &item, run.as_ref(), sequence, data, cap)
                        })
                    })
                    .await?;
                    sequence += 1;
                    reference.bytes += bytes.len() as u64;
                }
            }
            let item = reference.clone();
            let writer = owner.clone();
            let expiry = now_ms()
                .saturating_add(self.inner.options.artifact_grace_ms)
                .min(i64::MAX as u64);
            self.call(move |s| {
                s.write(&writer, |tx| {
                    if let Some(access) = &access {
                        authorize_run(tx, access, &item.scope, true)?;
                    }
                    let changed = tx
                        .execute(
                            "UPDATE wf_artifacts SET state='ready',orphan_after_ms=?4 \
                     WHERE id=?1 AND state='staging' AND bytes=?2 AND chunks=?3",
                            params![item.id, item.bytes, sequence, expiry],
                        )
                        .map_err(db_error)?;
                    if changed != 1 {
                        return Err(conflict());
                    }
                    Ok(())
                })
            })
            .await?;
            Ok::<_, ForgeError>(())
        }
        .await;
        if let Err(mut error) = result {
            if let Err(cleanup) = staging.cleanup().await {
                error.diagnostics.extend(cleanup.diagnostics);
            }
            return Err(error);
        }
        staging.id = None;
        Ok(reference)
    }

    async fn read_artifact(
        &self,
        access: Option<ArtifactAccess>,
        host_owner: Option<String>,
        reference: &ArtifactRef,
    ) -> Result<ByteStream, ForgeError> {
        let item = reference.clone();
        let run = access.clone();
        let (owner, chunks) = self
            .call(move |s| {
                let owner = match (host_owner, &run) {
                    (Some(owner), _) => owner,
                    (_, Some(access)) => access.runtime_owner.clone(),
                    _ => s.current_owner()?,
                };
                let chunks = s.read_owned(&owner, |tx| check_read(tx, &item, run.as_ref()))?;
                Ok((owner, chunks))
            })
            .await?;
        let cursor = Cursor {
            store: self.clone(),
            owner,
            access,
            reference: reference.clone(),
            chunks,
            sequence: 0,
            offset: 0,
        };
        Ok(Box::pin(stream::try_unfold(
            cursor,
            |mut cursor| async move {
                if cursor.sequence == cursor.chunks {
                    if cursor.offset != cursor.reference.bytes {
                        return Err(corrupt());
                    }
                    return Ok(None);
                }
                let item = cursor.reference.clone();
                let owner = cursor.owner.clone();
                let access = cursor.access.clone();
                let sequence = cursor.sequence;
                let data: Vec<u8> = cursor.store.call(move |s| s.read_owned(&owner, |tx| {
                check_read(tx, &item, access.as_ref())?;
                tx.query_row("SELECT body FROM wf_artifact_chunks WHERE artifact_id=?1 AND sequence=?2", params![item.id,sequence], |r|r.get(0)).optional().map_err(db_error)?.ok_or_else(corrupt)
            })).await?;
                if data.is_empty()
                    || data.len() > CHUNK_BYTES
                    || data.len() as u64 > cursor.reference.bytes.saturating_sub(cursor.offset)
                {
                    return Err(corrupt());
                }
                cursor.sequence += 1;
                cursor.offset += data.len() as u64;
                Ok(Some((data, cursor)))
            },
        )))
    }
}

struct Cursor {
    store: SqliteExecutionStore,
    owner: String,
    access: Option<ArtifactAccess>,
    reference: ArtifactRef,
    chunks: u64,
    sequence: u64,
    offset: u64,
}
struct Staging {
    store: SqliteExecutionStore,
    owner: String,
    id: Option<String>,
}
impl Staging {
    async fn cleanup(&mut self) -> Result<(), ForgeError> {
        if let Some(id) = self.id.clone() {
            let owner = self.owner.clone();
            self.store
                .call(move |s| {
                    s.write(&owner, |tx| {
                        tx.execute(
                            "DELETE FROM wf_artifacts WHERE id=?1 AND state='staging'",
                            [id],
                        )
                        .map_err(db_error)?;
                        Ok(())
                    })
                })
                .await?;
            self.id = None;
        }
        Ok(())
    }
}
impl Drop for Staging {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                return;
            };
            let store = self.store.clone();
            let owner = self.owner.clone();
            runtime.spawn(async move {
                let _ = store
                    .call(move |s| {
                        s.write(&owner, |tx| {
                            // Never erase a published reference whose acknowledgement
                            // was lost; it may be needed by its retained run.
                            tx.execute(
                                "DELETE FROM wf_artifacts WHERE id=?1 AND state='staging'",
                                [id],
                            )
                            .map_err(db_error)?;
                            Ok(())
                        })
                    })
                    .await;
            });
        }
    }
}

fn authorize_run(
    tx: &Transaction<'_>,
    access: &ArtifactAccess,
    scope: &str,
    writing: bool,
) -> Result<(), ForgeError> {
    let run: Option<(String, String, bool)> = tx
        .query_row(
            "SELECT scope,state,cancel_requested FROM wf_runs WHERE id=?1",
            [&access.run_id.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(db_error)?;
    let Some((actual, state, cancelled)) = run else {
        return Err(denied());
    };
    if scope != actual {
        return Err(denied());
    }
    if writing && (cancelled || matches!(state.as_str(), "succeeded" | "failed" | "cancelled")) {
        return Err(conflict());
    }
    Ok(())
}

fn check_read(
    tx: &Transaction<'_>,
    reference: &ArtifactRef,
    access: Option<&ArtifactAccess>,
) -> Result<u64, ForgeError> {
    let chunks = check_reference(tx, reference)?;
    if let Some(access) = access {
        authorize_run(tx, access, &reference.scope, false)?;
        let owns: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM wf_artifact_owners WHERE artifact_id=?1 AND run_id=?2)", params![reference.id,access.run_id.0], |r|r.get(0)).map_err(db_error)?;
        if !owns {
            return Err(denied());
        }
    }
    Ok(chunks)
}

fn check_reference(tx: &Transaction<'_>, reference: &ArtifactRef) -> Result<u64, ForgeError> {
    tx.query_row("SELECT chunks FROM wf_artifacts WHERE id=?1 AND scope=?2 AND media_type=?3 AND bytes=?4 AND state='ready' AND (EXISTS(SELECT 1 FROM wf_artifact_owners WHERE artifact_id=wf_artifacts.id) OR (ever_owned=0 AND orphan_after_ms>?5))", params![reference.id,reference.scope,reference.media_type,reference.bytes,now_ms()], |r|r.get(0)).optional().map_err(db_error)?.ok_or_else(unavailable)
}

pub(super) fn pin_inputs(tx: &Transaction<'_>, run: &RunSnapshot) -> Result<(), ForgeError> {
    pin_references(tx, &run.id, &run.scope, &run.artifacts)
}
pub(super) fn pin_references(
    tx: &Transaction<'_>,
    id: &RunId,
    scope: &str,
    references: &[ArtifactRef],
) -> Result<(), ForgeError> {
    for reference in references {
        if reference.scope != scope {
            return Err(denied());
        }
        check_reference(tx, reference)?;
        tx.execute(
            "INSERT INTO wf_artifact_owners(artifact_id,run_id) VALUES(?1,?2) ON CONFLICT DO NOTHING",
            params![reference.id, id.0],
        )
        .map_err(db_error)?;
        tx.execute(
            "UPDATE wf_artifacts SET ever_owned=1 WHERE id=?1",
            [&reference.id],
        )
        .map_err(db_error)?;
    }
    Ok(())
}

fn begin(
    tx: &Transaction<'_>,
    reference: &ArtifactRef,
    access: Option<&ArtifactAccess>,
    options: &SqliteOptions,
) -> Result<(), ForgeError> {
    collect(tx, now_ms())?;
    if let Some(access) = access {
        authorize_run(tx, access, &reference.scope, true)?;
    }
    let count: usize = tx
        .query_row("SELECT count(*) FROM wf_artifacts", [], |r| r.get(0))
        .map_err(db_error)?;
    if count >= options.max_artifacts {
        return Err(exhausted());
    }
    let expires = now_ms()
        .saturating_add(options.artifact_grace_ms)
        .min(i64::MAX as u64);
    tx.execute("INSERT INTO wf_artifacts(id,scope,media_type,state,bytes,chunks,orphan_after_ms,ever_owned) VALUES(?1,?2,?3,'staging',0,0,?4,?5)", params![reference.id,reference.scope,reference.media_type,expires,access.is_some()]).map_err(db_error)?;
    if let Some(access) = access {
        tx.execute(
            "INSERT INTO wf_artifact_owners(artifact_id,run_id) VALUES(?1,?2)",
            params![reference.id, access.run_id.0],
        )
        .map_err(db_error)?;
    }
    Ok(())
}

fn append(
    tx: &Transaction<'_>,
    reference: &ArtifactRef,
    access: Option<&ArtifactAccess>,
    sequence: u64,
    data: Vec<u8>,
    cap: u64,
) -> Result<(), ForgeError> {
    if let Some(access) = access {
        authorize_run(tx, access, &reference.scope, true)?;
    }
    let used: u64 = tx
        .query_row("SELECT coalesce(sum(bytes),0) FROM wf_artifacts", [], |r| {
            r.get(0)
        })
        .map_err(db_error)?;
    if data.len() as u64 > cap.saturating_sub(used) {
        return Err(exhausted());
    }
    let changed = tx.execute("UPDATE wf_artifacts SET bytes=bytes+?1,chunks=chunks+1 WHERE id=?2 AND bytes=?3 AND chunks=?4 AND state='staging'", params![data.len(),reference.id,reference.bytes,sequence]).map_err(db_error)?;
    if changed != 1 {
        return Err(conflict());
    }
    tx.execute(
        "INSERT INTO wf_artifact_chunks(artifact_id,sequence,body) VALUES(?1,?2,?3)",
        params![reference.id, sequence, data],
    )
    .map_err(db_error)?;
    Ok(())
}

pub(super) fn collect(tx: &Transaction<'_>, now: u64) -> Result<(), ForgeError> {
    tx.execute("DELETE FROM wf_artifacts WHERE state='ready' AND (ever_owned=1 OR orphan_after_ms<=?1) AND NOT EXISTS(SELECT 1 FROM wf_artifact_owners WHERE artifact_id=wf_artifacts.id)", [now]).map_err(db_error)?;
    Ok(())
}
