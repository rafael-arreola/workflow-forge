use super::*;
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum RowOutcome {
    Succeeded { index: usize, output: Value },
    Failed { index: usize, error: ForgeError },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchInput {
    state: Cursor,
    page: Page,
    results: Vec<RowOutcome>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchReport {
    partial: bool,
    start: usize,
    end: usize,
    summary: Summary,
    previous: Option<ArtifactRef>,
    rows: Vec<Value>,
}

pub(super) struct ReportBatch {
    pub descriptor: OperationDescriptor,
}
impl Operation for ReportBatch {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, ctx: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let BatchInput {
                mut state,
                page,
                results,
            } = decode(invocation.input)?;
            if page.rows.len() > MAX_BATCH
                || results.len() != page.rows.len()
                || page.cursor != state.cursor
                || state.summary.rows != state.cursor
                || state.summary.succeeded.checked_add(state.summary.failed)
                    != Some(state.summary.rows)
                || page.cursor.checked_add(page.rows.len()) != Some(page.next_cursor)
                || page.next_cursor > page.total
                || page.total > MAX_ROWS
                || (!state.more || (page.rows.is_empty() && page.total != 0))
            {
                return Err(invalid("Page, results and cursor disagree"));
            }
            let mut rows = Vec::with_capacity(page.rows.len());
            for (local, (row, result)) in page.rows.iter().zip(results).enumerate() {
                if row.index != page.cursor + local {
                    return Err(invalid("Source row identity is inconsistent"));
                }
                let mut entry =
                    serde_json::to_value(row).map_err(|_| invalid("Row cannot be encoded"))?;
                match result {
                    RowOutcome::Succeeded { index, output } => {
                        if index != local || output["index"].as_u64() != Some(row.index as u64) {
                            return Err(invalid("Result belongs to another source row"));
                        }
                        entry["status"] = json!("succeeded");
                        entry["output"] = output;
                        state.summary.succeeded += 1;
                    }
                    RowOutcome::Failed { index, error } => {
                        if index != local || error.code() == "effect.unknown" {
                            return Err(invalid("A report cannot finalize an unresolved effect"));
                        }
                        entry["status"] = json!("failed");
                        entry["error"] = json!(error);
                        state.summary.failed += 1;
                    }
                }
                rows.push(entry);
            }
            state.summary.rows = page.next_cursor;
            let report = BatchReport {
                partial: true,
                start: page.cursor,
                end: page.next_cursor,
                summary: state.summary.clone(),
                previous: state.report,
                rows,
            };
            state.report = Some(
                write_json(
                    &ctx,
                    &report,
                    "application/vnd.workflow-forge.inventory-batch+json",
                )
                .await?,
            );
            state.cursor = page.next_cursor;
            state.more = page.next_cursor < page.total;
            output(&state)
        })
    }
}
async fn read_report(
    ctx: &OperationContext,
    reference: &ArtifactRef,
) -> Result<BatchReport, OperationError> {
    let bytes = read_bytes(ctx, reference, MAX_REPORT_BYTES).await?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("Batch report is invalid"))
}
async fn references(
    ctx: &OperationContext,
    state: &Cursor,
) -> Result<Vec<ArtifactRef>, OperationError> {
    let mut next = state.report.clone();
    let mut seen = BTreeSet::new();
    let mut references = Vec::new();
    let mut end = state.cursor;
    let mut succeeded = state.summary.succeeded;
    let mut failed = state.summary.failed;
    while let Some(reference) = next {
        if !seen.insert(reference.id.clone()) || seen.len() > MAX_ROWS + 1 {
            return Err(invalid("Report chain is cyclic or exceeds budget"));
        }
        let report = read_report(ctx, &reference).await?;
        if !report.partial
            || report.end != end
            || report.start > report.end
            || report.rows.len() > MAX_BATCH
            || report.end - report.start != report.rows.len()
            || report.summary.rows != end
            || report.summary.succeeded != succeeded
            || report.summary.failed != failed
            || (report.rows.is_empty() && report.previous.is_some())
        {
            return Err(invalid("Report chain has a gap or inconsistent summary"));
        }
        for (index, row) in report.rows.iter().enumerate() {
            if row["index"].as_u64() != Some((report.start + index) as u64) {
                return Err(invalid("Report row order is inconsistent"));
            }
            match row["status"].as_str() {
                Some("succeeded") => {
                    succeeded = succeeded
                        .checked_sub(1)
                        .ok_or_else(|| invalid("Report success count is inconsistent"))?
                }
                Some("failed") => {
                    failed = failed
                        .checked_sub(1)
                        .ok_or_else(|| invalid("Report failure count is inconsistent"))?
                }
                _ => return Err(invalid("Report contains an unresolved row")),
            }
        }
        end = report.start;
        next = report.previous;
        references.push(reference);
    }
    if end != 0 || succeeded != 0 || failed != 0 {
        return Err(invalid("Report chain is incomplete"));
    }
    references.reverse();
    Ok(references)
}

pub(super) struct PublishReport {
    pub descriptor: OperationDescriptor,
}
impl Operation for PublishReport {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, ctx: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let state: Cursor = decode(invocation.input)?;
            if state.more
                || state.cursor != state.summary.rows
                || state.cursor > MAX_ROWS
                || state.summary.succeeded.checked_add(state.summary.failed) != Some(state.cursor)
            {
                return Err(invalid("A final report requires all rows to be settled"));
            }
            let references = references(&ctx, &state).await?;
            let content = stream::try_unfold(
                (references.into_iter(), ctx.clone()),
                |(mut references, ctx)| async move {
                    let Some(reference) = references.next() else {
                        return Ok(None);
                    };
                    let report = read_report(&ctx, &reference)
                        .await
                        .map_err(|e| ForgeError::new(&e.code, &e.message))?;
                    let mut bytes = Vec::new();
                    for row in report.rows {
                        serde_json::to_writer(&mut bytes, &row).map_err(|_| {
                            ForgeError::new(
                                "reference.inventory.invalid",
                                "Report cannot be encoded",
                            )
                        })?;
                        bytes.push(b'\n');
                    }
                    Ok::<_, ForgeError>(Some((bytes, (references, ctx))))
                },
            );
            let report = ctx
                .write_artifact(Box::pin(content), "application/x-ndjson")
                .await
                .map_err(|_| artifact_error(EffectCertainty::Unknown))?;
            Ok(OperationOutput::json(
                json!({"rows":state.summary.rows,"succeeded":state.summary.succeeded,"failed":state.summary.failed,"report":report}),
            ))
        })
    }
}
