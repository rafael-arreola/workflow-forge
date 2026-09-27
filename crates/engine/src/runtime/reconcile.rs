use super::*;

fn conflict(message: &str) -> ForgeError {
    ForgeError::new("state.conflict", message)
}
fn evidence_valid(evidence: &EffectEvidence) -> bool {
    !evidence.authority.trim().is_empty() && !evidence.reference.trim().is_empty()
}
fn previous_receipt(
    run: &RunSnapshot,
    command: &ReconcileCommand,
    actor: &str,
) -> Result<Option<ReconcileReceipt>, ForgeError> {
    for entry in &run.audit {
        if let AuditEntry::Resolution(audit) = entry {
            if audit.command.command_id == command.command_id {
                return if audit.command == *command && audit.actor == actor {
                    Ok(Some(audit.receipt.clone()))
                } else {
                    Err(conflict(
                        "Command identity was used with different content or actor",
                    ))
                };
            }
        }
    }
    Ok(None)
}

impl WorkflowApplication {
    pub async fn inspect_effect(
        &self,
        access: AccessContext,
        id: RunId,
        invocation_id: String,
    ) -> Result<EffectInspection, ForgeError> {
        self.authorize(&access, Permission::Reconcile, false)?;
        let c = &self.shared.composition;
        let _permit = tokio::time::timeout(
            Duration::from_millis(c.limits.attempt_timeout_ms),
            self.shared.attempts.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ForgeError::new("admission.full", "No capacity for effect inspection"))?
        .map_err(|_| unavailable())?;
        let run = state::read(&self.shared, &id).await?;
        if run.state != RunState::Blocked {
            return Err(conflict("Effect inspection requires a settled blocked run"));
        }
        let record = run
            .invocations
            .values()
            .find(|r| r.id == invocation_id && r.state == InvocationState::Unknown)
            .ok_or_else(|| conflict("Invocation is not awaiting effect resolution"))?;
        let revision = record.operation.as_ref().ok_or_else(|| {
            ForgeError::new(
                "capability.unsupported",
                "Control instruction has no effect inspector",
            )
        })?;
        let operation = compiler::recover_operation(c, &run.package, revision)?;
        require_resources(&access, &operation.descriptor)?;
        let inspector = c.inspectors.get(revision).ok_or_else(|| {
            ForgeError::new(
                "capability.unsupported",
                "This operation has no effect inspector",
            )
        })?;
        let cancel = self.shared.cancel.child_token();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let context = OperationContext::new(
            ArtifactAccess {
                runtime_owner: c.id.clone(),
                run_id: id,
            },
            now_ms().saturating_add(c.limits.attempt_timeout_ms),
            cancel.clone(),
            run.scope,
            operation.descriptor.required_resources.clone(),
            c.secrets.clone(),
            c.artifacts.clone(),
        );
        let invocation = Invocation {
            id: record.id.clone(),
            attempt_id: record.attempt_id.clone(),
            operation: revision.clone(),
            input: record.input.clone(),
            config: record.config.clone(),
            effect_key: record.effect_key.clone(),
        };
        let future =
            AssertUnwindSafe(async { inspector.inspect(context, invocation).await }).catch_unwind();
        let result = tokio::select! {
            _=cancel.cancelled()=>Err(unavailable()),
            outcome=tokio::time::timeout(Duration::from_millis(c.limits.attempt_timeout_ms),future)=>match outcome {
                Ok(Ok(value))=>value,
                Ok(Err(_))=>Err(ForgeError::new("effect.inspection_failed","Inspector panicked")),
                Err(_)=>Err(ForgeError::new("effect.inspection_failed","Inspector reached its deadline")),
            }
        };
        cancel.cancel();
        let result = result?;
        crate::schema::check_value(
            &serde_json::to_value(&result).expect("inspection serializes"),
            c.limits.evidence_bytes,
            c.limits.json_depth,
        )?;
        Ok(result)
    }

    pub async fn reconcile(
        &self,
        access: AccessContext,
        command: ReconcileCommand,
    ) -> Result<ReconcileReceipt, ForgeError> {
        let _admission = self.shared.admission.lock().await;
        self.authorize(&access, Permission::Reconcile, true)?;
        if matches!(command.resolution, EffectResolution::StopTracking { .. })
            && !access.permissions.contains(&Permission::StopTracking)
        {
            return Err(ForgeError::new(
                "access.denied",
                "Stopping effect tracking requires its own permission",
            ));
        }
        let c = &self.shared.composition;
        if command.command_id.is_empty() || command.command_id.len() > 256 {
            return Err(ForgeError::new(
                "definition.invalid",
                "Invalid reconciliation command identity",
            ));
        }
        crate::schema::check_value(
            &serde_json::to_value(&command).expect("command serializes"),
            c.limits.evidence_bytes,
            c.limits.json_depth,
        )?;
        let mut run = state::read(&self.shared, &command.run_id).await?;
        if let Some(receipt) = previous_receipt(&run, &command, &access.actor)? {
            return Ok(receipt);
        }
        if run.revision != command.expected_revision || run.state != RunState::Blocked {
            return Err(conflict(
                "Resolution must match the current blocked run revision",
            ));
        }
        if run.audit.len() >= c.limits.audit_entries {
            return Err(ForgeError::new(
                "resource.limit",
                "Investigation retention is full",
            ));
        }
        let key = run
            .invocations
            .iter()
            .find(|(_, r)| {
                r.id == command.invocation_id
                    && r.attempt_id == command.observed_attempt
                    && r.state == InvocationState::Unknown
            })
            .map(|(key, _)| key.clone())
            .ok_or_else(|| conflict("Invocation or observed attempt is no longer unresolved"))?;
        let record = run.invocations.get_mut(&key).expect("resolved key");
        let descriptor = record.operation.as_ref().and_then(|revision| {
            run.package
                .operations
                .iter()
                .find(|d| &d.revision == revision)
        });
        if let Some(descriptor) = descriptor {
            require_resources(&access, descriptor)?;
        }
        let mut diagnostic = None;
        let status = match &command.resolution {
            EffectResolution::ConfirmApplied { output, evidence } => {
                if !evidence_valid(evidence) {
                    return Err(ForgeError::new(
                        "effect.evidence_invalid",
                        "Authoritative evidence reference is required",
                    ));
                }
                let revision = record.operation.as_ref().ok_or_else(|| {
                    ForgeError::new("reference.missing", "Operation revision is unavailable")
                })?;
                let operation = compiler::recover_operation(c, &run.package, revision)?;
                match operation.output.validate(output, &c.limits) {
                    Ok(()) => {
                        record.output = Some(output.clone());
                        record.input = Value::Null;
                        record.error = None;
                        record.next_attempt_at_ms = None;
                        record.certainty = EffectCertainty::Applied;
                        record.state = InvocationState::Succeeded;
                        ResolutionStatus::Applied
                    }
                    Err(error) => {
                        record.certainty = EffectCertainty::Applied;
                        record.error = Some(error.clone());
                        diagnostic = Some(error);
                        ResolutionStatus::StillBlocked
                    }
                }
            }
            EffectResolution::ConfirmNotApplied {
                evidence,
                quiescent,
                retry,
            } => {
                if !evidence_valid(evidence) {
                    return Err(ForgeError::new(
                        "effect.evidence_invalid",
                        "Authoritative evidence reference is required",
                    ));
                }
                if !quiescent || record.certainty == EffectCertainty::Applied {
                    diagnostic = Some(ForgeError::new(
                        "effect.evidence_insufficient",
                        "Non-application needs quiescence and must not contradict a confirmed effect",
                    ));
                    ResolutionStatus::StillBlocked
                } else {
                    record.certainty = EffectCertainty::NotApplied;
                    record.next_attempt_at_ms = None;
                    if *retry
                        && record.attempts < record.retry.max_attempts
                        && now_ms() < run.deadline_at_ms
                        && !run.cancel_requested
                    {
                        record.state = InvocationState::Pending;
                        record.error = None;
                        ResolutionStatus::RetryReady
                    } else {
                        record.state = InvocationState::Failed;
                        record.error = Some(state::operation_failure(
                            "operation.failed",
                            ErrorClass::Rejected,
                            EffectCertainty::NotApplied,
                            "Effect was not applied and no new attempt was authorized",
                        ));
                        ResolutionStatus::Failed
                    }
                }
            }
            EffectResolution::RecordInconclusive { reason, .. } => {
                if reason.trim().is_empty() {
                    return Err(ForgeError::new(
                        "effect.evidence_invalid",
                        "Investigation reason is required",
                    ));
                }
                ResolutionStatus::StillBlocked
            }
            EffectResolution::StopTracking { reason, .. } => {
                if reason.trim().is_empty() {
                    return Err(ForgeError::new(
                        "effect.evidence_invalid",
                        "Reason for stopping tracking is required",
                    ));
                }
                run.unresolved_effects = run
                    .invocations
                    .values()
                    .filter(|r| r.state == InvocationState::Unknown)
                    .map(|r| UnresolvedEffect {
                        invocation_id: r.id.clone(),
                        attempt_id: r.attempt_id.clone(),
                        effect_key: r.effect_key.clone(),
                        reason: reason.chars().take(1024).collect(),
                    })
                    .collect();
                run.state = if run.cancel_requested {
                    RunState::Cancelled
                } else {
                    RunState::Failed
                };
                run.close_waits();
                run.error = Some(ForgeError::new(
                    "effect.unresolved",
                    "Tracking stopped with unresolved external effects",
                ));
                run.finished_at_ms = Some(now_ms());
                ResolutionStatus::StoppedTracking
            }
        };
        if !run.state.is_terminal()
            && !run
                .invocations
                .values()
                .any(|r| r.state == InvocationState::Unknown)
        {
            run.state = if run.cancel_requested {
                RunState::Cancelling
            } else {
                RunState::Accepted
            };
            run.error = None;
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| conflict("Revision exhausted"))?;
        let receipt = ReconcileReceipt {
            command_id: command.command_id.clone(),
            run_id: run.id.clone(),
            revision: run.revision,
            status,
            diagnostic,
        };
        run.audit
            .push(AuditEntry::Resolution(Box::new(ResolutionAudit {
                command: command.clone(),
                actor: access.actor.clone(),
                at_ms: now_ms(),
                receipt: receipt.clone(),
            })));
        if state::retained_bytes(&run) > c.limits.run_bytes {
            return Err(ForgeError::new(
                "resource.limit",
                "Evidence or confirmed output exceeds run retention budget",
            ));
        }
        match c
            .store
            .commit(&c.id, command.expected_revision, run.clone())
            .await
        {
            Ok(()) => {
                state::publish(&self.shared, &run);
                self.shared.wake.notify_one();
                Ok(receipt)
            }
            Err(error) if error.code() == "state.conflict" => {
                let current = state::read(&self.shared, &command.run_id).await?;
                previous_receipt(&current, &command, &access.actor)?.ok_or(error)
            }
            Err(error) => Err(error),
        }
    }
}

fn require_resources(
    access: &AccessContext,
    descriptor: &OperationDescriptor,
) -> Result<(), ForgeError> {
    if descriptor
        .required_resources
        .iter()
        .any(|r| !access.permits_resource(r))
    {
        Err(ForgeError::new(
            "access.denied",
            "Caller lacks effect resource permissions",
        ))
    } else {
        Ok(())
    }
}
