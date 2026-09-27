use super::*;

fn evidence(run: &RunSnapshot) -> EffectEvidence {
    EffectEvidence {
        authority: "test.process.ledger".into(),
        reference: run.invocations["/nodes/write"].effect_key.clone().unwrap(),
        note: "The original controlled writer process has terminated; its ledger is available"
            .into(),
    }
}
fn command(run: &RunSnapshot, id: &str, resolution: EffectResolution) -> ReconcileCommand {
    let record = &run.invocations["/nodes/write"];
    ReconcileCommand {
        command_id: id.into(),
        run_id: run.id.clone(),
        invocation_id: record.id.clone(),
        expected_revision: run.revision,
        observed_attempt: record.attempt_id.clone(),
        resolution,
    }
}

pub async fn resolve_for_crash(app: &WorkflowApplication, mut run: RunSnapshot, mode: &str) {
    assert_eq!(run.state, RunState::Blocked);
    if mode == "resolution:after_cancelled_stop" {
        app.cancel(access(), run.id.clone()).await.unwrap();
        run = wait(app, run.id).await;
        assert_eq!(run.state, RunState::Blocked);
    }
    let decision = match mode {
        "resolution:before_applied" | "resolution:after_applied" => {
            EffectResolution::ConfirmApplied {
                output: json!(7),
                evidence: evidence(&run),
            }
        }
        "resolution:after_retry" => EffectResolution::ConfirmNotApplied {
            evidence: evidence(&run),
            quiescent: true,
            retry: true,
        },
        "resolution:after_inconclusive" => EffectResolution::RecordInconclusive {
            reason: "Awaiting destination evidence".into(),
            evidence: None,
        },
        "resolution:after_stop" | "resolution:after_cancelled_stop" => {
            EffectResolution::StopTracking {
                reason: "Host closes tracking without asserting non-application".into(),
                evidence: None,
            }
        }
        _ => panic!("unknown crash mode {mode}"),
    };
    app.reconcile(access(), command(&run, "crash-decision", decision))
        .await
        .unwrap();
}

#[tokio::test]
async fn inconclusive_competing_and_unauthorized_resolutions_preserve_the_durable_block() {
    let dir = Directory::new();
    crash_at(&dir, "intent").await;
    let runtime = EngineRuntime::boot(
        builder(store(&dir.db()), dir.effects(), "non-quiescent")
            .build()
            .unwrap(),
        BootOptions::default(),
    )
    .await
    .unwrap();
    let app = runtime.application();
    let receipt = request(&app, json!(7)).await.unwrap();
    let run = wait(&app, receipt.run_id.clone()).await;
    assert_eq!(run.state, RunState::Blocked);
    let record = &run.invocations["/nodes/write"];
    let finding = app
        .inspect_effect(access(), run.id.clone(), record.id.clone())
        .await
        .unwrap();
    assert!(matches!(
        finding,
        EffectInspection::NotApplied {
            quiescent: false,
            ..
        }
    ));
    assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), run);
    let inconclusive = command(
        &run,
        "investigation",
        EffectResolution::RecordInconclusive {
            reason: "No definitive answer yet".into(),
            evidence: None,
        },
    );
    let mut unauthorized = access();
    unauthorized.permissions.remove(&Permission::Reconcile);
    for caller in [unauthorized, AccessContext::trusted("foreign")] {
        assert_eq!(
            app.reconcile(caller.clone(), inconclusive.clone())
                .await
                .unwrap_err()
                .code(),
            "access.denied"
        );
        assert_eq!(
            app.inspect_effect(caller, run.id.clone(), record.id.clone())
                .await
                .unwrap_err()
                .code(),
            "access.denied"
        );
    }
    let mut invalid = evidence(&run);
    invalid.authority.clear();
    assert_eq!(
        app.reconcile(
            access(),
            command(
                &run,
                "invalid-evidence",
                EffectResolution::ConfirmApplied {
                    output: json!(7),
                    evidence: invalid
                }
            )
        )
        .await
        .unwrap_err()
        .code(),
        "effect.evidence_invalid"
    );
    assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), run);

    let insufficient = command(
        &run,
        "absence-is-not-proof",
        EffectResolution::ConfirmNotApplied {
            evidence: evidence(&run),
            quiescent: false,
            retry: true,
        },
    );
    let refused = app.reconcile(access(), insufficient.clone()).await.unwrap();
    assert_eq!(refused.status, ResolutionStatus::StillBlocked);
    assert_eq!(
        refused.diagnostic.as_ref().unwrap().code(),
        "effect.evidence_insufficient"
    );
    let run = app.status(access(), run.id.clone()).await.unwrap();
    assert_eq!(
        run.invocations["/nodes/write"].state,
        InvocationState::Unknown
    );
    let a = command(&run, "investigation-a", inconclusive.resolution.clone());
    let b = command(&run, "investigation-b", inconclusive.resolution);
    let (left, right) = tokio::join!(
        app.reconcile(access(), a.clone()),
        app.reconcile(access(), b.clone())
    );
    let (winner, accepted, error) = match (left, right) {
        (Ok(receipt), Err(error)) => (a, receipt, error),
        (Err(error), Ok(receipt)) => (b, receipt, error),
        other => panic!("Expected exactly one decision: {other:?}"),
    };
    assert_eq!(error.code(), "state.conflict");
    assert_eq!(accepted.status, ResolutionStatus::StillBlocked);
    let blocked = app.status(access(), run.id.clone()).await.unwrap();
    assert_eq!(blocked.audit.len(), 2);
    assert_eq!(blocked.state, RunState::Blocked);
    assert!(records(&dir.effects()).is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), blocked);
    assert_eq!(
        app.reconcile(access(), insufficient).await.unwrap(),
        refused
    );
    assert_eq!(
        app.reconcile(access(), winner.clone()).await.unwrap(),
        accepted
    );
    let mut changed = winner.clone();
    changed.observed_attempt = "another-attempt".into();
    assert_eq!(
        app.reconcile(access(), changed).await.unwrap_err().code(),
        "state.conflict"
    );
    let mut other_actor = access();
    other_actor.actor = "another-operator".into();
    assert_eq!(
        app.reconcile(other_actor, winner).await.unwrap_err().code(),
        "state.conflict"
    );
    let denied_stop = command(
        &blocked,
        "stop",
        EffectResolution::StopTracking {
            reason: "close tracking".into(),
            evidence: None,
        },
    );
    let mut caller = access();
    caller.permissions.remove(&Permission::StopTracking);
    assert_eq!(
        app.reconcile(caller, denied_stop).await.unwrap_err().code(),
        "access.denied"
    );
    assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), blocked);
    let no_retry = command(
        &blocked,
        "definitive-no-retry",
        EffectResolution::ConfirmNotApplied {
            evidence: evidence(&blocked),
            quiescent: true,
            retry: false,
        },
    );
    let resolved = app.reconcile(access(), no_retry.clone()).await.unwrap();
    assert_eq!(resolved.status, ResolutionStatus::Failed);
    let terminal = wait(&app, run.id.clone()).await;
    assert_eq!(terminal.state, RunState::Failed);
    assert_eq!(terminal.invocations["/nodes/write"].attempts, 1);
    assert!(records(&dir.effects()).is_empty());
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    assert_eq!(app.status(access(), run.id).await.unwrap(), terminal);
    assert_eq!(app.reconcile(access(), no_retry).await.unwrap(), resolved);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn invalid_applied_output_keeps_certainty_and_audit_across_restarts_until_corrected() {
    let dir = Directory::new();
    crash_at(&dir, "effect").await;
    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    let run = wait(&app, request(&app, json!(7)).await.unwrap().run_id).await;
    let invalid = command(
        &run,
        "bad-output",
        EffectResolution::ConfirmApplied {
            output: json!("not-an-integer"),
            evidence: evidence(&run),
        },
    );
    let rejected = app.reconcile(access(), invalid.clone()).await.unwrap();
    assert_eq!(rejected.status, ResolutionStatus::StillBlocked);
    assert_eq!(rejected.diagnostic.as_ref().unwrap().code(), "data.invalid");
    let blocked = app.status(access(), run.id.clone()).await.unwrap();
    assert_eq!(
        blocked.invocations["/nodes/write"].certainty,
        EffectCertainty::Applied
    );
    assert_eq!(
        blocked.invocations["/nodes/write"].state,
        InvocationState::Unknown
    );
    assert!(!blocked.invocations.contains_key("/nodes/return"));
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();

    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), blocked);
    assert_eq!(app.reconcile(access(), invalid).await.unwrap(), rejected);
    let contradiction = command(
        &blocked,
        "contradiction",
        EffectResolution::ConfirmNotApplied {
            evidence: evidence(&blocked),
            quiescent: true,
            retry: true,
        },
    );
    assert_eq!(
        app.reconcile(access(), contradiction).await.unwrap().status,
        ResolutionStatus::StillBlocked
    );
    let blocked = app.status(access(), run.id.clone()).await.unwrap();
    assert_eq!(
        blocked.invocations["/nodes/write"].certainty,
        EffectCertainty::Applied
    );
    let corrected = command(
        &blocked,
        "corrected-output",
        EffectResolution::ConfirmApplied {
            output: json!(7),
            evidence: evidence(&blocked),
        },
    );
    let receipt = app.reconcile(access(), corrected.clone()).await.unwrap();
    assert_eq!(receipt.status, ResolutionStatus::Applied);
    let terminal = wait(&app, run.id.clone()).await;
    assert_eq!(terminal.state, RunState::Succeeded);
    assert_eq!(terminal.output, Some(json!(7)));
    assert_eq!(records(&dir.effects()).len(), 1);
    assert_eq!(terminal.audit.len(), 3);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    let runtime = boot(&dir.db(), dir.effects()).await;
    let app = runtime.application();
    assert_eq!(app.status(access(), run.id).await.unwrap(), terminal);
    assert_eq!(app.reconcile(access(), corrected).await.unwrap(), receipt);
    assert_eq!(records(&dir.effects()).len(), 1);
    runtime.shutdown(ShutdownOptions::default()).await.unwrap();
}

#[tokio::test]
async fn crashing_before_or_after_resolution_keeps_decision_audit_and_ack_atomic() {
    for mode in [
        "before_applied",
        "after_applied",
        "after_retry",
        "after_inconclusive",
        "after_stop",
        "after_cancelled_stop",
    ] {
        let dir = Directory::new();
        crash_at(
            &dir,
            if mode == "after_retry" {
                "intent"
            } else {
                "effect"
            },
        )
        .await;
        crash_at(&dir, &format!("resolution:{mode}")).await;
        let runtime = boot(&dir.db(), dir.effects()).await;
        let app = runtime.application();
        let accepted = request(&app, json!(7)).await.unwrap();
        assert!(accepted.durable && accepted.duplicate);
        let run = wait(&app, accepted.run_id.clone()).await;
        if mode == "before_applied" {
            assert_eq!(run.state, RunState::Blocked);
            assert!(run.audit.is_empty());
            assert_eq!(
                run.invocations["/nodes/write"].state,
                InvocationState::Unknown
            );
            assert!(run.output.is_none());
        } else {
            let expected = match mode {
                "after_applied" => ResolutionStatus::Applied,
                "after_retry" => ResolutionStatus::RetryReady,
                "after_inconclusive" => ResolutionStatus::StillBlocked,
                _ => ResolutionStatus::StoppedTracking,
            };
            assert_eq!(run.audit.len(), 1);
            let AuditEntry::Resolution(audit) = &run.audit[0] else {
                panic!("missing atomic audit");
            };
            assert_eq!(audit.receipt.status, expected);
            assert_eq!(audit.actor, "host");
            assert_eq!(
                app.reconcile(access(), audit.command.clone())
                    .await
                    .unwrap(),
                audit.receipt
            );
            match expected {
                ResolutionStatus::Applied | ResolutionStatus::RetryReady => {
                    assert_eq!(run.state, RunState::Succeeded);
                    assert_eq!(run.output, Some(json!(7)));
                    assert_eq!(
                        run.invocations["/nodes/write"].attempts,
                        if mode == "after_retry" { 2 } else { 1 }
                    );
                }
                ResolutionStatus::StillBlocked => {
                    assert_eq!(run.state, RunState::Blocked);
                    assert_eq!(
                        run.invocations["/nodes/write"].state,
                        InvocationState::Unknown
                    );
                }
                ResolutionStatus::StoppedTracking => {
                    assert_eq!(
                        run.state,
                        if mode == "after_stop" {
                            RunState::Failed
                        } else {
                            RunState::Cancelled
                        }
                    );
                    assert_eq!(run.error.as_ref().unwrap().code(), "effect.unresolved");
                    assert!(run.output.is_none());
                    assert_eq!(run.unresolved_effects.len(), 1);
                    let effect = &run.unresolved_effects[0];
                    let record = &run.invocations["/nodes/write"];
                    assert_eq!(effect.invocation_id, record.id);
                    assert_eq!(effect.attempt_id, record.attempt_id);
                    assert_eq!(effect.effect_key, record.effect_key);
                    assert_eq!(record.certainty, EffectCertainty::Unknown);
                    assert!(!run.invocations.contains_key("/nodes/return"));
                    assert_eq!(
                        app.reconcile(
                            access(),
                            command(
                                &run,
                                "cannot-reopen",
                                EffectResolution::ConfirmApplied {
                                    output: json!(7),
                                    evidence: evidence(&run)
                                }
                            )
                        )
                        .await
                        .unwrap_err()
                        .code(),
                        "state.conflict"
                    );
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(records(&dir.effects()).len(), 1, "{mode}: effect repeated");
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
        let runtime = boot(&dir.db(), dir.effects()).await;
        let app = runtime.application();
        assert_eq!(app.status(access(), run.id.clone()).await.unwrap(), run);
        for entry in &run.audit {
            let AuditEntry::Resolution(audit) = entry else {
                unreachable!()
            };
            assert_eq!(
                app.reconcile(access(), audit.command.clone())
                    .await
                    .unwrap(),
                audit.receipt
            );
        }
        assert_eq!(records(&dir.effects()).len(), 1);
        runtime.shutdown(ShutdownOptions::default()).await.unwrap();
    }
}
