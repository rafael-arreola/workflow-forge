use super::*;

impl WorkflowApplication {
    pub fn is_ready(&self) -> bool {
        self.shared.phase.load(Ordering::Acquire) == READY
    }
    fn authorize(
        &self,
        access: &AccessContext,
        permission: Permission,
        admission: bool,
    ) -> Result<(), ForgeError> {
        let phase = self.shared.phase.load(Ordering::Acquire);
        if phase >= STOPPED || (admission && phase != READY) {
            return Err(unavailable());
        }
        if access.scope != self.shared.composition.scope
            || !access.permissions.contains(&permission)
        {
            return Err(ForgeError::new(
                "access.denied",
                "Command is not permitted in this scope",
            ));
        }
        Ok(())
    }
    pub async fn close_admission(&self) {
        let _guard = self.shared.admission.lock().await;
        let _ = self.shared.phase.compare_exchange(
            READY,
            DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.shared.wake.notify_one();
    }
    pub fn catalog(&self, access: &AccessContext) -> Result<Vec<OperationDescriptor>, ForgeError> {
        self.authorize(access, Permission::Read, false)?;
        Ok(self
            .shared
            .composition
            .operations
            .values()
            .filter(|op| {
                op.descriptor
                    .required_resources
                    .iter()
                    .all(|r| access.permits_resource(r))
            })
            .map(|op| op.descriptor.clone())
            .collect())
    }
    pub async fn prepare_json(
        &self,
        access: AccessContext,
        json: &[u8],
    ) -> Result<PreparedWorkflow, ForgeError> {
        self.authorize(&access, Permission::Prepare, true)?;
        if json.len() > self.shared.composition.limits.document_bytes {
            return Err(ForgeError::new(
                "definition.invalid",
                "Document exceeds its byte limit",
            ));
        }
        let value: Value = serde_json::from_slice(json)
            .map_err(|_| ForgeError::new("definition.invalid", "Invalid JSON document"))?;
        if value
            .get("nodes")
            .and_then(Value::as_array)
            .is_some_and(|nodes| {
                nodes
                    .iter()
                    .any(|n| n.get("kind").and_then(Value::as_str) != Some("operation"))
            })
        {
            return Err(ForgeError::new(
                "capability.unsupported",
                "This profile supports operation sequences",
            ));
        }
        let definition = serde_json::from_value(value).map_err(|_| {
            ForgeError::new(
                "definition.invalid",
                "Document fields do not match forge.workflow/2",
            )
        })?;
        self.prepare(access, definition).await
    }
    pub async fn prepare(
        &self,
        access: AccessContext,
        definition: WorkflowDefinition,
    ) -> Result<PreparedWorkflow, ForgeError> {
        let _guard = self.shared.admission.lock().await;
        self.authorize(&access, Permission::Prepare, true)?;
        for node in &definition.nodes {
            if self
                .shared
                .composition
                .operations
                .get(&node.operation)
                .is_some_and(|op| {
                    op.descriptor
                        .required_resources
                        .iter()
                        .any(|r| !access.permits_resource(r))
                })
            {
                return Err(ForgeError::new(
                    "access.denied",
                    "Plan requires a resource not granted to this caller",
                ));
            }
        }
        let plan = prepare_registered(&self.shared, definition).await?;
        authorize_resources(&plan, &access)?;
        Ok(plan)
    }
    pub async fn start(
        &self,
        access: AccessContext,
        request: StartRunRequest,
    ) -> Result<StartReceipt, ForgeError> {
        let _guard = self.shared.admission.lock().await;
        self.authorize(&access, Permission::Start, true)?;
        let composition = &self.shared.composition;
        if request.plan.0.composition != composition.id {
            return Err(ForgeError::new(
                "state.conflict",
                "Plan belongs to another composition",
            ));
        }
        authorize_resources(&request.plan, &access)?;
        if request.options.require_durable {
            return Err(ForgeError::new(
                "capability.unsupported",
                "This composition is ephemeral",
            ));
        }
        let timeout = request
            .options
            .timeout_ms
            .unwrap_or(composition.limits.run_timeout_ms)
            .min(composition.limits.run_timeout_ms);
        if timeout == 0
            || request
                .options
                .receipt_key
                .as_ref()
                .is_some_and(|k| k.is_empty() || k.len() > 256)
        {
            return Err(ForgeError::new(
                "definition.invalid",
                "Invalid start deadline or receipt key",
            ));
        }
        request
            .plan
            .0
            .input
            .validate(&request.input, &composition.limits)?;
        let now = now_ms();
        composition
            .store
            .collect(&composition.id, now, &composition.limits)
            .await?;
        let reservation=request.options.receipt_key.as_ref().map(|key|ReceiptReservation {
            key:key.clone(),expires_at_ms:now.saturating_add(composition.limits.receipt_ttl_ms),
            request:json!({"definition":request.plan.definition().semantic_value(),"input":request.input,"options":{"require_durable":false,"timeout_ms":timeout},"resources":access.resources}),
        });
        let deduplicated_until_ms = reservation.as_ref().map(|r| r.expires_at_ms);
        let id = RunId(uuid::Uuid::now_v7().to_string());
        let run = RunSnapshot {
            checkpoint_format: 1,
            id: id.clone(),
            scope: access.scope,
            actor: access.actor,
            resources: access.resources,
            revision: 0,
            definition: request.plan.definition().clone(),
            input: request.input,
            state: RunState::Accepted,
            invocations: BTreeMap::new(),
            output: None,
            error: None,
            created_at_ms: now,
            deadline_at_ms: now.saturating_add(timeout),
            finished_at_ms: None,
            cancel_requested: false,
        };
        match composition
            .store
            .create(&composition.id, run, reservation, &composition.limits)
            .await?
        {
            CreateOutcome::Duplicate {
                run_id,
                expires_at_ms,
            } => Ok(StartReceipt {
                run_id,
                durable: false,
                duplicate: true,
                deduplicated_until_ms: Some(expires_at_ms),
            }),
            CreateOutcome::Created => {
                self.shared
                    .plans
                    .lock()
                    .await
                    .insert(id.clone(), request.plan);
                self.shared.wake.notify_one();
                Ok(StartReceipt {
                    run_id: id,
                    durable: false,
                    duplicate: false,
                    deduplicated_until_ms,
                })
            }
        }
    }
    pub async fn status(
        &self,
        access: AccessContext,
        id: RunId,
    ) -> Result<RunSnapshot, ForgeError> {
        self.authorize(&access, Permission::Read, false)?;
        let c = &self.shared.composition;
        c.store.collect(&c.id, now_ms(), &c.limits).await?;
        c.store
            .get(&id)
            .await?
            .filter(|run| run.scope == access.scope)
            .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))
    }
    pub async fn result(&self, access: AccessContext, id: RunId) -> Result<Value, ForgeError> {
        let run = self.status(access, id).await?;
        if let Some(output) = run.output {
            return Ok(output);
        }
        if let Some(error) = run.error {
            return Err(error);
        }
        Err(ForgeError::new(
            "not_ready",
            "Run has not produced a final result",
        ))
    }
    pub async fn wait(&self, access: AccessContext, id: RunId) -> Result<RunSnapshot, ForgeError> {
        loop {
            let notified = self.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let run = self.status(access.clone(), id.clone()).await?;
            if run.state.is_terminal() || run.state == RunState::Blocked {
                return Ok(run);
            }
            tokio::select! { _=notified=>(), _=tokio::time::sleep(Duration::from_millis(50))=>() }
        }
    }
    pub async fn cancel(&self, access: AccessContext, id: RunId) -> Result<(), ForgeError> {
        self.authorize(&access, Permission::Cancel, false)?;
        transition(&self.shared, &id, |run| {
            run.cancel_requested = true;
            run.state = RunState::Cancelling;
        })
        .await?;
        if let Some(token) = self.shared.cancellations.lock().await.get(&id) {
            token.cancel();
        }
        self.shared.wake.notify_one();
        Ok(())
    }
}

fn authorize_resources(plan: &PreparedWorkflow, access: &AccessContext) -> Result<(), ForgeError> {
    if plan.0.sequence.iter().any(|node| {
        node.operation
            .descriptor
            .required_resources
            .iter()
            .any(|r| !access.permits_resource(r))
    }) {
        Err(ForgeError::new(
            "access.denied",
            "Plan requires a resource not granted to this caller",
        ))
    } else {
        Ok(())
    }
}

pub(super) async fn prepare_registered(
    shared: &Arc<Shared>,
    definition: WorkflowDefinition,
) -> Result<PreparedWorkflow, ForgeError> {
    let composition = shared.composition.clone();
    let plan = tokio::task::spawn_blocking(move || compiler::prepare(&composition, definition))
        .await
        .map_err(|_| ForgeError::new("definition.invalid", "Compiler could not complete"))??;
    let key = (
        plan.definition().id.clone(),
        plan.definition().revision.clone(),
    );
    let semantic = plan.definition().semantic_value();
    let mut versions = shared.versions.lock().await;
    if let Some(previous) = versions.get(&key) {
        if previous != &semantic {
            return Err(ForgeError::new(
                "state.conflict",
                "Workflow revision already identifies different content",
            ));
        }
    } else {
        if versions.len() >= 1000 {
            return Err(ForgeError::new(
                "admission.full",
                "Prepared revision registry is full",
            ));
        }
        versions.insert(key, semantic);
    }
    Ok(plan)
}
