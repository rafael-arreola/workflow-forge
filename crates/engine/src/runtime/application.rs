use super::*;

impl WorkflowApplication {
    pub fn is_ready(&self) -> bool {
        self.shared.phase.load(Ordering::Acquire) == READY
    }
    pub(super) fn authorize(
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
            if node
                .instruction
                .operation_revision()
                .and_then(|revision| self.shared.composition.operations.get(revision))
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
        let plan = prepare_registered(&self.shared, definition, Some(&access)).await?;
        authorize_resources(&plan, &access)?;
        Ok(plan)
    }
    pub async fn start(
        &self,
        access: AccessContext,
        mut request: StartRunRequest,
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
        let durable = composition.store.capabilities().durable;
        if request.options.require_durable && !durable {
            return Err(ForgeError::new(
                "capability.unsupported",
                "This composition is ephemeral",
            ));
        }
        if !request.options.artifacts.is_empty() {
            if !access.permits_resource("artifacts")
                || request
                    .options
                    .artifacts
                    .iter()
                    .any(|reference| reference.scope != access.scope)
            {
                return Err(ForgeError::new(
                    "access.denied",
                    "Input artifacts are outside the caller's grants",
                ));
            }
            if durable && !composition.coordinated_artifacts() {
                return Err(ForgeError::new(
                    "capability.unsupported",
                    "Input artifacts require coordinated retention",
                ));
            }
            request.options.artifacts.sort_by(|a, b| a.id.cmp(&b.id));
            if request.options.artifacts.len() > 1000
                || request
                    .options
                    .artifacts
                    .windows(2)
                    .any(|pair| pair[0].id == pair[1].id)
                || serde_json::to_vec(&request.options.artifacts)
                    .expect("references serialize")
                    .len()
                    > composition.limits.value_bytes
            {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Input artifact declarations exceed their budget or contain duplicates",
                ));
            }
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
        let mut receipt_options =
            json!({"require_durable":request.options.require_durable,"timeout_ms":timeout});
        // Preserve receipt identity for existing JSON-only format-2 runs.
        if !request.options.artifacts.is_empty() {
            receipt_options["artifacts"] = json!(request.options.artifacts);
        }
        let reservation=request.options.receipt_key.as_ref().map(|key|ReceiptReservation {
            key:key.clone(),expires_at_ms:now.saturating_add(composition.limits.receipt_ttl_ms),
            request:json!({"package":request.plan.0.package.semantic_value(),"input":request.input,"options":receipt_options,"resources":access.resources}),
        });
        let deduplicated_until_ms = reservation.as_ref().map(|r| r.expires_at_ms);
        let id = RunId(uuid::Uuid::now_v7().to_string());
        let run = RunSnapshot {
            waits: BTreeMap::new(),
            checkpoint_format: CHECKPOINT_FORMAT,
            id: id.clone(),
            scope: access.scope,
            actor: access.actor,
            resources: access.resources,
            revision: 0,
            definition: request.plan.definition().clone(),
            package: request.plan.0.package.clone(),
            input: request.input,
            artifacts: request.options.artifacts,
            state: RunState::Accepted,
            invocations: BTreeMap::new(),
            output: None,
            error: None,
            created_at_ms: now,
            deadline_at_ms: now.saturating_add(timeout),
            finished_at_ms: None,
            cancel_requested: false,
            audit: Vec::new(),
            unresolved_effects: Vec::new(),
        };
        if run.retained_data_bytes() > composition.limits.run_bytes {
            return Err(ForgeError::new(
                "resource.limit",
                "Accepted data exceeds the run budget",
            ));
        }
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
                durable,
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
                    durable,
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
            self.authorize(&access, Permission::Read, false)?;
            let view = state::view(&self.shared, &id, None).await?;
            if view.head.state.is_terminal() || view.head.state == RunState::Blocked {
                let run = self.status(access.clone(), id.clone()).await?;
                if run.state.is_terminal() || run.state == RunState::Blocked {
                    return Ok(run);
                }
            }
            tokio::select! { _=notified=>(), _=tokio::time::sleep(Duration::from_millis(50))=>() }
        }
    }
    pub async fn cancel(&self, access: AccessContext, id: RunId) -> Result<(), ForgeError> {
        self.authorize(&access, Permission::Cancel, false)?;
        transition(&self.shared, &id, |run| {
            run.cancel_requested = true;
            run.state = RunState::Cancelling;
            run.close_waits();
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
    if plan
        .0
        .resources
        .iter()
        .any(|resource| !access.permits_resource(resource))
    {
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
    access: Option<&AccessContext>,
) -> Result<PreparedWorkflow, ForgeError> {
    let composition = shared.composition.clone();
    let plan = tokio::task::spawn_blocking(move || compiler::prepare(&composition, definition))
        .await
        .map_err(|_| ForgeError::new("definition.invalid", "Compiler could not complete"))??;
    if let Some(access) = access {
        authorize_resources(&plan, access)?;
    }
    let candidates: BTreeMap<_, _> = plan
        .0
        .definitions
        .iter()
        .map(|(revision, definition)| {
            (
                (revision.id.clone(), revision.revision.clone()),
                definition.semantic_value(),
            )
        })
        .collect();
    let mut versions = shared.versions.lock().await;
    for (key, value) in &candidates {
        if versions.get(key).is_some_and(|previous| previous != value) {
            return Err(ForgeError::new(
                "state.conflict",
                "Workflow revision already identifies different content",
            ));
        }
    }
    if versions.len()
        + candidates
            .keys()
            .filter(|key| !versions.contains_key(*key))
            .count()
        > 1000
    {
        return Err(ForgeError::new(
            "admission.full",
            "Prepared revision registry is full",
        ));
    }
    versions.extend(candidates);
    Ok(plan)
}

pub(super) async fn prepare_recovered(
    shared: &Arc<Shared>,
    run: RunSnapshot,
) -> Result<PreparedWorkflow, ForgeError> {
    let composition = shared.composition.clone();
    tokio::task::spawn_blocking(move || compiler::recover(&composition, &run))
        .await
        .map_err(|_| {
            ForgeError::new(
                "recovery.unavailable",
                "Recovery compiler could not complete",
            )
        })?
        .map_err(|mut error| {
            if error.code() != "recovery.unavailable" {
                error.diagnostics.insert(
                    0,
                    Diagnostic::new(
                        "recovery.unavailable",
                        "The accepted package cannot be recovered under this composition",
                    ),
                );
            }
            error
        })
}
