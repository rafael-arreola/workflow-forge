use super::*;

impl EngineRuntime {
    pub async fn boot(assembly: EngineAssembly, options: BootOptions) -> Result<Self, ForgeError> {
        let composition = assembly.0;
        composition.store.claim(&composition.id).await?;
        let (events, mut receiver) = mpsc::channel::<ExecutionEvent>(128);
        let (late, late_receiver) = mpsc::channel(composition.limits.concurrent_attempts);
        let shared = Arc::new(Shared {
            attempts: Arc::new(Semaphore::new(composition.limits.concurrent_attempts)),
            scope_slots: Arc::new(Semaphore::new(composition.limits.active_scopes)),
            composition: composition.clone(),
            phase: AtomicU8::new(DRAINING),
            admission: Mutex::new(()),
            cancel: CancellationToken::new(),
            wake: Notify::new(),
            changed: Notify::new(),
            plans: Mutex::new(BTreeMap::new()),
            versions: Mutex::new(BTreeMap::new()),
            cancellations: Mutex::new(BTreeMap::new()),
            events,
            late,
        });
        let initialization = async {
            for definition in options.definitions {
                prepare_registered(&shared, definition, None).await?;
            }
            for mut run in composition.store.unfinished().await? {
                if run.scope != composition.scope {
                    return Err(ForgeError::new(
                        "state.conflict",
                        "Store contains a different execution scope",
                    ));
                }
                // Resolve exact revisions before classifying unfinished attempts.
                match prepare_registered(&shared, run.definition.clone(), None).await {
                    Ok(plan) => {
                        recovery::classify_unfinished(&shared, &run).await?;
                        shared.plans.lock().await.insert(run.id.clone(), plan);
                    }
                    Err(error) => {
                        let revision = run.revision;
                        run.state = RunState::Blocked;
                        run.error = Some(error);
                        run.revision += 1;
                        composition
                            .store
                            .commit(&composition.id, revision, run)
                            .await?;
                    }
                }
            }
            Ok::<_, ForgeError>(())
        }
        .await;
        if let Err(mut error) = initialization {
            if let Err(cleanup) = composition.store.release(&composition.id).await {
                error.diagnostics.extend(cleanup.diagnostics);
            }
            return Err(error);
        }
        let observer = composition.observer.clone();
        let observer_task = tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                // Observation has its own bounded queue and deadline; it is not a commit.
                let future =
                    AssertUnwindSafe(async { observer.observe(event).await }).catch_unwind();
                let _ = tokio::time::timeout(Duration::from_millis(100), future).await;
            }
        });
        shared.phase.store(READY, Ordering::Release);
        let worker = shared.clone();
        let supervisor = tokio::spawn(async move {
            let outcome = AssertUnwindSafe(supervise(worker.clone(), late_receiver))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(ForgeError::new(
                        "runtime.failed",
                        "Essential runtime task panicked",
                    ))
                });
            if outcome.is_err() {
                worker.phase.store(FAILED, Ordering::Release);
                worker.cancel.cancel();
                worker.changed.notify_waiters();
            }
            outcome
        });
        Ok(Self {
            app: WorkflowApplication { shared },
            supervisor: Some(supervisor),
            observer: Some(observer_task),
        })
    }

    pub fn application(&self) -> WorkflowApplication {
        self.app.clone()
    }
    pub fn is_ready(&self) -> bool {
        self.app.is_ready()
    }

    pub async fn shutdown(
        mut self,
        options: ShutdownOptions,
    ) -> Result<ShutdownReport, ForgeError> {
        self.app.close_admission().await;
        let mut supervisor = self.supervisor.take().expect("owned supervisor");
        let mut failures = Vec::new();
        let mut record_failure =
            |result: Result<Result<(), ForgeError>, tokio::task::JoinError>| {
                if let Err(error) = result.map_err(|_| unavailable()).and_then(|r| r) {
                    failures.extend(error.diagnostics);
                }
            };
        let forced = match tokio::time::timeout(options.timeout, &mut supervisor).await {
            Ok(outcome) => {
                record_failure(outcome);
                false
            }
            Err(_) => {
                self.app.shared.cancel.cancel();
                match tokio::time::timeout(Duration::from_secs(1), &mut supervisor).await {
                    Ok(outcome) => record_failure(outcome),
                    Err(_) => {
                        supervisor.abort();
                        let _ = supervisor.await;
                    }
                }
                true
            }
        };
        let composition = &self.app.shared.composition;
        let pending = match composition.store.unfinished().await {
            Ok(runs) => runs.into_iter().map(|r| r.id).collect(),
            Err(error) => {
                failures.extend(error.diagnostics);
                Vec::new()
            }
        };
        if let Err(error) = composition.store.release(&composition.id).await {
            failures.extend(error.diagnostics);
        }
        self.app.shared.phase.store(STOPPED, Ordering::Release);
        if let Some(observer) = self.observer.take() {
            observer.abort();
            let _ = observer.await;
        }
        if failures.is_empty() {
            Ok(ShutdownReport { forced, pending })
        } else {
            Err(ForgeError {
                diagnostics: failures,
            })
        }
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        self.app.shared.phase.store(STOPPED, Ordering::Release);
        self.app.shared.cancel.cancel();
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.abort();
        }
        if let Some(observer) = self.observer.take() {
            observer.abort();
        }
    }
}
