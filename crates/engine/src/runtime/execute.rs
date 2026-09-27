use super::*;

impl WorkflowApplication {
    /// Execute under a host cancellation scope and return the final value.
    ///
    /// Dropping this future requests cancellation. Admission and cleanup remain
    /// supervised by the engine, including a drop while the store accepts work.
    /// Receipt deduplication belongs to the explicit `start`/`wait` API: an owned
    /// call must never cancel another caller's execution.
    pub async fn execute(
        &self,
        access: AccessContext,
        request: StartRunRequest,
        cancellation: CancellationToken,
    ) -> Result<Value, ForgeError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        if token.is_cancelled() {
            return Err(cancelled());
        }
        let admission = self.shared.admission.lock().await;
        for permission in [Permission::Start, Permission::Read, Permission::Cancel] {
            self.authorize(&access, permission, true)?;
        }
        if request.options.receipt_key.is_some() {
            return Err(ForgeError::new(
                "definition.invalid",
                "Owned execution does not accept a receipt key; use start/wait for shared receipts",
            ));
        }
        let (send, receive) = tokio::sync::oneshot::channel();
        let app = self.clone();
        {
            let mut calls = self.shared.calls.lock().expect("owned calls lock");
            while let Some(result) = calls.try_join_next() {
                result.map_err(|_| unavailable())?;
            }
            let limits = &self.shared.composition.limits;
            if calls.len() >= limits.active_runs.saturating_add(limits.pending_runs) {
                return Err(ForgeError::new(
                    "admission.full",
                    "Owned execution capacity reached",
                ));
            }
            calls.spawn(async move {
                let result = app.execute_owned(access, request, token).await;
                let _ = send.send(result);
                app.shared.wake.notify_one();
            });
        }
        drop(admission);
        receive.await.map_err(|_| unavailable())?
    }

    async fn execute_owned(
        &self,
        access: AccessContext,
        request: StartRunRequest,
        token: CancellationToken,
    ) -> Result<Value, ForgeError> {
        if token.is_cancelled() || self.shared.cancel.is_cancelled() {
            return Err(cancelled());
        }
        // Do not race cancellation against the store's acceptance transaction.
        let receipt = self.start(access.clone(), request).await?;
        let id = receipt.run_id;
        let run = tokio::select! {
            biased;
            _ = token.cancelled() => {
                self.cancel(access.clone(), id.clone()).await?;
                self.wait(access, id).await?
            }
            _ = self.shared.cancel.cancelled() => return Err(unavailable()),
            result = self.wait(access.clone(), id.clone()) => result?,
        };
        if run.state == RunState::Succeeded {
            run.output
                .ok_or_else(|| ForgeError::new("state.conflict", "Completed run has no output"))
        } else {
            Err(run.error.unwrap_or_else(|| {
                ForgeError::new("execution.incomplete", "Execution has no successful result")
            }))
        }
    }
}

fn cancelled() -> ForgeError {
    ForgeError::new("operation.cancelled", "Host cancelled the execution")
}
