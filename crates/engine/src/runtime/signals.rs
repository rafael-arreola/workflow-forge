use super::*;

impl WorkflowApplication {
    pub async fn signal(
        &self,
        access: AccessContext,
        mut command: SignalCommand,
    ) -> Result<SignalReceipt, ForgeError> {
        self.authorize(&access, Permission::Signal, false)?;
        let c = &self.shared.composition;
        if [&command.wait_id, &command.message_id, &command.correlation]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256)
        {
            return Err(ForgeError::new(
                "data.invalid",
                "Signal identities must contain 1 to 256 bytes",
            ));
        }
        crate::schema::check_value(
            &command.payload,
            c.limits.signal_bytes.min(c.limits.value_bytes),
            c.limits.json_depth,
        )?;
        command.artifacts.sort_by(|a, b| a.id.cmp(&b.id));
        if command.artifacts.len() > 1000
            || command.artifacts.windows(2).any(|p| p[0].id == p[1].id)
            || state::json_bytes(&command.artifacts) > c.limits.value_bytes
        {
            return Err(ForgeError::new(
                "resource.limit",
                "Signal artifact declarations exceed their budget",
            ));
        }
        if !command.artifacts.is_empty() {
            if !access.permits_resource("artifacts")
                || command.artifacts.iter().any(|a| a.scope != access.scope)
            {
                return Err(ForgeError::new(
                    "access.denied",
                    "Signal artifacts are outside the caller's grants",
                ));
            }
            if c.store.capabilities().durable && !c.coordinated_artifacts() {
                return Err(ForgeError::new(
                    "capability.unsupported",
                    "Signal artifacts require coordinated retention",
                ));
            }
        }
        loop {
            self.authorize(&access, Permission::Signal, false)?;
            let mut run = state::read(&self.shared, &command.run_id).await?;
            let wait = run.waits.get(&command.wait_id).ok_or_else(|| {
                ForgeError::new("wait.not_found", "Signal reservation is unavailable")
            })?;
            let WaitKind::Signal {
                correlation,
                payload_schema,
                ..
            } = &wait.kind
            else {
                return Err(ForgeError::new(
                    "wait.not_found",
                    "Target is not a signal reservation",
                ));
            };
            if let Some(delivery) = &wait.delivery {
                if delivery.command != command || delivery.actor != access.actor {
                    return Err(ForgeError::new(
                        "state.conflict",
                        "Reservation already accepted a different signal",
                    ));
                }
                let mut receipt = delivery.receipt.clone();
                receipt.duplicate = true;
                return Ok(receipt);
            }
            if correlation != &command.correlation {
                return Err(ForgeError::new(
                    "data.invalid",
                    "Signal correlation does not match",
                ));
            }
            if wait.state == WaitState::Expired {
                return Err(ForgeError::new(
                    "wait.expired",
                    "Signal reservation reached its deadline",
                ));
            }
            if run.state.is_terminal() || run.cancel_requested || wait.state == WaitState::Closed {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Run no longer admits signals",
                ));
            }
            compiler::recover_schema(&run.package, payload_schema, &c.limits)?
                .validate(&command.payload, &c.limits)?;
            let now = now_ms();
            if wait.state == WaitState::Expired || now >= wait.deadline_at_ms {
                return Err(ForgeError::new(
                    "wait.expired",
                    "Signal reservation reached its deadline",
                ));
            }
            let receipt = SignalReceipt {
                run_id: command.run_id.clone(),
                wait_id: command.wait_id.clone(),
                message_id: command.message_id.clone(),
                accepted_at_ms: now,
                durable: c.store.capabilities().durable,
                duplicate: false,
            };
            let expected = run.revision;
            run.waits
                .get_mut(&command.wait_id)
                .expect("checked reservation")
                .delivery = Some(SignalDelivery {
                command: command.clone(),
                actor: access.actor.clone(),
                receipt: receipt.clone(),
            });
            // Blocked remains blocked: a callback never resolves an uncertain start.
            if run.state == RunState::Waiting {
                run.state = RunState::Running;
            }
            if run.retained_data_bytes() > c.limits.run_bytes {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Signal exceeds the run retention budget",
                ));
            }
            run.revision = expected
                .checked_add(1)
                .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?;
            match c.store.commit(&c.id, expected, run.clone()).await {
                Ok(()) => {
                    state::publish(&self.shared, &run);
                    self.shared.wake.notify_one();
                    return Ok(receipt);
                }
                Err(error) if error.code() == "state.conflict" => {
                    if state::view(&self.shared, &command.run_id, None)
                        .await?
                        .head
                        .revision
                        == expected
                    {
                        return Err(error);
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}
