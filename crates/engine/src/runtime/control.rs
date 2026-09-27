use super::*;
use crate::compiler::{PreparedBody, PreparedInstruction, PreparedNode};
use futures::future::BoxFuture;
use steps::{StepError, expected};

/// Tags and escaped identifiers are separate path segments, never raw concatenated IDs.
#[derive(Clone, Default)]
pub(super) struct ScopePath(Vec<String>);
impl ScopePath {
    pub fn node(&self, id: &str) -> String {
        self.child(id, &[]).key()
    }
    pub fn child(&self, node: &str, segments: &[&str]) -> Self {
        let mut parts = self.0.clone();
        parts.extend(["nodes".into(), node.into()]);
        parts.extend(segments.iter().map(|s| (*s).into()));
        Self(parts)
    }
    fn key(&self) -> String {
        let parts: Vec<_> = self
            .0
            .iter()
            .map(|s| s.replace('~', "~0").replace('/', "~1"))
            .collect();
        format!("/{}", parts.join("/"))
    }
}
#[derive(Clone)]
pub(super) struct ScopeExecution {
    pub shared: Arc<Shared>,
    pub run_id: RunId,
    pub path: ScopePath,
    pub input: Value,
    pub cancellation: CancellationToken,
}
impl ScopeExecution {
    pub fn child(
        &self,
        node: &str,
        segments: &[&str],
        input: Value,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            shared: self.shared.clone(),
            run_id: self.run_id.clone(),
            path: self.path.child(node, segments),
            input,
            cancellation,
        }
    }
}

pub(super) fn execute_body(
    scope: ScopeExecution,
    body: Arc<PreparedBody>,
) -> BoxFuture<'static, Result<Value, StepError>> {
    Box::pin(async move {
        let limits = &scope.shared.composition.limits;
        let mut outputs = BTreeMap::new();
        for node in &body.sequence {
            let key = scope.path.node(&node.id);
            let view = state::view(&scope.shared, &scope.run_id, Some(&key)).await?;
            let run = &view.head;
            let record = view.invocation.as_ref();
            let action = planner::node(
                record,
                run.cancel_requested || scope.cancellation.is_cancelled(),
                now_ms() >= run.deadline_at_ms,
            )?;
            if let planner::NodeAction::Confirmed(value) = action {
                outputs.insert(node.id.clone(), value);
                continue;
            }
            let input = if let Some(record) = record {
                record.input.clone()
            } else {
                binding::evaluate(&node.input, &scope.input, &outputs, limits).map_err(
                    |mut error| {
                        locate(&mut error, &key, "/input");
                        expected(error)
                    },
                )?
            };
            let value =
                match &node.instruction {
                    PreparedInstruction::Operation(operation) => {
                        operation.operation.input.validate(&input, limits).map_err(
                            |mut error| {
                                locate(&mut error, &key, "/input");
                                expected(error)
                            },
                        )?;
                        steps::execute_operation(
                            &scope.shared,
                            &scope.run_id,
                            &key,
                            operation,
                            input,
                            &scope.cancellation,
                        )
                        .await?
                    }
                    _ => execute_control(&scope, node, &key, input).await?,
                };
            outputs.insert(node.id.clone(), value);
        }
        binding::evaluate(&body.output, &scope.input, &outputs, limits).map_err(|mut error| {
            error
                .diagnostics
                .iter_mut()
                .for_each(|d| d.location.field = "/output".into());
            expected(error)
        })
    })
}
fn locate(error: &mut ForgeError, node: &str, field: &str) {
    for d in &mut error.diagnostics {
        d.location.node = Some(node.into());
        d.location.field = field.into();
    }
}

async fn execute_control(
    scope: &ScopeExecution,
    node: &PreparedNode,
    key: &str,
    input: Value,
) -> Result<Value, StepError> {
    if matches!(
        node.instruction,
        PreparedInstruction::Timer { .. } | PreparedInstruction::AwaitSignal { .. }
    ) {
        return waits::execute(scope, node, key, input).await;
    }
    let limits = &scope.shared.composition.limits;
    let existing = state::view(&scope.shared, &scope.run_id, Some(key))
        .await?
        .invocation;
    let frame = match existing.as_ref().and_then(|r| r.control.clone()) {
        Some(frame) => frame,
        None => initial_frame(&node.instruction, &input, limits).map_err(expected)?,
    };
    state::update_node(&scope.shared, &scope.run_id, key, true, |_, current| {
        if current.is_some() {
            return Ok(false);
        }
        *current = Some(InvocationRecord {
            id: uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                &serde_json::to_vec(&(&scope.run_id, key)).expect("identity serializes"),
            )
            .to_string(),
            attempt_id: String::new(),
            attempts: 0,
            state: InvocationState::Running,
            input: input.clone(),
            output: None,
            error: None,
            operation: None,
            config: Value::Null,
            effect_key: None,
            retry: RetryPolicy::default(),
            next_attempt_at_ms: None,
            certainty: EffectCertainty::NotApplied,
            control: Some(frame.clone()),
        });
        Ok(true)
    })
    .await
    .map_err(steps::transition_error)?;
    let result = async {
        let value = run_control(scope, node, key, input, frame).await?;
        crate::schema::check_value(&value, limits.value_bytes, limits.json_depth)
            .map_err(expected)?;
        state::update_node(&scope.shared, &scope.run_id, key, true, |_, current| {
            let record = current.as_mut().expect("control activation committed");
            record.state = InvocationState::Succeeded;
            record.output = Some(value.clone());
            record.input = Value::Null;
            record.error = None;
            Ok(true)
        })
        .await
        .map_err(steps::transition_error)?;
        Ok(value)
    }
    .await;
    match result {
        Ok(value) => Ok(value),
        Err(StepError::Execution(mut error)) => {
            for d in &mut error.diagnostics {
                if d.location.node.is_none() {
                    d.location.node = Some(key.into());
                }
            }
            state::update_node(&scope.shared, &scope.run_id, key, false, |_, current| {
                let record = current.as_mut().expect("control activation committed");
                record.state = if error.code() == "effect.unknown" {
                    InvocationState::Pending
                } else if error.code() == "operation.cancelled" {
                    InvocationState::Cancelled
                } else {
                    InvocationState::Failed
                };
                record.error = Some(error.clone());
                Ok(true)
            })
            .await?;
            Err(expected(error))
        }
        Err(error) => Err(error),
    }
}

fn initial_frame(
    instruction: &PreparedInstruction,
    input: &Value,
    limits: &Limits,
) -> Result<ControlFrame, ForgeError> {
    match instruction {
        PreparedInstruction::Try { .. } => Ok(ControlFrame::Try {
            handler: None,
            error: None,
        }),
        PreparedInstruction::Decision { cases, fallback } => {
            for case in cases {
                if planner::boolean(&case.when, input, limits)? {
                    return Ok(ControlFrame::Decision {
                        selected: case.id.clone(),
                    });
                }
            }
            fallback
                .as_ref()
                .map(|(id, _)| ControlFrame::Decision {
                    selected: id.clone(),
                })
                .ok_or_else(|| {
                    ForgeError::new(
                        "control.no_match",
                        "Decision has no matching alternative or fallback",
                    )
                })
        }
        PreparedInstruction::Parallel { branches, .. } => Ok(ControlFrame::Parallel {
            branches: branches.keys().cloned().collect(),
            next_index: 0,
            stopped: false,
            error: None,
        }),
        PreparedInstruction::Foreach { items, .. } => {
            let items = binding::evaluate(items, input, &BTreeMap::new(), limits)?;
            let items = items
                .as_array()
                .ok_or_else(|| ForgeError::new("data.invalid", "Foreach items must be an array"))?;
            if items.len() > limits.foreach_items {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Foreach item budget exceeded",
                ));
            }
            Ok(ControlFrame::Foreach {
                total: items.len(),
                next_index: 0,
                stopped: false,
                error: None,
            })
        }
        PreparedInstruction::Loop { .. } => Ok(ControlFrame::Loop {
            iteration: 0,
            state: input.clone(),
        }),
        PreparedInstruction::Subworkflow(child) => Ok(ControlFrame::Subworkflow {
            workflow: WorkflowRevision {
                id: child.definition().id.clone(),
                revision: child.definition().revision.clone(),
            },
        }),
        PreparedInstruction::Operation(_)
        | PreparedInstruction::Timer { .. }
        | PreparedInstruction::AwaitSignal { .. } => {
            unreachable!("instruction has its own dispatch path")
        }
    }
}

async fn run_control(
    scope: &ScopeExecution,
    node: &PreparedNode,
    key: &str,
    input: Value,
    frame: ControlFrame,
) -> Result<Value, StepError> {
    let limits = &scope.shared.composition.limits;
    match (&node.instruction, frame) {
        (
            PreparedInstruction::Try {
                body,
                catches,
                fallback,
            },
            ControlFrame::Try { handler, error },
        ) => {
            let (selected, error) = match (handler, error) {
                (Some(handler), Some(error)) => (handler, error),
                (None, None) => {
                    let child = scope.child(
                        &node.id,
                        &["try"],
                        input.clone(),
                        scope.cancellation.child_token(),
                    );
                    match execute_body(child, body.clone()).await {
                        Ok(output) => return Ok(json!({"outcome":"success", "output":output})),
                        Err(StepError::Execution(error)) if catchable(&error) => {
                            // Host cancellation and the run deadline always outrank workflow recovery.
                            let run = state::view(&scope.shared, &scope.run_id, None).await?.head;
                            planner::node(
                                None,
                                run.cancel_requested || scope.cancellation.is_cancelled(),
                                now_ms() >= run.deadline_at_ms,
                            )?;
                            let code = error
                                .diagnostics
                                .first()
                                .and_then(|d| d.operation_error.as_ref())
                                .map_or(error.code(), |e| e.code.as_str());
                            let selected = catches
                                .iter()
                                .find(|(_, candidate, _)| candidate == code)
                                .map_or(&fallback.0, |(id, _, _)| id)
                                .clone();
                            update_frame(
                                scope,
                                key,
                                &ControlFrame::Try {
                                    handler: Some(selected.clone()),
                                    error: Some(error.clone()),
                                },
                            )
                            .await?;
                            (selected, error)
                        }
                        Err(error) => return Err(error),
                    }
                }
                _ => {
                    return Err(ForgeError::new(
                        "state.conflict",
                        "Saved error handler is incomplete",
                    )
                    .into());
                }
            };
            let body = catches
                .iter()
                .find(|(id, _, _)| id == &selected)
                .map(|(_, _, body)| body)
                .or_else(|| (fallback.0 == selected).then_some(&fallback.1))
                .ok_or_else(|| {
                    ForgeError::new("state.conflict", "Saved error handler is unavailable")
                })?;
            let child = scope.child(
                &node.id,
                &["catch", &selected],
                json!({"input":input,"error":error}),
                scope.cancellation.child_token(),
            );
            let output = execute_body(child, body.clone()).await?;
            Ok(json!({"outcome":"handled", "handler":selected,"output":output}))
        }
        (
            PreparedInstruction::Decision { cases, fallback },
            ControlFrame::Decision { selected },
        ) => {
            let body = cases
                .iter()
                .find(|c| c.id == selected)
                .map(|c| c.body.clone())
                .or_else(|| {
                    fallback
                        .as_ref()
                        .filter(|(id, _)| id == &selected)
                        .map(|(_, body)| body.clone())
                })
                .ok_or_else(|| {
                    StepError::Infrastructure(ForgeError::new(
                        "state.conflict",
                        "Saved decision alternative is unavailable",
                    ))
                })?;
            let child = scope.child(
                &node.id,
                &["cases", &selected],
                input,
                scope.cancellation.child_token(),
            );
            let output = execute_body(child, body).await?;
            Ok(json!({"selected":selected,"output":output}))
        }
        (
            PreparedInstruction::Parallel {
                branches,
                concurrency,
                errors,
            },
            frame @ ControlFrame::Parallel { .. },
        ) => {
            let source = groups::GroupSource::Parallel {
                branches: branches
                    .iter()
                    .map(|(id, body)| (id.clone(), body.clone()))
                    .collect(),
                input,
            };
            let results = groups::execute(
                scope,
                &node.id,
                key,
                groups::GroupSpec {
                    source,
                    concurrency: *concurrency,
                    errors: *errors,
                    frame,
                },
            )
            .await?;
            Ok(Value::Object(results.into_iter().collect()))
        }
        (
            PreparedInstruction::Foreach {
                items,
                body,
                concurrency,
                errors,
            },
            frame @ ControlFrame::Foreach { .. },
        ) => {
            let values =
                binding::evaluate(items, &input, &BTreeMap::new(), limits).map_err(expected)?;
            let Value::Array(values) = values else {
                return Err(expected(ForgeError::new(
                    "data.invalid",
                    "Foreach items must be an array",
                )));
            };
            let source = groups::GroupSource::Foreach {
                items: values,
                context: input,
                body: body.clone(),
            };
            let results = groups::execute(
                scope,
                &node.id,
                key,
                groups::GroupSpec {
                    source,
                    concurrency: *concurrency,
                    errors: *errors,
                    frame,
                },
            )
            .await?;
            Ok(Value::Array(
                results.into_iter().map(|(_, value)| value).collect(),
            ))
        }
        (
            PreparedInstruction::Loop {
                condition,
                body,
                max_iterations,
                on_limit,
            },
            ControlFrame::Loop {
                mut iteration,
                mut state,
            },
        ) => loop {
            let run = state::view(&scope.shared, &scope.run_id, None).await?.head;
            planner::node(
                None,
                run.cancel_requested || scope.cancellation.is_cancelled(),
                now_ms() >= run.deadline_at_ms,
            )?;
            let input = json!({"state":state,"iteration":iteration});
            if !planner::boolean(condition, &input, limits).map_err(expected)? {
                return Ok(state);
            }
            if iteration >= *max_iterations {
                return if *on_limit == LoopLimit::ReturnLast {
                    Ok(state)
                } else {
                    Err(expected(ForgeError::new(
                        "control.iteration_limit",
                        "Loop reached its declared limit",
                    )))
                };
            }
            state = execute_body(
                scope.child(
                    &node.id,
                    &["iterations", &iteration.to_string()],
                    input,
                    scope.cancellation.child_token(),
                ),
                body.clone(),
            )
            .await?;
            iteration += 1;
            let frame = ControlFrame::Loop {
                iteration,
                state: state.clone(),
            };
            update_frame(scope, key, &frame).await?;
        },
        (PreparedInstruction::Subworkflow(child), ControlFrame::Subworkflow { .. }) => {
            child.0.input.validate(&input, limits).map_err(expected)?;
            let child_scope = scope.child(
                &node.id,
                &[
                    "workflows",
                    &child.definition().id,
                    "revisions",
                    &child.definition().revision,
                ],
                input,
                scope.cancellation.child_token(),
            );
            let output = execute_body(child_scope, child.0.body.clone()).await?;
            child.0.output.validate(&output, limits).map_err(expected)?;
            Ok(output)
        }
        _ => Err(ForgeError::new(
            "state.conflict",
            "Saved control instruction does not match the prepared plan",
        )
        .into()),
    }
}

pub(super) async fn update_frame(
    scope: &ScopeExecution,
    key: &str,
    frame: &ControlFrame,
) -> Result<(), StepError> {
    state::update_node(&scope.shared, &scope.run_id, key, true, |_, current| {
        let record = current.as_mut().ok_or_else(|| {
            ForgeError::new("state.conflict", "Control activation is unavailable")
        })?;
        record.control = Some(frame.clone());
        Ok(true)
    })
    .await
    .map_err(steps::transition_error)
    .map(|_| ())
}

// Uncertain effects and host/resource boundaries cannot become workflow success.
fn catchable(error: &ForgeError) -> bool {
    !error.diagnostics.iter().any(|d| {
        matches!(
            d.code.as_str(),
            "effect.unknown" | "operation.cancelled" | "resource.limit" | "access.denied"
        ) || d.operation_error.as_ref().is_some_and(|e| {
            e.certainty != EffectCertainty::NotApplied
                || matches!(
                    e.class,
                    ErrorClass::Cancelled | ErrorClass::Internal | ErrorClass::Resource
                )
        })
    })
}
