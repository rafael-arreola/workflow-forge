use super::*;

struct BodyRef<'a> {
    entry: &'a str,
    nodes: &'a [NodeDefinition],
    edges: &'a [Edge],
    output: &'a Binding,
}

pub(super) struct Compiler<'a> {
    composition: &'a Composition,
    active: BTreeSet<WorkflowRevision>,
    nodes: usize,
    bytes: usize,
}
impl<'a> Compiler<'a> {
    pub fn new(composition: &'a Composition) -> Self {
        Self {
            composition,
            active: BTreeSet::new(),
            nodes: 0,
            bytes: 0,
        }
    }
    pub fn workflow(
        &mut self,
        definition: WorkflowDefinition,
        depth: usize,
    ) -> Result<PreparedWorkflow, ForgeError> {
        let limits = &self.composition.limits;
        self.depth(depth)?;
        check_value(
            &serde_json::to_value(&definition).expect("definition serializes"),
            limits.document_bytes,
            limits.json_depth,
        )?;
        if definition.format != WORKFLOW_FORMAT
            || definition.schema_dialect != SCHEMA_DIALECT
            || definition.id.is_empty()
            || definition.revision.is_empty()
            || definition.revision == "latest"
            || definition.revision.contains('*')
        {
            return Err(ForgeError::new(
                "definition.invalid",
                "Workflow format, dialect and exact revision must be explicit",
            ));
        }
        let revision = WorkflowRevision {
            id: definition.id.clone(),
            revision: definition.revision.clone(),
        };
        if self
            .composition
            .workflows
            .get(&revision)
            .is_some_and(|stored| stored.semantic_value() != definition.semantic_value())
        {
            return Err(ForgeError::new(
                "state.conflict",
                "Workflow revision differs from the frozen catalog",
            ));
        }
        if !self.active.insert(revision.clone()) {
            return Err(ForgeError::new(
                "definition.invalid",
                "Subworkflow dependency cycle",
            ));
        }
        // Account for schemas and repeated referenced definitions as well as IR nodes.
        self.bytes = self.bytes.saturating_add(
            serde_json::to_vec(&definition)
                .expect("definition serializes")
                .len(),
        );
        let result = (|| {
            if self.bytes > limits.plan_bytes {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Expanded plan exceeds its byte budget",
                ));
            }
            let input = CompiledSchema::compile(
                &definition.input_schema,
                &self.composition.schemas,
                limits,
            )
            .map_err(|e| located(e, None, "/input_schema"))?;
            let output = CompiledSchema::compile(
                &definition.output_schema,
                &self.composition.schemas,
                limits,
            )
            .map_err(|e| located(e, None, "/output_schema"))?;
            let mut metadata = Metadata::default();
            let mut document_nodes = 0;
            let body = self.body(
                BodyRef {
                    entry: &definition.entry,
                    nodes: &definition.nodes,
                    edges: &definition.edges,
                    output: &definition.output,
                },
                depth,
                &mut document_nodes,
                &mut metadata,
            )?;
            metadata
                .definitions
                .insert(revision.clone(), definition.clone());
            if self.composition.store.capabilities().durable
                && metadata.resources.contains("artifacts")
                && !self.composition.coordinated_artifacts()
            {
                return Err(ForgeError::new(
                    "capability.unsupported",
                    "A durable workflow requires artifacts in its store coordination domain",
                ));
            }
            let package = ResolvedPackage {
                definitions: metadata.definitions.values().cloned().collect(),
                operations: metadata.operations.values().cloned().collect(),
                schemas: self
                    .composition
                    .schema_resources
                    .values()
                    .cloned()
                    .collect(),
            };
            if self.bytes.saturating_add(
                serde_json::to_vec(&package)
                    .expect("package serializes")
                    .len(),
            ) > limits.plan_bytes
            {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Resolved package exceeds the plan budget",
                ));
            }
            Ok(PreparedWorkflow(Arc::new(Plan {
                composition: self.composition.id.clone(),
                definition,
                body,
                input,
                output,
                warnings: metadata.warnings,
                resources: metadata.resources,
                definitions: metadata.definitions,
                package,
            })))
        })();
        self.active.remove(&revision);
        result
    }
    fn depth(&self, depth: usize) -> Result<(), ForgeError> {
        if depth > self.composition.limits.control_depth {
            Err(ForgeError::new(
                "resource.limit",
                "Control nesting exceeds the host limit",
            ))
        } else {
            Ok(())
        }
    }
    fn inline(
        &mut self,
        body: &BodyDefinition,
        depth: usize,
        document_nodes: &mut usize,
        metadata: &mut Metadata,
    ) -> Result<Arc<PreparedBody>, ForgeError> {
        self.body(
            BodyRef {
                entry: &body.entry,
                nodes: &body.nodes,
                edges: &body.edges,
                output: &body.output,
            },
            depth,
            document_nodes,
            metadata,
        )
    }
    fn body(
        &mut self,
        body: BodyRef<'_>,
        depth: usize,
        document_nodes: &mut usize,
        metadata: &mut Metadata,
    ) -> Result<Arc<PreparedBody>, ForgeError> {
        self.depth(depth)?;
        let BodyRef {
            entry,
            nodes,
            edges,
            output,
        } = body;
        let limits = &self.composition.limits;
        *document_nodes = document_nodes.saturating_add(nodes.len());
        self.nodes = self.nodes.saturating_add(nodes.len());
        if *document_nodes > limits.nodes || self.nodes > limits.activations {
            return Err(ForgeError::new(
                "resource.limit",
                "Prepared graph exceeds its node budget",
            ));
        }
        let order = ordered(entry, nodes, edges)?;
        let mut available = BTreeSet::new();
        let mut sequence = Vec::new();
        let mut diagnostics = Vec::new();
        for node in order {
            self.bytes = self
                .bytes
                .saturating_add(serde_json::to_vec(node).expect("node serializes").len());
            if self.bytes > limits.plan_bytes {
                return Err(ForgeError::new(
                    "resource.limit",
                    "Expanded plan exceeds its byte budget",
                ));
            }
            if let Err(error) = binding::check(&node.input, &available, limits) {
                diagnostics.extend(located(error, Some(&node.id), "/input").diagnostics);
            }
            match self.instruction(node, depth, document_nodes, metadata) {
                Ok(instruction) => sequence.push(PreparedNode {
                    id: node.id.clone(),
                    input: node.input.clone(),
                    instruction,
                }),
                Err(error) => {
                    diagnostics.extend(located(error, Some(&node.id), "/instruction").diagnostics)
                }
            }
            available.insert(node.id.clone());
        }
        if let Err(error) = binding::check(output, &available, limits) {
            diagnostics.extend(located(error, None, "/output").diagnostics);
        }
        if !diagnostics.is_empty() {
            return Err(ForgeError { diagnostics });
        }
        Ok(Arc::new(PreparedBody {
            sequence,
            output: output.clone(),
        }))
    }
    fn instruction(
        &mut self,
        node: &NodeDefinition,
        depth: usize,
        document_nodes: &mut usize,
        metadata: &mut Metadata,
    ) -> Result<PreparedInstruction, ForgeError> {
        let limits = &self.composition.limits;
        match &node.instruction {
            Instruction::Try {
                body,
                catches,
                fallback,
            } => {
                if catches.len().saturating_add(1) > limits.group_branches || fallback.id.is_empty()
                {
                    return Err(ForgeError::new(
                        "definition.invalid",
                        "Error handlers require a bounded set and a named fallback",
                    ));
                }
                let mut ids = BTreeSet::from([fallback.id.clone()]);
                let mut codes = BTreeSet::new();
                let body = self.inline(body, depth + 1, document_nodes, metadata)?;
                let mut compiled = Vec::new();
                for case in catches {
                    if case.id.is_empty()
                        || case.code.is_empty()
                        || !ids.insert(case.id.clone())
                        || !codes.insert(case.code.clone())
                    {
                        return Err(ForgeError::new(
                            "definition.invalid",
                            "Error handler identities and codes must be nonempty and unique",
                        ));
                    }
                    compiled.push((
                        case.id.clone(),
                        case.code.clone(),
                        self.inline(&case.body, depth + 1, document_nodes, metadata)?,
                    ));
                }
                Ok(PreparedInstruction::Try {
                    body,
                    catches: compiled,
                    fallback: (
                        fallback.id.clone(),
                        self.inline(&fallback.body, depth + 1, document_nodes, metadata)?,
                    ),
                })
            }
            Instruction::Timer { duration_ms } => {
                if *duration_ms > limits.wait_timeout_ms {
                    return Err(ForgeError::new(
                        "resource.limit",
                        "Timer exceeds the wait deadline budget",
                    ));
                }
                Ok(PreparedInstruction::Timer {
                    duration_ms: *duration_ms,
                })
            }
            Instruction::AwaitSignal {
                correlation,
                timeout_ms,
                payload_schema,
                start,
            } => {
                context_binding(correlation, limits, false)?;
                if let Binding::Literal(value) = correlation
                    && value.as_str().is_none_or(|s| s.is_empty() || s.len() > 256)
                {
                    return Err(ForgeError::new(
                        "data.invalid",
                        "Signal correlation must contain 1 to 256 bytes",
                    ));
                }
                if *timeout_ms == 0 || *timeout_ms > limits.wait_timeout_ms {
                    return Err(ForgeError::new(
                        "resource.limit",
                        "Signal deadline exceeds the wait budget",
                    ));
                }
                CompiledSchema::compile(payload_schema, &self.composition.schemas, limits)?;
                let start = if let Some(start) = start {
                    context_binding(&start.input, limits, false)?;
                    self.nodes = self.nodes.saturating_add(1);
                    *document_nodes = document_nodes.saturating_add(1);
                    if *document_nodes > limits.nodes || self.nodes > limits.activations {
                        return Err(ForgeError::new(
                            "resource.limit",
                            "Signal start exceeds the plan node budget",
                        ));
                    }
                    let synthetic = NodeDefinition {
                        id: node.id.clone(),
                        input: start.input.clone(),
                        instruction: Instruction::Operation {
                            operation: start.operation.clone(),
                            config: start.config.clone(),
                            retry: start.retry.clone(),
                        },
                    };
                    let PreparedInstruction::Operation(operation) = self
                        .instruction(&synthetic, depth, document_nodes, metadata)
                        .map_err(|mut error| {
                            for diagnostic in &mut error.diagnostics {
                                diagnostic.location.field =
                                    format!("/start{}", diagnostic.location.field);
                            }
                            error
                        })?
                    else {
                        unreachable!()
                    };
                    Some((start.input.clone(), operation))
                } else {
                    None
                };
                Ok(PreparedInstruction::AwaitSignal {
                    correlation: correlation.clone(),
                    timeout_ms: *timeout_ms,
                    payload_schema: payload_schema.clone(),
                    start,
                })
            }
            Instruction::Operation {
                operation: revision,
                config,
                retry,
            } => {
                let operation = self
                    .composition
                    .operations
                    .get(revision)
                    .cloned()
                    .ok_or_else(|| {
                        located(
                            ForgeError::new(
                                "reference.missing",
                                "Operation revision is unavailable",
                            ),
                            Some(&node.id),
                            "/operation",
                        )
                    })?;
                retry
                    .validate(limits.max_retry_attempts)
                    .map_err(|e| located(e, Some(&node.id), "/retry"))?;
                operation
                    .config
                    .validate(config, limits)
                    .map_err(|e| located(e, Some(&node.id), "/config"))?;
                if let Binding::Literal(value) = &node.input {
                    operation
                        .input
                        .validate(value, limits)
                        .map_err(|e| located(e, Some(&node.id), "/input/literal"))?;
                } else {
                    metadata.warnings.push(
                        Diagnostic::new(
                            "compatibility.unknown",
                            "Data compatibility will be validated at invocation",
                        )
                        .at("prepare", Some(&node.id), "/input"),
                    );
                }
                metadata
                    .resources
                    .extend(operation.descriptor.required_resources.iter().cloned());
                metadata
                    .operations
                    .insert(revision.clone(), operation.descriptor.clone());
                Ok(PreparedInstruction::Operation(PreparedOperation {
                    operation,
                    config: config.clone(),
                    retry: retry.clone(),
                }))
            }
            Instruction::Decision { cases, fallback } => {
                if cases.len() + usize::from(fallback.is_some()) > limits.group_branches
                    || (cases.is_empty() && fallback.is_none())
                {
                    return Err(ForgeError::new(
                        "definition.invalid",
                        "Decision needs a bounded set of alternatives",
                    ));
                }
                let mut ids = BTreeSet::new();
                let mut compiled = Vec::new();
                for case in cases {
                    if case.id.is_empty() || !ids.insert(case.id.clone()) {
                        return Err(ForgeError::new(
                            "definition.invalid",
                            "Decision alternative identities must be unique",
                        ));
                    }
                    context_binding(&case.when, limits, true)?;
                    compiled.push(PreparedCase {
                        id: case.id.clone(),
                        when: case.when.clone(),
                        body: self.inline(&case.body, depth + 1, document_nodes, metadata)?,
                    });
                }
                let fallback = match fallback {
                    Some(case) => {
                        if case.id.is_empty() || !ids.insert(case.id.clone()) {
                            return Err(ForgeError::new(
                                "definition.invalid",
                                "Decision fallback identity conflicts",
                            ));
                        }
                        Some((
                            case.id.clone(),
                            self.inline(&case.body, depth + 1, document_nodes, metadata)?,
                        ))
                    }
                    None => None,
                };
                Ok(PreparedInstruction::Decision {
                    cases: compiled,
                    fallback,
                })
            }
            Instruction::Parallel {
                branches,
                concurrency,
                errors,
                join: JoinPolicy::All,
            } => {
                if branches.is_empty()
                    || branches.len() > limits.group_branches
                    || branches.keys().any(String::is_empty)
                {
                    return Err(ForgeError::new(
                        "definition.invalid",
                        "Parallel group has invalid branches",
                    ));
                }
                let concurrency = concurrency_limit(*concurrency, limits)?;
                let mut compiled = BTreeMap::new();
                for (id, body) in branches {
                    compiled.insert(
                        id.clone(),
                        self.inline(body, depth + 1, document_nodes, metadata)?,
                    );
                }
                Ok(PreparedInstruction::Parallel {
                    branches: compiled,
                    concurrency,
                    errors: *errors,
                })
            }
            Instruction::Foreach {
                items,
                body,
                concurrency,
                errors,
            } => {
                context_binding(items, limits, false)?;
                if let Binding::Literal(value) = items
                    && (!value.is_array()
                        || value
                            .as_array()
                            .is_some_and(|a| a.len() > limits.foreach_items))
                {
                    return Err(ForgeError::new(
                        "data.invalid",
                        "Foreach items must be a bounded array",
                    ));
                }
                let concurrency = concurrency_limit(*concurrency, limits)?;
                Ok(PreparedInstruction::Foreach {
                    items: items.clone(),
                    body: self.inline(body, depth + 1, document_nodes, metadata)?,
                    concurrency,
                    errors: *errors,
                })
            }
            Instruction::Loop {
                r#while,
                body,
                max_iterations,
                on_limit,
            } => {
                context_binding(r#while, limits, true)?;
                if *max_iterations == 0 || *max_iterations > limits.loop_iterations {
                    return Err(ForgeError::new(
                        "resource.limit",
                        "Loop iteration budget is invalid",
                    ));
                }
                Ok(PreparedInstruction::Loop {
                    condition: r#while.clone(),
                    body: self.inline(body, depth + 1, document_nodes, metadata)?,
                    max_iterations: *max_iterations,
                    on_limit: *on_limit,
                })
            }
            Instruction::Subworkflow { workflow } => {
                let definition = self
                    .composition
                    .workflows
                    .get(workflow)
                    .cloned()
                    .ok_or_else(|| {
                        ForgeError::new("reference.missing", "Subworkflow revision is unavailable")
                    })?;
                let child = self.workflow(definition, depth + 1)?;
                if let Binding::Literal(value) = &node.input {
                    child.0.input.validate(value, limits)?;
                }
                metadata.resources.extend(child.0.resources.iter().cloned());
                metadata.warnings.extend(child.0.warnings.iter().cloned());
                metadata.definitions.extend(child.0.definitions.clone());
                metadata.operations.extend(
                    child
                        .0
                        .package
                        .operations
                        .iter()
                        .map(|d| (d.revision.clone(), d.clone())),
                );
                Ok(PreparedInstruction::Subworkflow(child))
            }
        }
    }
}
#[derive(Default)]
struct Metadata {
    warnings: Vec<Diagnostic>,
    resources: BTreeSet<String>,
    definitions: BTreeMap<WorkflowRevision, WorkflowDefinition>,
    operations: BTreeMap<OperationRevision, OperationDescriptor>,
}
fn context_binding(binding: &Binding, limits: &Limits, boolean: bool) -> Result<(), ForgeError> {
    binding::check(binding, &BTreeSet::new(), limits)?;
    if boolean && matches!(binding,Binding::Literal(v) if !v.is_boolean()) {
        return Err(ForgeError::new(
            "data.invalid",
            "Control condition must be boolean",
        ));
    }
    Ok(())
}
fn concurrency_limit(requested: usize, limits: &Limits) -> Result<usize, ForgeError> {
    if requested == 0 {
        Err(ForgeError::new(
            "definition.invalid",
            "Group concurrency must be positive",
        ))
    } else {
        Ok(requested.min(limits.group_concurrency))
    }
}
fn ordered<'a>(
    entry: &str,
    nodes: &'a [NodeDefinition],
    edges: &[Edge],
) -> Result<Vec<&'a NodeDefinition>, ForgeError> {
    if nodes.is_empty() {
        return Err(ForgeError::new(
            "definition.invalid",
            "A body must contain at least one node",
        ));
    }
    let mut by_id = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for node in nodes {
        if node.id.is_empty() || by_id.insert(node.id.clone(), node).is_some() {
            diagnostics.push(
                Diagnostic::new(
                    "definition.invalid",
                    "Node identities must be nonempty and unique",
                )
                .at("prepare", Some(&node.id), "/nodes"),
            );
        }
    }
    let mut next = BTreeMap::new();
    let mut previous = BTreeSet::new();
    let mut unique = BTreeSet::new();
    for edge in edges {
        if !by_id.contains_key(&edge.from) || !by_id.contains_key(&edge.to) {
            diagnostics.push(Diagnostic::new(
                "reference.missing",
                "Edge references an unknown local node",
            ));
            continue;
        }
        if !unique.insert(edge.clone()) {
            diagnostics.push(Diagnostic::new("definition.invalid", "Duplicate edge"));
        }
        if next.insert(edge.from.clone(), edge.to.clone()).is_some()
            || !previous.insert(edge.to.clone())
        {
            diagnostics.push(Diagnostic::new(
                "capability.unsupported",
                "Use structured control instructions for forks and joins",
            ));
        }
    }
    if !by_id.contains_key(entry) || previous.contains(entry) {
        diagnostics.push(Diagnostic::new(
            "definition.invalid",
            "Entry must exist and have no predecessor",
        ));
    }
    if !diagnostics.is_empty() {
        return Err(ForgeError { diagnostics });
    }
    let mut order = Vec::new();
    let mut visited = BTreeSet::new();
    let mut cursor = Some(entry.to_owned());
    while let Some(id) = cursor {
        if !visited.insert(id.clone()) {
            return Err(ForgeError::new(
                "definition.invalid",
                "Control cycles are not supported",
            ));
        }
        order.push(by_id[&id]);
        cursor = next.get(&id).cloned();
    }
    if visited.len() != nodes.len() {
        return Err(ForgeError::new(
            "definition.invalid",
            "All nodes must be reachable from entry",
        ));
    }
    Ok(order)
}
