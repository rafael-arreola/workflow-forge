//! Registry of reusable workflows as sub-workflows, by name.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{WorkflowError, codes};
use crate::spec::workflow::WorkflowDefinition;

/// Registry of reusable workflows as sub-workflows, indexed by name.
/// It is the shared complement of a document's inline `workflows` section
/// (the inline section takes precedence when resolving).
///
/// Thread-safe, just like [`crate::task::TaskRegistry`].
#[derive(Default)]
pub struct WorkflowRegistry {
    workflows: RwLock<HashMap<String, Arc<WorkflowDefinition>>>,
}

impl WorkflowRegistry {
    /// Creates an empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a workflow by its `name`. Error `WORKFLOW_NAME_CONFLICT`
    /// if one already exists with that name.
    pub fn register(&self, workflow: WorkflowDefinition) -> Result<(), WorkflowError> {
        let mut map = self.workflows.write();
        if map.contains_key(&workflow.name) {
            return Err(WorkflowError::new(
                codes::WORKFLOW_NAME_CONFLICT,
                format!(
                    "Cannot register workflow '{}': a workflow with that name already exists",
                    workflow.name
                ),
            ));
        }
        map.insert(workflow.name.clone(), Arc::new(workflow));
        Ok(())
    }

    /// Gets a workflow by name
    pub fn get(&self, name: &str) -> Option<Arc<WorkflowDefinition>> {
        let map = self.workflows.read();
        map.get(name).cloned()
    }

    /// Checks whether a workflow is registered
    pub fn contains(&self, name: &str) -> bool {
        let map = self.workflows.read();
        map.contains_key(name)
    }

    /// Names of all registered workflows
    pub fn list(&self) -> Vec<String> {
        let map = self.workflows.read();
        map.keys().cloned().collect()
    }
}
