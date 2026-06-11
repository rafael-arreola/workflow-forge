//! Registro de workflows reusables como sub-workflows, por nombre.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{WorkflowError, codes};
use crate::spec::workflow::WorkflowDefinition;

/// Registro de workflows reusables como sub-workflows, indexados por nombre.
/// Es el complemento compartido de la sección `workflows` inline de un
/// documento (la sección inline tiene precedencia al resolver).
///
/// Thread-safe, igual que [`crate::task::TaskRegistry`].
#[derive(Default)]
pub struct WorkflowRegistry {
    workflows: RwLock<HashMap<String, Arc<WorkflowDefinition>>>,
}

impl WorkflowRegistry {
    /// Crea un registro vacío
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra un workflow por su `name`. Error `WORKFLOW_NAME_CONFLICT`
    /// si ya existe uno con ese nombre.
    pub fn register(&self, workflow: WorkflowDefinition) -> Result<(), WorkflowError> {
        let mut map = self
            .workflows
            .write()
            .expect("WorkflowRegistry lock poisoned");
        if map.contains_key(&workflow.name) {
            return Err(WorkflowError::new(
                codes::WORKFLOW_NAME_CONFLICT,
                format!(
                    "No se puede registrar el workflow '{}': ya existe uno con ese nombre",
                    workflow.name
                ),
            ));
        }
        map.insert(workflow.name.clone(), Arc::new(workflow));
        Ok(())
    }

    /// Obtiene un workflow por nombre
    pub fn get(&self, name: &str) -> Option<Arc<WorkflowDefinition>> {
        let map = self
            .workflows
            .read()
            .expect("WorkflowRegistry lock poisoned");
        map.get(name).cloned()
    }

    /// Verifica si un workflow está registrado
    pub fn contains(&self, name: &str) -> bool {
        let map = self
            .workflows
            .read()
            .expect("WorkflowRegistry lock poisoned");
        map.contains_key(name)
    }

    /// Nombres de todos los workflows registrados
    pub fn list(&self) -> Vec<String> {
        let map = self
            .workflows
            .read()
            .expect("WorkflowRegistry lock poisoned");
        map.keys().cloned().collect()
    }
}
