use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::WorkflowError;
use crate::profile::{ProfileTask, TaskProfile};
use crate::secret::SecretProvider;
use crate::task::{Task, TaskId, TaskManifest};
use crate::workflow::WorkflowDefinition;

/// Registro global de tareas disponibles para ejecución.
/// Las tareas se registran por su `task_id()` y se resuelven en runtime
/// cuando el executor encuentra un nodo de tipo `Task`.
///
/// Thread-safe: usa `Arc<RwLock<>>` para permitir lectura concurrente
/// y registro puntual desde múltiples hilos.
#[derive(Default)]
pub struct TaskRegistry {
    tasks: Arc<RwLock<HashMap<TaskId, Arc<dyn Task>>>>,
}

impl TaskRegistry {
    /// Crea un registro vacío
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registra una tarea. Si ya existe una con el mismo tipo, la sobrescribe.
    pub fn register<T>(&self, task: T)
    where
        T: Task,
    {
        self.register_arc(Arc::new(task));
    }

    /// Registra una tarea ya envuelta en `Arc` (útil para compartir instancias
    /// entre registries). Si ya existe una con el mismo id, la sobrescribe.
    pub fn register_arc(&self, task: Arc<dyn Task>) {
        let key = task.task_id().clone();
        let mut map = self.tasks.write().expect("TaskRegistry lock poisoned");
        map.insert(key, task);
    }

    /// Registra un perfil de tarea: una instancia nombrada y reusable de una
    /// tarea base ya registrada, con config horneada (`bind`) y schemas
    /// propios. Los `{"$secret": "X"}` del bind se resuelven aquí con el
    /// provider dado.
    ///
    /// Errores: `PROFILE_BASE_NOT_FOUND` si `extends` no está registrado,
    /// `PROFILE_ID_CONFLICT` si el id ya existe, `SECRET_NOT_FOUND` si un
    /// secreto no resuelve.
    pub fn register_profile(
        &self,
        profile: TaskProfile,
        secrets: &dyn SecretProvider,
    ) -> Result<(), WorkflowError> {
        if self.contains(&profile.id) {
            return Err(WorkflowError::new(
                "PROFILE_ID_CONFLICT",
                format!(
                    "No se puede registrar el perfil '{}': ya existe una tarea con ese id",
                    profile.id
                ),
            ));
        }
        let base = self.get(&profile.extends).ok_or_else(|| {
            WorkflowError::new(
                "PROFILE_BASE_NOT_FOUND",
                format!(
                    "El perfil '{}' extiende '{}', que no está registrada",
                    profile.id, profile.extends
                ),
            )
        })?;
        let task = ProfileTask::new(profile, base, secrets)?;
        self.register(task);
        Ok(())
    }

    /// Copia superficial e independiente del registry: las tareas (Arc) se
    /// comparten, pero los registros posteriores no afectan al original.
    /// Es la base de los perfiles inline por workflow.
    pub fn scoped(&self) -> TaskRegistry {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        TaskRegistry {
            tasks: Arc::new(RwLock::new(map.clone())),
        }
    }

    /// Obtiene una tarea por su tipo. Devuelve `None` si no está registrada.
    pub fn get(&self, task_id: &TaskId) -> Option<Arc<dyn Task>> {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.get(task_id).cloned()
    }

    /// Lista todos los tipos de tarea registrados
    pub fn list(&self) -> Vec<TaskId> {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.keys().cloned().collect()
    }

    /// Exporta el catálogo de manifiestos de todas las tareas registradas.
    /// Serializable a JSON: es la base de tooling, documentación y editores.
    pub fn catalog(&self) -> Vec<TaskManifest> {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        let mut catalog: Vec<TaskManifest> =
            map.values().map(|task| task.manifest().clone()).collect();
        catalog.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        catalog
    }

    /// Verifica si un tipo de tarea está registrado
    pub fn contains(&self, task_id: &TaskId) -> bool {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.contains_key(task_id)
    }
}

impl Clone for TaskRegistry {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
        }
    }
}

/// Registro de workflows reusables como sub-workflows, indexados por nombre.
/// Es el complemento compartido de la sección `workflows` inline de un
/// documento (la sección inline tiene precedencia al resolver).
///
/// Thread-safe, igual que [`TaskRegistry`].
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
                "WORKFLOW_NAME_CONFLICT",
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
