//! El registro de tareas: el catálogo vivo de todo lo ejecutable.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use std::future::Future;

use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{WorkflowError, codes};
use crate::io::secret::SecretProvider;
use crate::spec::profile::TaskProfile;
use crate::task::profile::ProfileTask;
use crate::task::typed::{FnTask, TaskCtx, TypedTask};
use crate::task::{Task, TaskId, TaskManifest, WorkflowData, WorkflowResult};

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
        let mut map = self.tasks.write();
        map.insert(key, task);
    }

    /// Registra una tarea **tipada** a partir de una closure async sobre tus
    /// propios tipos. Los JSON Schema de input/output se derivan de `In`/`Out`
    /// vía `schemars` y el engine los valida como con cualquier otra tarea.
    ///
    /// El atajo de baja fricción para extender: una tarea es esencialmente una
    /// función `entrada → salida`. Para añadir una descripción al manifiesto,
    /// usa [`TypedTask::new`] + [`TypedTask::description`] y `register`.
    ///
    /// ```
    /// # use workflow_forge_core::task::{TaskRegistry, TaskCtx};
    /// # use serde::{Deserialize, Serialize};
    /// # use schemars::JsonSchema;
    /// #[derive(Deserialize, JsonSchema)]
    /// struct In { nombre: String }
    /// #[derive(Serialize, JsonSchema)]
    /// struct Out { saludo: String }
    ///
    /// let registry = TaskRegistry::new();
    /// registry.register_typed("demo.saludar", |_ctx: TaskCtx, input: In| async move {
    ///     Ok(Out { saludo: format!("Hola {}!", input.nombre) })
    /// });
    /// ```
    pub fn register_typed<In, Out, F, Fut>(&self, id: impl Into<TaskId>, f: F)
    where
        In: DeserializeOwned + JsonSchema + Send + 'static,
        Out: Serialize + JsonSchema + Send + 'static,
        F: Fn(TaskCtx, In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Out, WorkflowError>> + Send + 'static,
    {
        self.register(TypedTask::new(id, f));
    }

    /// Registra una tarea a partir de una closure async sobre JSON crudo
    /// ([`WorkflowData`]), sin schemas declarados. El atajo mínimo para
    /// transformaciones triviales. Para tipos y validación, usa
    /// [`register_typed`](Self::register_typed).
    ///
    /// ```
    /// # use workflow_forge_core::task::TaskRegistry;
    /// let registry = TaskRegistry::new();
    /// registry.register_fn("util.echo", |_ctx, input| async move { Ok(input) });
    /// ```
    pub fn register_fn<F, Fut>(&self, id: impl Into<TaskId>, f: F)
    where
        F: Fn(TaskCtx, WorkflowData) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WorkflowResult> + Send + 'static,
    {
        self.register(FnTask::new(id, f));
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
                codes::PROFILE_ID_CONFLICT,
                format!(
                    "No se puede registrar el perfil '{}': ya existe una tarea con ese id",
                    profile.id
                ),
            ));
        }
        let base = self.get(&profile.extends).ok_or_else(|| {
            WorkflowError::new(
                codes::PROFILE_BASE_NOT_FOUND,
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
        let map = self.tasks.read();
        TaskRegistry {
            tasks: Arc::new(RwLock::new(map.clone())),
        }
    }

    /// Obtiene una tarea por su tipo. Devuelve `None` si no está registrada.
    pub fn get(&self, task_id: &TaskId) -> Option<Arc<dyn Task>> {
        let map = self.tasks.read();
        map.get(task_id).cloned()
    }

    /// Lista todos los tipos de tarea registrados
    pub fn list(&self) -> Vec<TaskId> {
        let map = self.tasks.read();
        map.keys().cloned().collect()
    }

    /// Exporta el catálogo de manifiestos de todas las tareas registradas.
    /// Serializable a JSON: es la base de tooling, documentación y editores.
    pub fn catalog(&self) -> Vec<TaskManifest> {
        let map = self.tasks.read();
        let mut catalog: Vec<TaskManifest> =
            map.values().map(|task| task.manifest().clone()).collect();
        catalog.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        catalog
    }

    /// Verifica si un tipo de tarea está registrado
    pub fn contains(&self, task_id: &TaskId) -> bool {
        let map = self.tasks.read();
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

impl<T: Task> FromIterator<T> for TaskRegistry {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let registry = TaskRegistry::new();
        for task in iter {
            registry.register(task);
        }
        registry
    }
}
