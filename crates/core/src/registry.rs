use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::task::{Task, TaskId, TaskManifest};

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
        let key = task.task_id().clone();
        let mut map = self.tasks.write().expect("TaskRegistry lock poisoned");
        map.insert(key, Arc::new(task));
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
