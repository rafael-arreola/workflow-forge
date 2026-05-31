use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::task::Task;

/// Registro global de tareas disponibles para ejecución.
/// Las tareas se registran por su `task_type()` y se resuelven en runtime
/// cuando el executor encuentra un nodo de tipo `Task`.
///
/// Thread-safe: usa `Arc<RwLock<>>` para permitir lectura concurrente
/// y registro puntual desde múltiples hilos.
#[derive(Default)]
pub struct TaskRegistry {
    tasks: Arc<RwLock<HashMap<String, Arc<dyn Task>>>>,
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
        let key = task.task_type().to_string();
        let mut map = self.tasks.write().expect("TaskRegistry lock poisoned");
        map.insert(key, Arc::new(task));
    }

    /// Obtiene una tarea por su tipo. Devuelve `None` si no está registrada.
    pub fn get(&self, task_type: &str) -> Option<Arc<dyn Task>> {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.get(task_type).cloned()
    }

    /// Lista todos los tipos de tarea registrados
    pub fn list(&self) -> Vec<String> {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.keys().cloned().collect()
    }

    /// Verifica si un tipo de tarea está registrado
    pub fn contains(&self, task_type: &str) -> bool {
        let map = self.tasks.read().expect("TaskRegistry lock poisoned");
        map.contains_key(task_type)
    }
}

impl Clone for TaskRegistry {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
        }
    }
}
