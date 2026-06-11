//! Nodos `foreach`: iteración de un array invocando una tarea por elemento,
//! con concurrencia limitada, throttle entre arranques y política de error
//! por elemento (`fail` corta rápido, `collect` separa éxitos de fallos).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::{self, StreamExt};
use serde_json::{Value, json};

use crate::error::{WorkflowError, codes};
use crate::expr::mapping;
use crate::observe::EventKind;
use crate::runtime::context::WorkflowContext;
use crate::runtime::executor::WorkflowExecutor;
use crate::runtime::policy::ExecPolicy;
use crate::spec::node::Node;
use crate::spec::node::foreach::{ForeachNode, OnItemError};
use crate::task::Task;

impl WorkflowExecutor {
    /// Ejecuta un nodo foreach: resuelve `items` y ejecuta la tarea por cada
    /// elemento con la concurrencia/throttle configurados.
    pub(crate) async fn run_foreach(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        ctx: &WorkflowContext,
    ) -> Result<Value, WorkflowError> {
        let task = self
            .registry
            .get(&foreach.task)
            .expect("validate_tasks garantiza el registro");

        let items_value = ctx
            .with_state(|s| mapping::resolve(&foreach.items, s))
            .map_err(|e| e.with_source_task(node.id.to_string()))?;
        let Value::Array(items) = items_value else {
            return Err(WorkflowError::new(
                codes::FOREACH_ITEMS_NOT_ARRAY,
                format!(
                    "El mapping `items` del foreach '{}' no resolvió a un array",
                    node.id
                ),
            )
            .with_source_task(node.id.to_string()));
        };

        // Throttle: separa los ARRANQUES de elementos al menos `throttle_ms`
        // entre sí, también bajo concurrencia (rate limit hacia el destino)
        let gate = Arc::new(Mutex::new(Instant::now()));

        let futures: Vec<_> = items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                self.foreach_item(
                    node,
                    foreach,
                    Arc::clone(&task),
                    Arc::clone(&gate),
                    index,
                    item,
                    ctx,
                )
            })
            .collect();
        let mut buffered = stream::iter(futures).buffered(foreach.concurrency);

        match foreach.on_item_error {
            // Fallo rápido: el primer error corta el stream y cancela los
            // elementos aún en vuelo (drop del buffered)
            OnItemError::Fail => {
                let mut outputs = Vec::new();
                while let Some((_, _, result)) = buffered.next().await {
                    outputs.push(result?);
                }
                Ok(Value::Array(outputs))
            }
            // Ejecuta todo y separa éxitos de fallos; el nodo no falla...
            // salvo que un elemento panickee: eso aborta el nodo completo
            OnItemError::Collect => {
                let mut ok = Vec::new();
                let mut failed = Vec::new();
                while let Some((index, item, result)) = buffered.next().await {
                    match result {
                        Ok(output) => ok.push(output),
                        Err(error) if error.code == codes::TASK_PANIC => return Err(error),
                        Err(error) => failed.push(json!({
                            "index": index,
                            "item": item,
                            "error": serde_json::to_value(&error).unwrap_or(Value::Null),
                        })),
                    }
                }
                Ok(json!({ "ok": ok, "failed": failed }))
            }
        }
    }

    /// Ejecuta UN elemento de un foreach: respeta el throttle, aplica la
    /// política de retry/timeout y emite los eventos del elemento.
    /// Devuelve un future boxeado con lifetimes explícitos (rustc no logra
    /// probar `Send` de closures async dentro del executor recursivo).
    #[allow(clippy::too_many_arguments)]
    fn foreach_item<'a>(
        &'a self,
        node: &'a Node,
        foreach: &'a ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &'a WorkflowContext,
    ) -> BoxFuture<'a, (usize, Value, Result<Value, WorkflowError>)> {
        Box::pin(self.foreach_item_inner(node, foreach, task, gate, index, item, ctx))
    }

    #[allow(clippy::too_many_arguments)]
    async fn foreach_item_inner(
        &self,
        node: &Node,
        foreach: &ForeachNode,
        task: Arc<dyn Task>,
        gate: Arc<Mutex<Instant>>,
        index: usize,
        item: Value,
        ctx: &WorkflowContext,
    ) -> (usize, Value, Result<Value, WorkflowError>) {
        let throttle = Duration::from_millis(foreach.throttle_ms);
        if !throttle.is_zero() {
            let wait = {
                let mut next = gate.lock().expect("foreach gate lock poisoned");
                let now = Instant::now();
                let start_at = (*next).max(now);
                *next = start_at + throttle;
                start_at - now
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }

        let result = self
            .execute_with_policy(
                &task,
                &node.id,
                item.clone(),
                ExecPolicy {
                    retry: foreach.retry.as_ref(),
                    timeout_ms: foreach.timeout_ms,
                    emit_attempts: false,
                },
                ctx,
            )
            .await;
        match &result {
            Ok(output) => self.emit(
                ctx,
                EventKind::ForeachItemCompleted {
                    node_id: node.id.0.clone(),
                    index,
                    output: output.clone(),
                },
            ),
            Err(error) => self.emit(
                ctx,
                EventKind::ForeachItemFailed {
                    node_id: node.id.0.clone(),
                    index,
                    error: error.clone(),
                },
            ),
        }
        (index, item, result)
    }
}
