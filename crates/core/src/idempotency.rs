//! Deterministic idempotency keys for tasks with effects.
//!
//! Creating an order (or any non-reversible effect) under retry is a
//! correctness risk: a timeout that did reach the destination, followed by a
//! retry, can create the order twice. The defense is an **idempotency
//! key** that the target system uses for deduplication.
//!
//! [`key_for`] derives a **stable, content-addressed key** from a
//! JSON value: the same payload always produces the same key — across
//! retries *and* across re-executions or replays of the same data. That is the
//! correct semantic for "never create this order twice." The key is
//! a UUID (v5, derived from the payload — deterministic, not random), so it
//! is a safe value for headers or fields.
//!
//! There are two ways to obtain a key:
//!
//! - **Declarative** — the `util.idempotency_key` task (`{ value } -> { key }`),
//!   whose output is wired to the input or `bind` of the task profile that
//!   creates the effect (e.g., an `Idempotency-Key` header).
//! - **In code** — calling [`key_for`] (or
//!   [`crate::task::TaskCtx::idempotency_key`]) from a custom
//!   [`crate::task::Task`].
//!
//! Scope: the key is a pure function of the value. For per-execution uniqueness,
//! include a discriminator in the value (e.g., `$.workflow.execution_id`);
//! for per-target-system uniqueness, include the system. Composition, not
//! flags.

use serde_json::Value;
use uuid::Uuid;

/// Fixed namespace for workflow-forge idempotency keys (UUID v5
/// names live under this namespace to avoid collisions with other
/// v5 uses).
const NAMESPACE: Uuid = Uuid::from_u128(0x77_6f_72_6b_66_6c_6f_77_5f_66_6f_72_67_65_69_64);

/// Stable key derived from `value`: identical payloads always produce the
/// identical key. Returns a UUID (v5) as a string.
///
/// The order of an object's keys does not affect the result (canonical
/// JSON encoding sorts them), so two logically equal payloads produce
/// the same hash.
///
/// ```
/// # use serde_json::json;
/// # use workflow_forge_core::idempotency::key_for;
/// let a = key_for(&json!({ "sku": "A", "qty": 2 }));
/// let b = key_for(&json!({ "qty": 2, "sku": "A" })); // different key order
/// assert_eq!(a, b);
/// ```
pub fn key_for(value: &Value) -> String {
    let canonical = serde_json::to_vec(value).unwrap_or_default();
    Uuid::new_v5(&NAMESPACE, &canonical).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn estable_ante_orden_de_llaves() {
        assert_eq!(
            key_for(&json!({ "a": 1, "b": 2 })),
            key_for(&json!({ "b": 2, "a": 1 })),
        );
    }

    #[test]
    fn distinta_por_contenido() {
        assert_ne!(key_for(&json!({ "n": 1 })), key_for(&json!({ "n": 2 })));
    }

    #[test]
    fn es_un_uuid() {
        let key = key_for(&json!({ "order": "o-1" }));
        assert!(Uuid::parse_str(&key).is_ok());
    }
}
