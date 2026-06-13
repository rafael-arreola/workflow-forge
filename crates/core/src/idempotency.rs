//! Claves de idempotencia deterministas para tareas con efectos.
//!
//! Crear una orden (o cualquier efecto no reversible) bajo reintento es un
//! riesgo de correctitud: un timeout que sí llegó al destino, seguido de un
//! retry, puede crear la orden dos veces. La defensa es una **clave de
//! idempotencia** que el sistema destino usa para deduplicar.
//!
//! [`key_for`] deriva una clave **estable y direccionada por contenido** de un
//! valor JSON: el mismo payload produce siempre la misma clave — entre
//! reintentos *y* entre re-ejecuciones o replays de los mismos datos. Esa es
//! la semántica correcta para "nunca crear esta orden dos veces". La clave es
//! un UUID (v5, derivado del payload — determinista, no aleatorio), así que
//! es un valor seguro para headers o campos.
//!
//! Hay dos formas de obtener una clave:
//!
//! - **Declarativa** — la tarea `util.idempotency_key` (`{ value } -> { key }`),
//!   cuyo output se cablea al input o al `bind` del perfil de la tarea que
//!   crea el efecto (p. ej. un header `Idempotency-Key`).
//! - **En código** — llamando [`key_for`] (o
//!   [`crate::task::TaskCtx::idempotency_key`]) desde una
//!   [`crate::task::Task`] propia.
//!
//! Alcance: la clave es función pura del valor. Para unicidad por ejecución,
//! incluye un discriminador en el valor (p. ej. `$.workflow.execution_id`);
//! para unicidad por sistema destino, incluye el sistema. Composición, no
//! flags.

use serde_json::Value;
use uuid::Uuid;

/// Namespace fijo de las claves de idempotencia de workflow-forge (los
/// nombres UUID v5 viven bajo este namespace para no colisionar con otros
/// usos de v5).
const NAMESPACE: Uuid = Uuid::from_u128(0x77_6f_72_6b_66_6c_6f_77_5f_66_6f_72_67_65_69_64);

/// Clave estable derivada de `value`: payloads idénticos producen siempre la
/// clave idéntica. Devuelve un UUID (v5) como string.
///
/// El orden de las llaves de un objeto no afecta el resultado (la
/// codificación JSON canónica las ordena), así que dos payloads lógicamente
/// iguales producen el mismo hash.
///
/// ```
/// # use serde_json::json;
/// # use workflow_forge_core::idempotency::key_for;
/// let a = key_for(&json!({ "sku": "A", "qty": 2 }));
/// let b = key_for(&json!({ "qty": 2, "sku": "A" })); // otro orden de llaves
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
