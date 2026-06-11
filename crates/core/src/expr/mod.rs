//! La familia **expr**: resolución de expresiones del lenguaje.
//!
//! Los workflows declaran datos con tres convenciones de string, todas
//! resueltas con el mismo motor JSONPath ([`path`]):
//!
//! | Convención | Dónde se usa | Documento fuente |
//! |---|---|---|
//! | `$.path` ([`mapping`]) | `input` de nodos, `items` de foreach, `output` de end | contexto de ejecución |
//! | `@.path` ([`shape`]) | `bind`/`output` de perfiles, `data.transform` | un valor local (input/output) |
//! | `{"$secret": "X"}` | `bind` de perfiles | provider de secretos ([`crate::io::secret`]) |
//!
//! Diferencia semántica clave: un path `$.` que no resuelve es **error**
//! (`MAPPING_PATH_NOT_FOUND`, casi siempre bug de definición); un path `@.`
//! que no resuelve produce **`null`** (los shapes rellenan estructuras).
//!
//! Las condiciones de la spec se evalúan en [`operators`], que además es el
//! punto de extensión del vocabulario: un host registra operadores propios
//! ([`operators::ConditionOperator`]) y la validación los reconoce.

pub mod mapping;
pub mod operators;
pub mod path;
pub mod shape;

pub use mapping::resolve;
pub use shape::apply_shape;
