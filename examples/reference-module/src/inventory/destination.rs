use super::*;
use serde_json::json;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryUpdate {
    pub index: usize,
    pub line: u64,
    pub sku: String,
    pub quantity: u64,
}

/// The destination implements deduplication by key; the adapter does not infer it.
pub trait InventoryDestination: Send + Sync {
    fn apply<'a>(
        &'a self,
        key: &'a str,
        update: InventoryUpdate,
    ) -> PortFuture<'a, InventoryUpdate, OperationError>;
    fn inspect<'a>(&'a self, key: &'a str) -> PortFuture<'a, EffectInspection>;
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct InventorySnapshot {
    pub attempts: usize,
    pub effects: BTreeMap<String, InventoryUpdate>,
}
pub struct MemoryInventory {
    state: Mutex<InventorySnapshot>,
}
impl Default for MemoryInventory {
    fn default() -> Self {
        Self {
            state: Mutex::new(InventorySnapshot::default()),
        }
    }
}
impl MemoryInventory {
    pub fn snapshot(&self) -> Result<InventorySnapshot, ForgeError> {
        self.state.lock().map(|s| s.clone()).map_err(|_| {
            ForgeError::new(
                "reference.inventory.unavailable",
                "Destination is unavailable",
            )
        })
    }
}
impl InventoryDestination for MemoryInventory {
    fn apply<'a>(
        &'a self,
        key: &'a str,
        update: InventoryUpdate,
    ) -> PortFuture<'a, InventoryUpdate, OperationError> {
        Box::pin(async move {
            let mut state = self.state.lock().map_err(|_| {
                error(
                    "reference.inventory.unavailable",
                    ErrorClass::Internal,
                    EffectCertainty::NotApplied,
                    "Destination is unavailable",
                )
            })?;
            if state.attempts >= 100_000 {
                return Err(error(
                    "reference.inventory.full",
                    ErrorClass::Resource,
                    EffectCertainty::NotApplied,
                    "Destination attempt budget exhausted",
                ));
            }
            state.attempts += 1;
            if let Some(previous) = state.effects.get(key) {
                return if previous == &update {
                    Ok(previous.clone())
                } else {
                    Err(error(
                        "reference.inventory.conflict",
                        ErrorClass::Rejected,
                        EffectCertainty::Applied,
                        "Effect key already belongs to another update",
                    ))
                };
            }
            if state.effects.len() >= 20_000 {
                return Err(error(
                    "reference.inventory.full",
                    ErrorClass::Resource,
                    EffectCertainty::NotApplied,
                    "Destination capacity exhausted",
                ));
            }
            state.effects.insert(key.into(), update.clone());
            Ok(update)
        })
    }
    fn inspect<'a>(&'a self, key: &'a str) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            let state = self.state.lock().map_err(|_| {
                ForgeError::new(
                    "reference.inventory.unavailable",
                    "Destination is unavailable",
                )
            })?;
            let evidence = EffectEvidence {
                authority: "reference.memory_inventory".into(),
                reference: format!("effects/{key}"),
                note: "Atomic lookup in the controlled destination".into(),
            };
            Ok(match state.effects.get(key) {
                Some(output) => EffectInspection::Applied {
                    output: json!(output),
                    evidence,
                },
                None => EffectInspection::NotApplied {
                    evidence,
                    quiescent: true,
                },
            })
        })
    }
}

pub(super) struct ApplyRow {
    pub descriptor: OperationDescriptor,
    pub destination: Arc<dyn InventoryDestination>,
}
impl Operation for ApplyRow {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, _: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let update: InventoryUpdate = decode(invocation.input)?;
            if update.sku.is_empty() || update.sku.chars().count() > 64 {
                return Err(invalid("SKU is invalid"));
            }
            let key = invocation
                .effect_key
                .as_deref()
                .ok_or_else(|| invalid("A write requires its logical effect key"))?;
            let receipt = self.destination.apply(key, update.clone()).await?;
            if receipt != update {
                return Err(error(
                    "reference.inventory.receipt",
                    ErrorClass::Internal,
                    EffectCertainty::Applied,
                    "Destination receipt belongs to another row",
                ));
            }
            output(&receipt)
        })
    }
}
pub(super) struct Inspector {
    pub revision: OperationRevision,
    pub destination: Arc<dyn InventoryDestination>,
}
impl EffectInspector for Inspector {
    fn operation(&self) -> &OperationRevision {
        &self.revision
    }
    fn inspect<'a>(
        &'a self,
        _: OperationContext,
        invocation: Invocation,
    ) -> PortFuture<'a, EffectInspection> {
        Box::pin(async move {
            let key = invocation.effect_key.as_deref().ok_or_else(|| {
                ForgeError::new("reference.inventory.invalid", "Effect key is unavailable")
            })?;
            self.destination.inspect(key).await
        })
    }
}
