use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;

/// Referencia a un binario/archivo grande según la convención `$blob` de la
/// spec: los blobs no viajan inline en el contexto JSON, viajan por referencia.
///
/// ```json
/// { "$blob": "01J…", "name": "ventas.csv.gz", "size": 52428800 }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRef {
    /// Identificador único del blob dentro de la ejecución
    #[serde(rename = "$blob")]
    pub id: String,
    /// Nombre de archivo sugerido
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Tamaño en bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

impl BlobRef {
    /// Interpreta un valor JSON como referencia a blob, si tiene la forma
    pub fn from_value(value: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(value.clone()).ok()
    }
}

/// Almacenamiento de blobs con ciclo de vida atado a una ejecución.
/// Las extensiones leen/escriben blobs a través del contexto, nunca tocan
/// el filesystem por su cuenta.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Almacena bytes como un blob nuevo
    async fn put(&self, data: Vec<u8>, name: Option<String>) -> Result<BlobRef, WorkflowError>;

    /// Lee el contenido completo de un blob
    async fn get(&self, blob: &BlobRef) -> Result<Vec<u8>, WorkflowError>;

    /// Importa un archivo existente copiándolo al store (para productores
    /// que escriben a disco por streaming antes de registrar el blob)
    async fn import_file(
        &self,
        path: &Path,
        name: Option<String>,
    ) -> Result<BlobRef, WorkflowError>;

    /// Ruta local del blob, para consumidores que leen por streaming.
    /// En v1 todos los blobs son file-backed.
    fn local_path(&self, blob: &BlobRef) -> Result<PathBuf, WorkflowError>;

    /// Elimina todos los blobs de la ejecución
    async fn cleanup(&self) -> Result<(), WorkflowError>;
}

/// `BlobStore` v1: un directorio temporal por ejecución, eliminado al
/// terminar el workflow (y como red de seguridad, al hacer drop).
pub struct TempDirBlobStore {
    dir: PathBuf,
}

impl TempDirBlobStore {
    /// Crea el store de una ejecución. El directorio se crea de forma
    /// perezosa en el primer `put`/`import_file`.
    pub fn new(execution_id: &str) -> Self {
        Self {
            dir: std::env::temp_dir().join(format!("workflow-forge-{execution_id}")),
        }
    }

    /// Directorio raíz de los blobs de esta ejecución
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    async fn ensure_dir(&self) -> Result<(), WorkflowError> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| io_error("no se pudo crear el directorio de blobs", e))
    }

    fn blob_path(&self, id: &str) -> Result<PathBuf, WorkflowError> {
        // Los ids son UUIDs generados por el store; cualquier otra cosa es
        // una referencia forjada (p.ej. path traversal)
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(WorkflowError::new(
                "INVALID_BLOB_ID",
                format!("El id de blob '{id}' no es válido"),
            ));
        }
        Ok(self.dir.join(id))
    }
}

#[async_trait]
impl BlobStore for TempDirBlobStore {
    async fn put(&self, data: Vec<u8>, name: Option<String>) -> Result<BlobRef, WorkflowError> {
        self.ensure_dir().await?;
        let id = uuid::Uuid::now_v7().to_string();
        let size = data.len() as u64;
        let path = self.dir.join(&id);
        tokio::fs::write(&path, data)
            .await
            .map_err(|e| io_error("no se pudo escribir el blob", e))?;
        Ok(BlobRef {
            id,
            name,
            size: Some(size),
        })
    }

    async fn get(&self, blob: &BlobRef) -> Result<Vec<u8>, WorkflowError> {
        let path = self.blob_path(&blob.id)?;
        tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                blob_not_found(&blob.id)
            } else {
                io_error("no se pudo leer el blob", e)
            }
        })
    }

    async fn import_file(
        &self,
        path: &Path,
        name: Option<String>,
    ) -> Result<BlobRef, WorkflowError> {
        self.ensure_dir().await?;
        let id = uuid::Uuid::now_v7().to_string();
        let dest = self.dir.join(&id);
        let size = tokio::fs::copy(path, &dest)
            .await
            .map_err(|e| io_error("no se pudo importar el archivo al store", e))?;
        let name = name.or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()));
        Ok(BlobRef {
            id,
            name,
            size: Some(size),
        })
    }

    fn local_path(&self, blob: &BlobRef) -> Result<PathBuf, WorkflowError> {
        let path = self.blob_path(&blob.id)?;
        if !path.exists() {
            return Err(blob_not_found(&blob.id));
        }
        Ok(path)
    }

    async fn cleanup(&self) -> Result<(), WorkflowError> {
        match tokio::fs::remove_dir_all(&self.dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("no se pudo limpiar el directorio de blobs", e)),
        }
    }
}

// Red de seguridad: si la ejecución termina sin cleanup (p.ej. panic),
// el directorio temporal no queda huérfano
impl Drop for TempDirBlobStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn io_error(message: &str, source: std::io::Error) -> WorkflowError {
    let mut err = WorkflowError::new("BLOB_IO_ERROR", format!("{message}: {source}"));
    err.source = Some(Box::new(source));
    err
}

fn blob_not_found(id: &str) -> WorkflowError {
    WorkflowError::new(
        "BLOB_NOT_FOUND",
        format!("El blob '{id}' no existe en esta ejecución"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> TempDirBlobStore {
        TempDirBlobStore::new(&uuid::Uuid::now_v7().to_string())
    }

    #[tokio::test]
    async fn put_get_roundtrip_y_serde() {
        let store = store();
        let blob = store
            .put(b"hola blob".to_vec(), Some("saludo.txt".into()))
            .await
            .unwrap();

        assert_eq!(blob.name.as_deref(), Some("saludo.txt"));
        assert_eq!(blob.size, Some(9));

        // La referencia serializa con la forma de la spec
        let as_json = serde_json::to_value(&blob).unwrap();
        assert_eq!(as_json["$blob"], json!(blob.id));
        assert_eq!(BlobRef::from_value(&as_json), Some(blob.clone()));

        assert_eq!(store.get(&blob).await.unwrap(), b"hola blob");
        store.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn import_file_y_local_path() {
        let store = store();
        let origen = std::env::temp_dir().join(format!("wf-test-{}", uuid::Uuid::now_v7()));
        tokio::fs::write(&origen, b"contenido").await.unwrap();

        let blob = store.import_file(&origen, None).await.unwrap();
        assert_eq!(blob.size, Some(9));
        assert!(blob.name.as_deref().unwrap().starts_with("wf-test-"));

        let path = store.local_path(&blob).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"contenido");

        tokio::fs::remove_file(&origen).await.unwrap();
        store.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn ids_forjados_son_rechazados() {
        let store = store();
        store.put(b"x".to_vec(), None).await.unwrap();

        let forjado = BlobRef {
            id: "../../../etc/passwd".into(),
            name: None,
            size: None,
        };
        assert_eq!(
            store.get(&forjado).await.unwrap_err().code,
            "INVALID_BLOB_ID"
        );
        assert_eq!(
            store.local_path(&forjado).unwrap_err().code,
            "INVALID_BLOB_ID"
        );

        let inexistente = BlobRef {
            id: uuid::Uuid::now_v7().to_string(),
            name: None,
            size: None,
        };
        assert_eq!(
            store.get(&inexistente).await.unwrap_err().code,
            "BLOB_NOT_FOUND"
        );
        store.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_elimina_el_directorio() {
        let store = store();
        store.put(b"x".to_vec(), None).await.unwrap();
        let dir = store.dir().to_path_buf();
        assert!(dir.exists());

        store.cleanup().await.unwrap();
        assert!(!dir.exists());
        // idempotente
        store.cleanup().await.unwrap();
    }
}
