//! Extensión `sftp` de workflow-forge: transferencia de archivos sobre SFTP,
//! usando la convención `$blob` para el contenido.
//!
//! | Tarea | Contrato |
//! |-------|----------|
//! | `sftp.get` | `{ connection, path }` → `{ file: $blob }` |
//! | `sftp.put` | `{ connection, file: $blob, path }` → `{ path, size }` |
//! | `sftp.list` | `{ connection, path }` → `{ entries: [{name, kind, size, modified}] }` |
//!
//! `connection`: `{ host, port?, username, auth: {type: "password"|"key", ...},
//! known_hosts? }`. Si se provee `known_hosts` (ruta a un archivo formato
//! OpenSSH), la llave del host se verifica y la conexión falla si no coincide;
//! sin él, la conexión NO verifica la identidad del servidor.
//!
//! Los archivos se transfieren por streaming hacia/desde el `BlobStore` de la
//! ejecución: nunca se cargan completos en memoria.

use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use workflow_forge_core::blob::BlobRef;
use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(GetTask::default());
    registry.register(PutTask::default());
    registry.register(ListTask::default());
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}

fn sftp_error(message: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new("SFTP_ERROR", message.to_string())
}

// ---------------------------------------------------------------------------
// Conexión
// ---------------------------------------------------------------------------

#[derive(Clone, Deserialize)]
struct Connection {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    username: String,
    auth: Auth,
    /// Ruta a un archivo known_hosts (formato OpenSSH) para verificar el host
    #[serde(default)]
    known_hosts: Option<PathBuf>,
}

fn default_port() -> u16 {
    22
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Auth {
    Password {
        password: String,
    },
    Key {
        /// Ruta local a la llave privada
        private_key: PathBuf,
        #[serde(default)]
        passphrase: Option<String>,
    },
}

/// Schema JSON reutilizable del objeto `connection`
fn connection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["host", "username", "auth"],
        "properties": {
            "host": { "type": "string" },
            "port": { "type": "integer", "default": 22 },
            "username": { "type": "string" },
            "auth": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": { "enum": ["password", "key"] },
                    "password": { "type": "string" },
                    "private_key": { "type": "string" },
                    "passphrase": { "type": "string" }
                }
            },
            "known_hosts": { "type": "string" }
        }
    })
}

/// Abre sesión + canal SFTP. Bloqueante: invocar dentro de spawn_blocking.
fn connect(conn: &Connection) -> Result<(ssh2::Session, ssh2::Sftp), WorkflowError> {
    let tcp = TcpStream::connect((conn.host.as_str(), conn.port)).map_err(|e| {
        sftp_error(format!(
            "no se pudo conectar a {}:{}: {e}",
            conn.host, conn.port
        ))
    })?;
    let mut session = ssh2::Session::new().map_err(sftp_error)?;
    session.set_tcp_stream(tcp);
    session.handshake().map_err(sftp_error)?;

    if let Some(known_hosts_path) = &conn.known_hosts {
        verify_host_key(&session, conn, known_hosts_path)?;
    }

    match &conn.auth {
        Auth::Password { password } => session
            .userauth_password(&conn.username, password)
            .map_err(|e| sftp_error(format!("autenticación por password falló: {e}")))?,
        Auth::Key {
            private_key,
            passphrase,
        } => session
            .userauth_pubkey_file(&conn.username, None, private_key, passphrase.as_deref())
            .map_err(|e| sftp_error(format!("autenticación por llave falló: {e}")))?,
    }

    let sftp = session.sftp().map_err(sftp_error)?;
    Ok((session, sftp))
}

fn verify_host_key(
    session: &ssh2::Session,
    conn: &Connection,
    known_hosts_path: &std::path::Path,
) -> Result<(), WorkflowError> {
    let mut known_hosts = session.known_hosts().map_err(sftp_error)?;
    known_hosts
        .read_file(known_hosts_path, ssh2::KnownHostFileKind::OpenSSH)
        .map_err(|e| sftp_error(format!("no se pudo leer known_hosts: {e}")))?;
    let (key, _) = session
        .host_key()
        .ok_or_else(|| sftp_error("el servidor no presentó llave de host"))?;
    match known_hosts.check_port(&conn.host, conn.port, key) {
        ssh2::CheckResult::Match => Ok(()),
        ssh2::CheckResult::NotFound => Err(WorkflowError::new(
            "SFTP_HOST_UNKNOWN",
            format!("El host '{}' no está en known_hosts", conn.host),
        )),
        ssh2::CheckResult::Mismatch => Err(WorkflowError::new(
            "SFTP_HOST_KEY_MISMATCH",
            format!(
                "La llave del host '{}' NO coincide con known_hosts (posible MITM)",
                conn.host
            ),
        )),
        ssh2::CheckResult::Failure => Err(sftp_error("la verificación de host falló")),
    }
}

// ---------------------------------------------------------------------------
// sftp.get
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetInput {
    connection: Connection,
    /// Ruta remota del archivo a descargar
    path: String,
    /// Nombre del blob resultante (default: nombre del archivo remoto)
    #[serde(default)]
    name: Option<String>,
}

pub struct GetTask {
    manifest: TaskManifest,
}

impl Default for GetTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.get");
        manifest.description =
            Some("Descarga un archivo remoto por SFTP y lo registra como $blob".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["connection", "path"],
            "properties": {
                "connection": connection_schema(),
                "path": { "type": "string" },
                "name": { "type": "string" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["file"],
            "properties": { "file": { "type": "object", "required": ["$blob"] } }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for GetTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: GetInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new("SFTP_INPUT_INVALID", e.to_string()))?;
        let name = parsed.name.clone().unwrap_or_else(|| {
            parsed
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&parsed.path)
                .to_string()
        });

        // Descarga por streaming a un archivo temporal local
        let temp = std::env::temp_dir().join(format!("wf-sftp-{}", uuid::Uuid::now_v7()));
        let temp_for_blocking = temp.clone();
        let download = tokio::task::spawn_blocking(move || -> Result<(), WorkflowError> {
            let (_session, sftp) = connect(&parsed.connection)?;
            let mut remote = sftp
                .open(std::path::Path::new(&parsed.path))
                .map_err(|e| sftp_error(format!("no se pudo abrir '{}': {e}", parsed.path)))?;
            let mut local = std::fs::File::create(&temp_for_blocking).map_err(sftp_error)?;
            std::io::copy(&mut remote, &mut local).map_err(sftp_error)?;
            local.flush().map_err(sftp_error)?;
            Ok(())
        })
        .await
        .map_err(|e| sftp_error(format!("la descarga se interrumpió: {e}")))
        .and_then(|r| r);

        if let Err(err) = download {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err);
        }

        let blob = ctx.blobs().import_file(&temp, Some(name)).await;
        let _ = tokio::fs::remove_file(&temp).await;
        Ok(WorkflowData(json!({ "file": blob? })))
    }
}

// ---------------------------------------------------------------------------
// sftp.put
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PutInput {
    connection: Connection,
    /// Blob local a subir
    file: BlobRef,
    /// Ruta remota destino
    path: String,
}

pub struct PutTask {
    manifest: TaskManifest,
}

impl Default for PutTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.put");
        manifest.description = Some("Sube un $blob a una ruta remota por SFTP".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["connection", "file", "path"],
            "properties": {
                "connection": connection_schema(),
                "file": { "type": "object", "required": ["$blob"] },
                "path": { "type": "string" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["path", "size"],
            "properties": {
                "path": { "type": "string" },
                "size": { "type": "integer" }
            }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for PutTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: PutInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new("SFTP_INPUT_INVALID", e.to_string()))?;
        let local_path = ctx.blobs().local_path(&parsed.file)?;

        let remote_path = parsed.path.clone();
        let size = tokio::task::spawn_blocking(move || -> Result<u64, WorkflowError> {
            let (_session, sftp) = connect(&parsed.connection)?;
            let mut local = std::fs::File::open(&local_path).map_err(sftp_error)?;
            let mut remote = sftp
                .create(std::path::Path::new(&parsed.path))
                .map_err(|e| sftp_error(format!("no se pudo crear '{}': {e}", parsed.path)))?;
            let size = std::io::copy(&mut local, &mut remote).map_err(sftp_error)?;
            remote.flush().map_err(sftp_error)?;
            Ok(size)
        })
        .await
        .map_err(|e| sftp_error(format!("la subida se interrumpió: {e}")))
        .and_then(|r| r)?;

        Ok(WorkflowData(json!({ "path": remote_path, "size": size })))
    }
}

// ---------------------------------------------------------------------------
// sftp.list
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListInput {
    connection: Connection,
    /// Directorio remoto a listar
    path: String,
}

pub struct ListTask {
    manifest: TaskManifest,
}

impl Default for ListTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.list");
        manifest.description = Some("Lista las entradas de un directorio remoto".into());
        manifest.input_schema = Some(schema(json!({
            "type": "object",
            "required": ["connection", "path"],
            "properties": {
                "connection": connection_schema(),
                "path": { "type": "string" }
            }
        })));
        manifest.output_schema = Some(schema(json!({
            "type": "object",
            "required": ["entries"],
            "properties": {
                "entries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name", "kind"],
                        "properties": {
                            "name": { "type": "string" },
                            "kind": { "enum": ["file", "dir", "other"] },
                            "size": { "type": "integer" },
                            "modified": { "type": "integer" }
                        }
                    }
                }
            }
        })));
        Self { manifest }
    }
}

#[async_trait]
impl Task for ListTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: ListInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new("SFTP_INPUT_INVALID", e.to_string()))?;

        let entries = tokio::task::spawn_blocking(move || -> Result<Vec<Value>, WorkflowError> {
            let (_session, sftp) = connect(&parsed.connection)?;
            let listing = sftp
                .readdir(std::path::Path::new(&parsed.path))
                .map_err(|e| sftp_error(format!("no se pudo listar '{}': {e}", parsed.path)))?;
            Ok(listing
                .into_iter()
                .map(|(path, stat)| {
                    let kind = if stat.is_dir() {
                        "dir"
                    } else if stat.is_file() {
                        "file"
                    } else {
                        "other"
                    };
                    json!({
                        "name": path.file_name().map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        "kind": kind,
                        "size": stat.size,
                        "modified": stat.mtime
                    })
                })
                .collect())
        })
        .await
        .map_err(|e| sftp_error(format!("el listado se interrumpió: {e}")))
        .and_then(|r| r)?;

        Ok(WorkflowData(json!({ "entries": entries })))
    }
}
