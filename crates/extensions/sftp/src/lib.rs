//! workflow-forge `sftp` extension: file transfer over SFTP,
//! using the `$blob` convention for content.
//!
//! | Task | Contract |
//! |-------|----------|
//! | `sftp.get` | `{ connection, path }` → `{ file: $blob }` |
//! | `sftp.put` | `{ connection, file: $blob, path }` → `{ path, size }` |
//! | `sftp.list` | `{ connection, path }` → `{ entries: [{name, kind, size, modified}] }` |
//!
//! `connection`: `{ host, port?, username, auth: {type: "password"|"key", ...},
//! known_hosts? }`. If `known_hosts` is provided (path to an OpenSSH format
//! file), the host key is verified and the connection fails on mismatch;
//! without it, the connection does NOT verify the server identity.
//!
//! Files are transferred via streaming to/from the execution's `BlobStore`:
//! they are never fully loaded into memory.

use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::WorkflowContext;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

mod pool;
use pool::{Connector, Pool};

/// Pool of reusable SSH sessions across calls (see [`register_pooled`]).
pub type SftpPool = Pool<Ssh2Connector>;

/// Registers SFTP tasks that **open a new session per call**.
/// For high volume against the same hosts, prefer [`register_pooled`].
pub fn register(registry: &TaskRegistry) {
    registry.register(GetTask::default());
    registry.register(PutTask::default());
    registry.register(ListTask::default());
}

/// Registers SFTP tasks backed by a shared [`SftpPool`], which
/// **reuses authenticated SSH sessions** across calls (the expensive part:
/// TCP + handshake + auth). `max_idle_per_conn` bounds how many idle sessions
/// are kept per connection identity. The recommended approach when a `foreach`
/// makes many transfers against the same server.
pub fn register_pooled(registry: &TaskRegistry, max_idle_per_conn: usize) {
    let pool = Arc::new(SftpPool::new(Ssh2Connector, max_idle_per_conn));
    registry.register(GetTask::pooled(pool.clone()));
    registry.register(PutTask::pooled(pool.clone()));
    registry.register(ListTask::pooled(pool));
}

fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
}

/// Error codes this extension can emit. Same contract as
/// [`workflow_forge_core::error::codes`]: stable constants, never change
/// value. Blob errors reuse the core codes.
pub mod codes {
    /// The input of an `sftp.*` task does not deserialize against its contract.
    pub const SFTP_INPUT_INVALID: &str = "SFTP_INPUT_INVALID";
    /// SFTP connection, authentication, or transfer failure. Usually
    /// transient: a natural candidate for `retry`.
    pub const SFTP_ERROR: &str = "SFTP_ERROR";
    /// The host does not appear in the provided `known_hosts` file.
    pub const SFTP_HOST_UNKNOWN: &str = "SFTP_HOST_UNKNOWN";
    /// The host key does NOT match `known_hosts` (possible MITM).
    /// Should never be blindly retried.
    pub const SFTP_HOST_KEY_MISMATCH: &str = "SFTP_HOST_KEY_MISMATCH";
}

fn sftp_error(message: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::new(codes::SFTP_ERROR, message.to_string())
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// SFTP connection parameters. Opaque type (private fields): deserialized
/// from task input; appears in the pool's public signature.
#[derive(Clone, Deserialize, Serialize)]
pub struct Connection {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    username: String,
    auth: Auth,
    /// Path to a known_hosts file (OpenSSH format) to verify the host
    #[serde(default)]
    known_hosts: Option<PathBuf>,
}

fn default_port() -> u16 {
    22
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Auth {
    Password {
        password: String,
    },
    Key {
        /// Local path to the private key
        private_key: PathBuf,
        #[serde(default)]
        passphrase: Option<String>,
    },
}

/// Reusable JSON schema for the `connection` object
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

/// Opens an authenticated SSH session (without an SFTP channel yet). Blocking:
/// invoke inside spawn_blocking. This is the expensive part (TCP + handshake + auth)
/// that the pool reuses.
fn connect_session(conn: &Connection) -> Result<ssh2::Session, WorkflowError> {
    let tcp = TcpStream::connect((conn.host.as_str(), conn.port)).map_err(|e| {
        sftp_error(format!(
            "could not connect to {}:{}: {e}",
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
            .map_err(|e| sftp_error(format!("password authentication failed: {e}")))?,
        Auth::Key {
            private_key,
            passphrase,
        } => session
            .userauth_pubkey_file(&conn.username, None, private_key, passphrase.as_deref())
            .map_err(|e| sftp_error(format!("key authentication failed: {e}")))?,
    }

    Ok(session)
}

/// ssh2 pool connector: opens real sessions and checks their health with a
/// keepalive (a session killed by server timeout is discarded and
/// reconnected).
pub struct Ssh2Connector;

impl Connector for Ssh2Connector {
    type Conn = ssh2::Session;

    fn connect(&self, conn: &Connection) -> Result<ssh2::Session, WorkflowError> {
        connect_session(conn)
    }

    fn alive(&self, session: &ssh2::Session) -> bool {
        session.keepalive_send().is_ok()
    }
}

/// Stable identity of a connection (host/port/user/auth/known_hosts),
/// to index the pool. Reuses the deterministic idempotency hash.
fn conn_key(conn: &Connection) -> String {
    workflow_forge_core::idempotency::key_for(&serde_json::to_value(conn).unwrap_or(Value::Null))
}

/// Executes `f` with an SFTP channel, taking the session from the pool (reused)
/// or from a fresh `connect_session`. The session is returned to the pool
/// **only if `f` succeeded** (a session that failed mid-operation is
/// discarded). Blocking.
fn with_sftp<R>(
    pool: &Option<Arc<SftpPool>>,
    conn: &Connection,
    f: impl FnOnce(&ssh2::Sftp) -> Result<R, WorkflowError>,
) -> Result<R, WorkflowError> {
    let key = conn_key(conn);
    let session = match pool {
        Some(p) => p.checkout(&key, conn)?,
        None => connect_session(conn)?,
    };
    let sftp = session.sftp().map_err(sftp_error)?;
    let result = f(&sftp);
    if result.is_ok() {
        drop(sftp);
        if let Some(p) = pool {
            p.checkin(&key, session);
        }
    }
    result
}

fn verify_host_key(
    session: &ssh2::Session,
    conn: &Connection,
    known_hosts_path: &std::path::Path,
) -> Result<(), WorkflowError> {
    let mut known_hosts = session.known_hosts().map_err(sftp_error)?;
    known_hosts
        .read_file(known_hosts_path, ssh2::KnownHostFileKind::OpenSSH)
        .map_err(|e| sftp_error(format!("could not read known_hosts: {e}")))?;
    let (key, _) = session
        .host_key()
        .ok_or_else(|| sftp_error("server did not present a host key"))?;
    match known_hosts.check_port(&conn.host, conn.port, key) {
        ssh2::CheckResult::Match => Ok(()),
        ssh2::CheckResult::NotFound => Err(WorkflowError::new(
            codes::SFTP_HOST_UNKNOWN,
            format!("Host '{}' is not in known_hosts", conn.host),
        )),
        ssh2::CheckResult::Mismatch => Err(WorkflowError::new(
            codes::SFTP_HOST_KEY_MISMATCH,
            format!(
                "Host key for '{}' does NOT match known_hosts (possible MITM)",
                conn.host
            ),
        )),
        ssh2::CheckResult::Failure => Err(sftp_error("host verification failed")),
    }
}

// ---------------------------------------------------------------------------
// sftp.get
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetInput {
    connection: Connection,
    /// Remote path of the file to download
    path: String,
    /// Name of the resulting blob (default: remote file name)
    #[serde(default)]
    name: Option<String>,
}

pub struct GetTask {
    manifest: TaskManifest,
    pool: Option<Arc<SftpPool>>,
}

impl Default for GetTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.get");
        manifest.description =
            Some("Downloads a remote file via SFTP and registers it as a $blob".into());
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
        Self {
            manifest,
            pool: None,
        }
    }
}

impl GetTask {
    /// Variant backed by a shared session pool.
    pub fn pooled(pool: Arc<SftpPool>) -> Self {
        Self {
            pool: Some(pool),
            ..Self::default()
        }
    }
}

#[async_trait]
impl Task for GetTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: GetInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new(codes::SFTP_INPUT_INVALID, e.to_string()))?;
        let name = parsed.name.clone().unwrap_or_else(|| {
            parsed
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&parsed.path)
                .to_string()
        });

        // Streaming download to a local temporary file
        let temp = std::env::temp_dir().join(format!("wf-sftp-{}", uuid::Uuid::now_v7()));
        let temp_for_blocking = temp.clone();
        let pool = self.pool.clone();
        let download = tokio::task::spawn_blocking(move || -> Result<(), WorkflowError> {
            with_sftp(&pool, &parsed.connection, |sftp| {
                let mut remote = sftp
                    .open(std::path::Path::new(&parsed.path))
                    .map_err(|e| sftp_error(format!("could not open '{}': {e}", parsed.path)))?;
                let mut local = std::fs::File::create(&temp_for_blocking).map_err(sftp_error)?;
                std::io::copy(&mut remote, &mut local).map_err(sftp_error)?;
                local.flush().map_err(sftp_error)?;
                Ok(())
            })
        })
        .await
        .map_err(|e| sftp_error(format!("download was interrupted: {e}")))
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
    /// Local blob to upload
    file: BlobRef,
    /// Remote destination path
    path: String,
}

pub struct PutTask {
    manifest: TaskManifest,
    pool: Option<Arc<SftpPool>>,
}

impl Default for PutTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.put");
        manifest.description = Some("Uploads a $blob to a remote path via SFTP".into());
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
        Self {
            manifest,
            pool: None,
        }
    }
}

impl PutTask {
    /// Variant backed by a shared session pool.
    pub fn pooled(pool: Arc<SftpPool>) -> Self {
        Self {
            pool: Some(pool),
            ..Self::default()
        }
    }
}

#[async_trait]
impl Task for PutTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: PutInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new(codes::SFTP_INPUT_INVALID, e.to_string()))?;
        let local_path = ctx.blobs().local_path(&parsed.file)?;

        let remote_path = parsed.path.clone();
        let pool = self.pool.clone();
        let size = tokio::task::spawn_blocking(move || -> Result<u64, WorkflowError> {
            with_sftp(&pool, &parsed.connection, |sftp| {
                let mut local = std::fs::File::open(&local_path).map_err(sftp_error)?;
                let mut remote = sftp
                    .create(std::path::Path::new(&parsed.path))
                    .map_err(|e| sftp_error(format!("could not create '{}': {e}", parsed.path)))?;
                let size = std::io::copy(&mut local, &mut remote).map_err(sftp_error)?;
                remote.flush().map_err(sftp_error)?;
                Ok(size)
            })
        })
        .await
        .map_err(|e| sftp_error(format!("upload was interrupted: {e}")))
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
    /// Remote directory to list
    path: String,
}

pub struct ListTask {
    manifest: TaskManifest,
    pool: Option<Arc<SftpPool>>,
}

impl Default for ListTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("sftp.list");
        manifest.description = Some("Lists the entries of a remote directory".into());
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
        Self {
            manifest,
            pool: None,
        }
    }
}

impl ListTask {
    /// Variant backed by a shared session pool.
    pub fn pooled(pool: Arc<SftpPool>) -> Self {
        Self {
            pool: Some(pool),
            ..Self::default()
        }
    }
}

#[async_trait]
impl Task for ListTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let parsed: ListInput = serde_json::from_value(input.0)
            .map_err(|e| WorkflowError::new(codes::SFTP_INPUT_INVALID, e.to_string()))?;

        let pool = self.pool.clone();
        let entries = tokio::task::spawn_blocking(move || -> Result<Vec<Value>, WorkflowError> {
            with_sftp(&pool, &parsed.connection, |sftp| {
                let listing = sftp
                    .readdir(std::path::Path::new(&parsed.path))
                    .map_err(|e| sftp_error(format!("could not list '{}': {e}", parsed.path)))?;
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
        })
        .await
        .map_err(|e| sftp_error(format!("listing was interrupted: {e}")))
        .and_then(|r| r)?;

        Ok(WorkflowData(json!({ "entries": entries })))
    }
}
