//! Reference composition root. Credentials are resolved by the host before boot;
//! custom applications can instead compose modules and call `ServiceRuntime`.
use crate::{BearerIdentity, ServiceOptions, ServiceRuntime, StaticBearerAuth};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncReadExt;
use workflow_forge::v2::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    pub token_env: String,
    pub actor: String,
    pub permissions: BTreeSet<Permission>,
    #[serde(default)]
    pub resources: BTreeSet<String>,
}
fn database_path() -> PathBuf {
    PathBuf::from("workflow-forge.sqlite")
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageConfig {
    Sqlite {
        #[serde(default = "database_path")]
        path: PathBuf,
        #[serde(default)]
        options: modules::SqliteOptions,
    },
    Memory,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self::Sqlite {
            path: database_path(),
            options: Default::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    pub max_prepared: usize,
    pub max_requests: usize,
    pub max_json_bytes: usize,
    pub max_response_bytes: usize,
    pub max_artifact_bytes: usize,
    pub request_timeout_ms: u64,
    pub http_shutdown_timeout_ms: u64,
    pub engine_shutdown_timeout_ms: u64,
}
impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_prepared: 1000,
            max_requests: 64,
            max_json_bytes: 2 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_artifact_bytes: 8 * 1024 * 1024,
            request_timeout_ms: 30_000,
            http_shutdown_timeout_ms: 30_000,
            engine_shutdown_timeout_ms: 30_000,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostConfig {
    pub scope: String,
    pub listen: SocketAddr,
    pub identities: Vec<IdentityConfig>,
    pub storage: StorageConfig,
    pub engine_limits: Limits,
    pub transport: TransportConfig,
    /// Workflow paths are relative to the configuration file, then pinned at boot.
    pub workflows: Vec<PathBuf>,
    /// Logical secret name -> environment variable. Values never enter config.
    pub secrets: BTreeMap<String, String>,
    pub http: Vec<modules::HttpJsonProfile>,
    pub files: Vec<modules::FileReadProfile>,
    pub csv: Option<modules::CsvOptions>,
}
impl Default for HostConfig {
    fn default() -> Self {
        Self {
            scope: "default".into(),
            listen: ([127, 0, 0, 1], 7070).into(),
            identities: Vec::new(),
            storage: Default::default(),
            engine_limits: Default::default(),
            transport: Default::default(),
            workflows: Vec::new(),
            secrets: BTreeMap::new(),
            http: Vec::new(),
            files: Vec::new(),
            csv: None,
        }
    }
}
fn invalid(message: &str) -> ForgeError {
    ForgeError::new("service.config", message)
}
async fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>, ForgeError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| invalid("Configured input file is unavailable"))?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| invalid("Configured input file could not be read"))?;
    if bytes.len() > limit {
        return Err(invalid("Configured input file exceeds its byte budget"));
    }
    Ok(bytes)
}
fn environment(name: &str) -> Result<String, ForgeError> {
    if name.is_empty()
        || name.len() > 256
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(invalid("Invalid credential environment reference"));
    }
    let value = std::env::var(name)
        .map_err(|_| invalid("Required credential environment variable is unavailable"))?;
    if value.is_empty() || value.len() > 16 * 1024 {
        return Err(invalid("Credential value exceeds its budget or is empty"));
    }
    Ok(value)
}
impl HostConfig {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self, ForgeError> {
        let path = path.as_ref();
        let path = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| invalid("Configuration directory is unavailable"))?
                .join(path)
        };
        let parent = path
            .parent()
            .ok_or_else(|| invalid("Invalid configuration path"))?;
        let mut config: Self = serde_json::from_slice(&read_limited(&path, 1024 * 1024).await?)
            .map_err(|_| invalid("Invalid service configuration JSON"))?;
        let absolute = |path: &mut PathBuf| {
            if !path.is_absolute() {
                *path = parent.join(&*path);
            }
        };
        if let StorageConfig::Sqlite { path, .. } = &mut config.storage {
            absolute(path);
        }
        for path in &mut config.workflows {
            absolute(path);
        }
        for profile in &mut config.files {
            absolute(&mut profile.root);
        }
        Ok(config)
    }

    pub async fn boot(
        self,
        observer: Arc<dyn ExecutionObserver>,
    ) -> Result<ServiceRuntime, ForgeError> {
        let identities = self
            .identities
            .into_iter()
            .map(|identity| {
                Ok(BearerIdentity {
                    token: SecretValue::new(environment(&identity.token_env)?),
                    access: AccessContext {
                        scope: self.scope.clone(),
                        actor: identity.actor,
                        permissions: identity.permissions,
                        resources: identity.resources,
                    },
                })
            })
            .collect::<Result<_, ForgeError>>()?;
        let authenticator = Arc::new(StaticBearerAuth::new(identities)?);
        if self.workflows.len() > self.transport.max_prepared
            || self.workflows.len() > 1000
            || self.secrets.len() > 256
        {
            return Err(invalid("Bootstrap resource count exceeded"));
        }
        let mut secrets = modules::MemorySecrets::default();
        for (name, variable) in &self.secrets {
            secrets.insert(&self.scope, name, environment(variable)?);
        }
        let mut builder = WorkflowBuilder::for_scope(&self.scope)
            .limits(self.engine_limits.clone())
            .secret_provider(Arc::new(secrets))
            .observer(observer);
        if let StorageConfig::Sqlite { path, options } = self.storage {
            let store = Arc::new(modules::SqliteExecutionStore::open(path, options)?);
            builder = builder.execution_store(store.clone()).artifact_store(store);
        }
        if !self.http.is_empty() {
            builder.register_bundle(modules::http_json_operations(self.http)?)?;
        }
        if !self.files.is_empty() {
            builder.register_bundle(modules::file_operations(self.files)?)?;
        }
        if let Some(csv) = self.csv {
            builder.register_bundle(modules::csv_operations(csv)?)?;
        }
        let mut definitions = Vec::new();
        let mut remaining = 16 * 1024 * 1024;
        for path in &self.workflows {
            let bytes =
                read_limited(path, self.engine_limits.document_bytes.min(remaining)).await?;
            remaining -= bytes.len();
            definitions.push(
                serde_json::from_slice(&bytes)
                    .map_err(|_| invalid("Invalid bootstrap workflow JSON"))?,
            );
        }
        let options = ServiceOptions {
            listen: self.listen,
            scope: self.scope,
            definitions,
            max_prepared: self.transport.max_prepared,
            max_requests: self.transport.max_requests,
            max_json_bytes: self.transport.max_json_bytes,
            max_response_bytes: self.transport.max_response_bytes,
            max_artifact_bytes: self.transport.max_artifact_bytes,
            request_timeout: Duration::from_millis(self.transport.request_timeout_ms),
            http_shutdown_timeout: Duration::from_millis(self.transport.http_shutdown_timeout_ms),
            engine_shutdown_timeout: Duration::from_millis(
                self.transport.engine_shutdown_timeout_ms,
            ),
        };
        ServiceRuntime::boot(builder.build()?, authenticator, options).await
    }
}
