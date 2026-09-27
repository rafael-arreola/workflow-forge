use crate::{
    error::{Failure, Result, transport},
    input,
};
use futures::StreamExt;
use reqwest::{Method, Response, StatusCode, Url};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use workflow_forge_protocol::*;

#[derive(Clone, Copy)]
pub struct Budgets {
    pub json: usize,
    pub response: usize,
    pub artifact: u64,
}
pub struct Client {
    http: reqwest::Client,
    base: Url,
    token: SecretValue,
    pub budgets: Budgets,
}
impl Client {
    pub fn new(server: &str, token: String, timeout: Duration, budgets: Budgets) -> Result<Self> {
        let base =
            Url::parse(server).map_err(|_| Failure::new("cli.server", "Invalid server URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Failure::new(
                "cli.server",
                "Server must be an HTTP(S) base URL without credentials, query or fragment",
            ));
        }
        if !(32..=4096).contains(&token.len()) || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Failure::new(
                "cli.credentials",
                "Bearer credential is missing or invalid",
            ));
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .build()
            .map_err(transport)?;
        Ok(Self {
            http,
            base,
            token: SecretValue::new(token),
            budgets,
        })
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| Failure::new("cli.server", "Server URL cannot contain path segments"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    fn body(&self, body: &Value) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(body)
            .map_err(|_| Failure::new("cli.json", "Request could not be serialized"))?;
        if bytes.len() > self.budgets.json {
            return Err(Failure::new(
                "cli.body_too_large",
                "Request exceeds the client byte budget",
            ));
        }
        Ok(bytes)
    }

    pub async fn json(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
    ) -> Result<Value> {
        let request = self
            .http
            .request(method, self.url(segments)?)
            .bearer_auth(self.token.expose());
        let request = if let Some(body) = body {
            request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(self.body(&body)?)
        } else {
            request
        };
        self.response(request.send().await.map_err(transport)?)
            .await
    }

    pub async fn run_json(
        &self,
        method: Method,
        id: &RunId,
        tail: &[&str],
        body: Option<Value>,
    ) -> Result<Value> {
        run_id(id)?;
        let mut path = vec!["v2", "runs", &id.0];
        path.extend_from_slice(tail);
        self.json(method, &path, body).await
    }

    pub async fn page(
        &self,
        id: &RunId,
        collection: &str,
        limit: u16,
        cursor: Option<&str>,
    ) -> Result<Value> {
        run_id(id)?;
        if cursor.is_some_and(|value| value.is_empty() || value.len() > 1024) {
            return Err(Failure::new("cli.pagination", "Invalid pagination cursor"));
        }
        let mut url = self.url(&["v2", "runs", &id.0, collection])?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let response = self
            .http
            .get(url)
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(transport)?;
        self.response(response).await
    }

    async fn response(&self, response: Response) -> Result<Value> {
        let status = response.status();
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(transport)?;
            if chunk.len() > self.budgets.response.saturating_sub(bytes.len()) {
                return Err(Failure::new(
                    "cli.response_too_large",
                    "Response exceeds the client byte budget",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let error: ForgeError = input::parse(&bytes)?;
            if error.diagnostics.is_empty() {
                return Err(protocol());
            }
            return Err(error.into());
        }
        input::parse(&bytes)
    }

    pub async fn catalog(&self) -> Result<Value> {
        let mut items = Vec::<OperationDescriptor>::new();
        let mut capabilities = None;
        let mut cursor = None::<String>;
        let mut seen = BTreeSet::new();
        let mut bytes = 0usize;
        for _ in 0..1000 {
            let mut request = self
                .http
                .get(self.url(&["v2", "catalog"])?)
                .bearer_auth(self.token.expose());
            if let Some(cursor) = &cursor {
                request = request.query(&[("cursor", cursor)]);
            }
            let page = self
                .response(request.send().await.map_err(transport)?)
                .await?;
            bytes = bytes.saturating_add(serde_json::to_vec(&page).map_err(|_| protocol())?.len());
            if bytes > self.budgets.response {
                return Err(Failure::new(
                    "cli.response_too_large",
                    "Catalog exceeds the aggregate byte budget",
                ));
            }
            let current: ApplicationCapabilities =
                serde_json::from_value(page.get("capabilities").cloned().ok_or_else(protocol)?)
                    .map_err(|_| protocol())?;
            let current = serde_json::to_value(current).map_err(|_| protocol())?;
            if capabilities.as_ref().is_some_and(|old| old != &current) {
                return Err(Failure::new(
                    "state.conflict",
                    "Catalog capabilities changed between pages",
                ));
            }
            capabilities = Some(current);
            let mut page_items: Vec<OperationDescriptor> =
                serde_json::from_value(page.get("items").cloned().ok_or_else(protocol)?)
                    .map_err(|_| protocol())?;
            items.append(&mut page_items);
            match page.get("next_cursor") {
                Some(Value::Null) => {
                    return Ok(
                        json!({"capabilities":capabilities,"items":items,"next_cursor":null}),
                    );
                }
                Some(Value::String(next))
                    if !next.is_empty() && next.len() <= 1024 && seen.insert(next.clone()) =>
                {
                    cursor = Some(next.clone())
                }
                _ => return Err(protocol()),
            }
        }
        Err(Failure::new(
            "cli.pagination",
            "Catalog exceeded its page budget",
        ))
    }

    pub async fn prepare(&self, definition: WorkflowDefinition) -> Result<Value> {
        let expected = WorkflowRevision {
            id: definition.id.clone(),
            revision: definition.revision.clone(),
        };
        let value = self
            .json(
                Method::POST,
                &["v2", "workflows", "prepare"],
                Some(json!({"definition":definition})),
            )
            .await?;
        let actual: WorkflowRevision =
            serde_json::from_value(value.get("workflow").cloned().ok_or_else(protocol)?)
                .map_err(|_| protocol())?;
        if actual != expected {
            return Err(protocol());
        }
        Ok(value)
    }

    pub async fn start(
        &self,
        workflow: WorkflowRevision,
        input: Value,
        options: StartOptions,
    ) -> Result<StartReceipt> {
        let value = self
            .json(
                Method::POST,
                &["v2", "runs"],
                Some(json!({"workflow":workflow,"input":input,"options":options})),
            )
            .await?;
        let receipt: StartReceipt = serde_json::from_value(value).map_err(|_| protocol())?;
        if options.require_durable && !receipt.durable {
            return Err(protocol());
        }
        Ok(receipt)
    }

    pub async fn wait(&self, id: &RunId, duration: Duration, interval: Duration) -> Result<Value> {
        let work = async {
            loop {
                let status = self.run_json(Method::GET, id, &[], None).await?;
                if status.get("id") != Some(&json!(id)) {
                    return Err(protocol());
                }
                let state: RunState =
                    serde_json::from_value(status.get("state").cloned().ok_or_else(protocol)?)
                        .map_err(|_| protocol())?;
                match state {
                    RunState::Blocked => {
                        return Err(Failure::pending(
                            "cli.run_blocked",
                            "Run requires intervention; consult its status",
                        ));
                    }
                    RunState::Failed | RunState::Cancelled => {
                        if let Some(error) = status.get("error").filter(|v| !v.is_null()) {
                            return Err(serde_json::from_value::<ForgeError>(error.clone())
                                .map_err(|_| protocol())?
                                .into());
                        }
                        return Err(Failure::new(
                            "cli.run_failed",
                            "Run ended without a successful result",
                        ));
                    }
                    RunState::Succeeded => {
                        let value = self.run_json(Method::GET, id, &["result"], None).await?;
                        if value.get("run_id") != Some(&json!(id)) {
                            return Err(protocol());
                        }
                        return value.get("output").cloned().ok_or_else(protocol);
                    }
                    _ => tokio::time::sleep(interval).await,
                }
            }
        };
        tokio::time::timeout(duration, work).await.map_err(|_| {
            Failure::pending(
                "cli.wait_expired",
                "Client wait expired; the run was not cancelled",
            )
        })?
    }

    pub async fn upload(&self, path: &Path, media_type: &str) -> Result<ArtifactRef> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|_| Failure::new("cli.file", "Upload file is unavailable"))?;
        let metadata = file.metadata().await.map_err(transport)?;
        if !metadata.is_file() {
            return Err(Failure::new("cli.file", "Upload requires a regular file"));
        }
        if metadata.len() > self.budgets.artifact {
            return Err(Failure::new(
                "cli.body_too_large",
                "Artifact exceeds the client byte budget",
            ));
        }
        let bytes = Arc::new(AtomicU64::new(0));
        let counter = bytes.clone();
        let limit = self.budgets.artifact;
        let stream =
            tokio_util::io::ReaderStream::with_capacity(file, 64 * 1024).map(move |chunk| {
                let chunk = chunk?;
                let previous = counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                if chunk.len() as u64 > limit.saturating_sub(previous) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Artifact exceeds client budget",
                    ));
                }
                Ok(chunk)
            });
        let response = self
            .http
            .post(self.url(&["v2", "artifacts"])?)
            .bearer_auth(self.token.expose())
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
            .map_err(transport)?;
        let value = self.response(response).await?;
        let reference: ArtifactRef = serde_json::from_value(value).map_err(|_| protocol())?;
        if reference.bytes != bytes.load(Ordering::Relaxed)
            || reference.bytes > limit
            || reference.media_type != media_type
        {
            return Err(protocol());
        }
        Ok(reference)
    }

    pub async fn download(&self, reference: ArtifactRef, output: &Path) -> Result<()> {
        if reference.bytes > self.budgets.artifact {
            return Err(Failure::new(
                "cli.body_too_large",
                "Artifact exceeds the client byte budget",
            ));
        }
        let response = self
            .http
            .post(self.url(&["v2", "artifacts", "read"])?)
            .bearer_auth(self.token.expose())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(self.body(&json!({"artifact":reference}))?)
            .send()
            .await
            .map_err(transport)?;
        if response.status() != StatusCode::OK {
            self.response(response).await?;
            return Err(protocol());
        }
        if response
            .content_length()
            .is_some_and(|length| length != reference.bytes)
        {
            return Err(protocol());
        }
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary =
            Temporary(parent.join(format!(".forge-download-{}.part", uuid::Uuid::now_v7())));
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary.0)
            .await
            .map_err(|_| Failure::new("cli.file", "Download directory is unavailable"))?;
        let mut stream = response.bytes_stream();
        let mut received = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(transport)?;
            if chunk.len() as u64 > reference.bytes.saturating_sub(received) {
                return Err(protocol());
            }
            received += chunk.len() as u64;
            file.write_all(&chunk)
                .await
                .map_err(|_| Failure::new("cli.file", "Download could not be written"))?;
        }
        if received != reference.bytes {
            return Err(protocol());
        }
        file.sync_all()
            .await
            .map_err(|_| Failure::new("cli.file", "Download could not be synchronized"))?;
        drop(file);
        // Same-filesystem publication fails if output already exists. No overwrite
        // race from checking existence before a rename, and no visible partial file.
        tokio::fs::hard_link(&temporary.0, output)
            .await
            .map_err(|_| {
                Failure::new(
                    "cli.file",
                    "Cannot publish download; destination must be new",
                )
            })?;
        Ok(())
    }
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn protocol() -> Failure {
    Failure::new(
        "cli.protocol",
        "Server response does not match the HTTP contract",
    )
}
fn run_id(id: &RunId) -> Result<()> {
    if id.0.is_empty()
        || id.0.len() > 256
        || !id.0.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(Failure::new("cli.run_id", "Invalid run identifier"));
    }
    Ok(())
}
