use super::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
    Put,
    Patch,
    Delete,
}
impl HttpMethod {
    fn method(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Put => reqwest::Method::PUT,
            Self::Patch => reqwest::Method::PATCH,
            Self::Delete => reqwest::Method::DELETE,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpJsonProfile {
    pub name: String,
    pub url: String,
    pub method: HttpMethod,
    pub bearer_secret: Option<String>,
    pub timeout_ms: u64,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
}
impl Default for HttpJsonProfile {
    fn default() -> Self {
        Self {
            name: String::new(),
            url: String::new(),
            method: HttpMethod::Get,
            bearer_secret: None,
            timeout_ms: 10_000,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
        }
    }
}
struct HttpJson {
    descriptor: OperationDescriptor,
    profile: HttpJsonProfile,
    url: reqwest::Url,
    client: reqwest::Client,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    #[serde(default)]
    query: BTreeMap<String, String>,
    #[serde(default, rename = "body")]
    _body: Option<Value>,
}

pub fn http_json_operations(profiles: Vec<HttpJsonProfile>) -> Result<OperationBundle, ForgeError> {
    if profiles.is_empty() || profiles.len() > 256 {
        return Err(configured("Expected 1 to 256 HTTP profiles"));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .map_err(|_| configured("HTTP client could not be configured"))?;
    let mut operations: Vec<Arc<dyn Operation>> = Vec::new();
    for profile in profiles {
        let url = reqwest::Url::parse(&profile.url)
            .map_err(|_| configured("Invalid HTTP profile URL"))?;
        if !name_valid(&profile.name)
            || !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || profile.url.len() > 4096
            || !(1..=60_000).contains(&profile.timeout_ms)
            || !(1..=64 * 1024 * 1024).contains(&profile.max_request_bytes)
            || !(1..=64 * 1024 * 1024).contains(&profile.max_response_bytes)
            || profile
                .bearer_secret
                .as_ref()
                .is_some_and(|name| !name_valid(name))
        {
            return Err(configured("HTTP profile violates its resource policy"));
        }
        let read = profile.method == HttpMethod::Get;
        let mut properties = json!({"query":{"type":"object","maxProperties":64,"propertyNames":{"minLength":1,"maxLength":128},"additionalProperties":{"type":"string","maxLength":4096}}});
        if !read {
            properties["body"] = json!(true);
        }
        let descriptor = OperationDescriptor {
            revision: OperationRevision::new(
                &format!("forge.http.{}", profile.name),
                "1",
                &revision(&profile)?,
            ),
            schema_dialect: SCHEMA_DIALECT.into(),
            config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
            input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","properties":properties,"additionalProperties":false}),
            output_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["status","body"],"properties":{"status":{"type":"integer","minimum":200,"maximum":299},"body":true},"additionalProperties":false}),
            effect: if read {
                EffectKind::Read
            } else {
                EffectKind::Write
            },
            repetition: if read {
                Repetition::Safe
            } else {
                Repetition::Unsafe
            },
            reconciliation: false,
            required_resources: profile
                .bearer_secret
                .iter()
                .map(|name| format!("secret:{name}"))
                .collect(),
            description: "Call a host-configured JSON endpoint".into(),
            examples: vec![json!({"input":{"query":{}}})],
        };
        operations.push(Arc::new(HttpJson {
            descriptor,
            profile,
            url,
            client: client.clone(),
        }));
    }
    Ok(bundle("forge.http", operations))
}

impl HttpJson {
    fn certainty(&self, dispatched: bool) -> EffectCertainty {
        if dispatched && self.profile.method != HttpMethod::Get {
            EffectCertainty::Unknown
        } else {
            EffectCertainty::NotApplied
        }
    }
    async fn request(
        &self,
        context: &OperationContext,
        invocation: Invocation,
        dispatched: &AtomicBool,
    ) -> Result<OperationOutput, OperationError> {
        // Preserve the distinction between an absent body and a JSON null body.
        let body = invocation.input.get("body").cloned();
        let input: Input = serde_json::from_value(invocation.input).map_err(|_| input_error())?;
        if (self.profile.method == HttpMethod::Get && body.is_some())
            || input.query.len() > 64
            || input
                .query
                .iter()
                .any(|(k, v)| k.is_empty() || k.len() > 128 || v.len() > 4096)
        {
            return Err(input_error());
        }
        let mut request = self
            .client
            .request(self.profile.method.method(), self.url.clone())
            .query(&input.query)
            .timeout(Duration::from_millis(self.profile.timeout_ms))
            .header(reqwest::header::ACCEPT, "application/json");
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(json_bytes(&body, self.profile.max_request_bytes)?);
        }
        if let Some(secret) = &self.profile.bearer_secret {
            let value = context.secret(secret).await.map_err(|_| {
                failure(
                    "resource.unavailable",
                    ErrorClass::Resource,
                    EffectCertainty::NotApplied,
                    "Required HTTP credential is unavailable",
                )
            })?;
            let token = value.expose();
            if token.is_empty()
                || token.len() > 4096
                || !token.bytes().all(|b| b.is_ascii_graphic())
            {
                return Err(failure(
                    "resource.unavailable",
                    ErrorClass::Resource,
                    EffectCertainty::NotApplied,
                    "HTTP credential is invalid",
                ));
            }
            request = request.bearer_auth(token);
        }
        if context.cancellation.is_cancelled() {
            return Err(failure(
                "operation.cancelled",
                ErrorClass::Cancelled,
                EffectCertainty::NotApplied,
                "Operation cancelled before dispatch",
            ));
        }
        let request = request.build().map_err(|_| input_error())?;
        dispatched.store(true, Ordering::Release);
        let response = self.client.execute(request).await.map_err(|_| {
            failure(
                "http.transport",
                ErrorClass::Transient,
                self.certainty(true),
                "HTTP request failed after dispatch",
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let class =
                if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    ErrorClass::Transient
                } else {
                    ErrorClass::Rejected
                };
            return Err(failure(
                "http.status",
                class,
                self.certainty(true),
                "Endpoint returned an unsuccessful status",
            ));
        }
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(OperationOutput::json(json!({"status":204,"body":null})));
        }
        let json_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                let media = v
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                media == "application/json"
                    || (media.starts_with("application/") && media.ends_with("+json"))
            });
        if !json_type {
            return Err(failure(
                "http.content_type",
                ErrorClass::Rejected,
                self.certainty(true),
                "Endpoint did not return JSON",
            ));
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                failure(
                    "http.transport",
                    ErrorClass::Transient,
                    self.certainty(true),
                    "HTTP response stream failed",
                )
            })?;
            if chunk.len() > self.profile.max_response_bytes.saturating_sub(bytes.len()) {
                return Err(failure(
                    "resource.limit",
                    ErrorClass::Resource,
                    self.certainty(true),
                    "HTTP response byte budget exceeded",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            failure(
                "http.invalid_json",
                ErrorClass::Rejected,
                self.certainty(true),
                "Endpoint returned invalid JSON",
            )
        })?;
        Ok(OperationOutput::json(
            json!({"status":status.as_u16(),"body":value}),
        ))
    }
}
impl Operation for HttpJson {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        Box::pin(async move {
            let dispatched = AtomicBool::new(false);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64;
            let timeout = Duration::from_millis(
                self.profile
                    .timeout_ms
                    .min(context.deadline_at_ms.saturating_sub(now)),
            );
            tokio::select! {
                biased;
                _=context.cancellation.cancelled()=>Err(failure("operation.cancelled",ErrorClass::Cancelled,self.certainty(dispatched.load(Ordering::Acquire)),"HTTP operation cancelled")),
                _=tokio::time::sleep(timeout)=>Err(failure("http.timeout",ErrorClass::Transient,self.certainty(dispatched.load(Ordering::Acquire)),"HTTP operation deadline exceeded")),
                outcome=self.request(&context,invocation,&dispatched)=>outcome,
            }
        })
    }
}
