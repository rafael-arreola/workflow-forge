use super::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileReadProfile {
    pub name: String,
    pub root: PathBuf,
    pub max_bytes: usize,
    pub media_type: String,
}
impl Default for FileReadProfile {
    fn default() -> Self {
        Self {
            name: String::new(),
            root: PathBuf::new(),
            max_bytes: 4 * 1024 * 1024,
            media_type: "application/octet-stream".into(),
        }
    }
}
struct FileRead {
    profile: FileReadProfile,
    descriptor: OperationDescriptor,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    path: String,
}

pub fn file_operations(profiles: Vec<FileReadProfile>) -> Result<OperationBundle, ForgeError> {
    if profiles.is_empty() || profiles.len() > 256 {
        return Err(configured("Expected 1 to 256 file profiles"));
    }
    let mut operations: Vec<Arc<dyn Operation>> = Vec::new();
    for profile in profiles {
        if !name_valid(&profile.name)
            || !profile.root.is_absolute()
            || profile.root.to_str().is_none()
            || !(1..=64 * 1024 * 1024).contains(&profile.max_bytes)
            || profile.media_type.is_empty()
            || profile.media_type.len() > 256
            || profile
                .media_type
                .bytes()
                .any(|b| b.is_ascii_control() || !b.is_ascii())
        {
            return Err(configured("File profile violates its resource policy"));
        }
        let descriptor = OperationDescriptor {
            revision: OperationRevision::new(
                &format!("forge.files.{}.read", profile.name),
                "1",
                &revision(&profile)?,
            ),
            schema_dialect: SCHEMA_DIALECT.into(),
            config_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","additionalProperties":false}),
            input_schema: json!({"$schema":SCHEMA_DIALECT,"type":"object","required":["path"],"properties":{"path":{"type":"string","minLength":1,"maxLength":1024}},"additionalProperties":false}),
            output_schema: artifact_schema(),
            effect: EffectKind::Read,
            repetition: Repetition::Safe,
            reconciliation: false,
            required_resources: BTreeSet::from(["artifacts".into()]),
            description: "Read a regular file within a host-configured directory into an artifact"
                .into(),
            examples: vec![json!({"input":{"path":"input.csv"}})],
        };
        operations.push(Arc::new(FileRead {
            profile,
            descriptor,
        }));
    }
    Ok(bundle("forge.files", operations))
}

impl Operation for FileRead {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(
        &'a self,
        context: OperationContext,
        invocation: Invocation,
    ) -> OperationFuture<'a> {
        Box::pin(async move {
            let input: Input =
                serde_json::from_value(invocation.input).map_err(|_| input_error())?;
            let path = Path::new(&input.path);
            if input.path.is_empty()
                || input.path.len() > 1024
                || path
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
            {
                return Err(input_error());
            }
            if context.cancellation.is_cancelled() {
                return Err(failure(
                    "operation.cancelled",
                    ErrorClass::Cancelled,
                    EffectCertainty::NotApplied,
                    "File read cancelled",
                ));
            }
            let root = self.profile.root.clone();
            let max = self.profile.max_bytes;
            // The host provides a local root. Capability-relative open resolves
            // symlinks without giving a workflow ambient filesystem authority.
            let file = tokio::task::spawn_blocking(move || {
                let directory =
                    cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())
                        .map_err(|_| {
                            failure(
                                "resource.unavailable",
                                ErrorClass::Resource,
                                EffectCertainty::NotApplied,
                                "File root is unavailable",
                            )
                        })?;
                let metadata = directory.metadata(&input.path).map_err(|_| input_error())?;
                if !metadata.is_file() {
                    return Err(input_error());
                }
                if metadata.len() > max as u64 {
                    return Err(budget_error());
                }
                let file = directory.open(&input.path).map_err(|_| input_error())?;
                if !file.metadata().map_err(|_| input_error())?.is_file() {
                    return Err(input_error());
                }
                Ok(file.into_std())
            })
            .await
            .map_err(|_| {
                failure(
                    "file.failed",
                    ErrorClass::Internal,
                    EffectCertainty::NotApplied,
                    "File worker failed",
                )
            })??;
            let cancel = context.cancellation.clone();
            let stream = futures::stream::try_unfold(
                (tokio::fs::File::from_std(file), max),
                move |(mut file, remaining)| {
                    let cancel = cancel.clone();
                    async move {
                        let mut buffer = vec![0u8; (64 * 1024).min(remaining.saturating_add(1))];
                        let n = tokio::select! {
                            biased;
                            _=cancel.cancelled()=>return Err(ForgeError::new("operation.cancelled","File read cancelled")),
                            result=file.read(&mut buffer)=>result.map_err(|_|ForgeError::new("file.failed","File read failed"))?,
                        };
                        if n == 0 {
                            return Ok(None);
                        }
                        if n > remaining {
                            return Err(ForgeError::new(
                                "resource.limit",
                                "File exceeds its byte budget",
                            ));
                        }
                        buffer.truncate(n);
                        Ok(Some((buffer, (file, remaining - n))))
                    }
                },
            );
            let artifact = context
                .write_artifact(Box::pin(stream), &self.profile.media_type)
                .await
                .map_err(|error| {
                    failure(
                        error.code(),
                        if error.code() == "operation.cancelled" {
                            ErrorClass::Cancelled
                        } else {
                            ErrorClass::Resource
                        },
                        EffectCertainty::NotApplied,
                        "File artifact could not be published",
                    )
                })?;
            Ok(OperationOutput::json(
                serde_json::to_value(artifact).expect("artifact reference serializes"),
            ))
        })
    }
}
