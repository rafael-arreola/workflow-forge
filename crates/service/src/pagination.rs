use crate::{dto::Page, error::ApiError};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use workflow_forge::v2::AccessContext;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    context: String,
    offset: usize,
    revision: Option<u64>,
}

pub(crate) fn page<T>(
    items: Vec<T>,
    query: PageQuery,
    instance: &str,
    access: &AccessContext,
    collection: &str,
    revision: Option<u64>,
) -> Result<Page<T>, ApiError> {
    let invalid = || ApiError::new("http.cursor_invalid", "Invalid cursor");
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::new(
            "http.invalid_request",
            "Page limit must be 1 to 100",
        ));
    }
    let identity = serde_json::to_vec(&(
        instance,
        &access.scope,
        &access.actor,
        &access.permissions,
        &access.resources,
        collection,
    ))
    .expect("identity serializes");
    let context = URL_SAFE_NO_PAD.encode(Sha256::digest(identity));
    let offset = if let Some(value) = query.cursor {
        if value.len() > 1024 {
            return Err(invalid());
        }
        let cursor: Cursor =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value).map_err(|_| invalid())?)
                .map_err(|_| invalid())?;
        if cursor.version != 1 || cursor.context != context {
            return Err(invalid());
        }
        if cursor.revision != revision {
            return Err(ApiError::new(
                "state.conflict",
                "Run revision changed; restart pagination",
            ));
        }
        if cursor.offset > items.len() {
            return Err(invalid());
        }
        cursor.offset
    } else {
        0
    };
    let end = offset.saturating_add(limit).min(items.len());
    let next_cursor = (end < items.len()).then(|| {
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&Cursor {
                version: 1,
                context,
                offset: end,
                revision,
            })
            .expect("cursor serializes"),
        )
    });
    Ok(Page {
        items: items.into_iter().skip(offset).take(limit).collect(),
        next_cursor,
    })
}
