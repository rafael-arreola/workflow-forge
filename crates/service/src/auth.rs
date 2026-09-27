use axum::http::{HeaderMap, header::AUTHORIZATION};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use workflow_forge::v2::{AccessContext, ForgeError, PortFuture, SecretValue};

/// The host accredits callers; the engine still authorizes every command.
pub trait RequestAuthenticator: Send + Sync {
    fn authenticate<'a>(&'a self, headers: &'a HeaderMap) -> PortFuture<'a, AccessContext>;
}

pub struct BearerIdentity {
    pub token: SecretValue,
    pub access: AccessContext,
}

pub struct StaticBearerAuth {
    identities: Vec<([u8; 32], AccessContext)>,
}

impl StaticBearerAuth {
    pub fn new(identities: Vec<BearerIdentity>) -> Result<Self, ForgeError> {
        if identities.is_empty() || identities.len() > 64 {
            return Err(ForgeError::new(
                "service.config",
                "Expected 1 to 64 bearer identities",
            ));
        }
        let mut registered = Vec::new();
        for identity in identities {
            let token = identity.token.expose().as_bytes();
            if !(32..=4096).contains(&token.len())
                || !token.iter().all(u8::is_ascii_graphic)
                || !valid_identity(&identity.access)
            {
                return Err(ForgeError::new(
                    "service.config",
                    "Invalid bearer identity configuration",
                ));
            }
            let digest: [u8; 32] = Sha256::digest(token).into();
            if registered.iter().any(|(other, _)| other == &digest) {
                return Err(ForgeError::new(
                    "service.config",
                    "Duplicate bearer credential",
                ));
            }
            registered.push((digest, identity.access));
        }
        Ok(Self {
            identities: registered,
        })
    }
}

pub(crate) fn valid_identity(access: &AccessContext) -> bool {
    !access.scope.is_empty()
        && access.scope.len() <= 256
        && !access.actor.is_empty()
        && access.actor.len() <= 256
        && access.resources.len() <= 256
        && access
            .resources
            .iter()
            .all(|r| !r.is_empty() && r.len() <= 256)
}

impl RequestAuthenticator for StaticBearerAuth {
    fn authenticate<'a>(&'a self, headers: &'a HeaderMap) -> PortFuture<'a, AccessContext> {
        Box::pin(async move {
            let denied = || {
                ForgeError::new(
                    "http.unauthenticated",
                    "A valid bearer credential is required",
                )
            };
            let mut values = headers.get_all(AUTHORIZATION).iter();
            let value = values.next().ok_or_else(denied)?;
            if values.next().is_some() {
                return Err(denied());
            }
            let (scheme, token) = value
                .to_str()
                .map_err(|_| denied())?
                .split_once(' ')
                .ok_or_else(denied)?;
            if !scheme.eq_ignore_ascii_case("bearer")
                || !(32..=4096).contains(&token.len())
                || !token.as_bytes().iter().all(u8::is_ascii_graphic)
            {
                return Err(denied());
            }
            let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            let mut found = None;
            for (expected, access) in &self.identities {
                if bool::from(digest.ct_eq(expected)) {
                    found = Some(access.clone());
                }
            }
            found.ok_or_else(denied)
        })
    }
}
