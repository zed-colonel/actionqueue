//! Host-provided authentication hook and trusted extractor. No body field can
//! supply this extension. Authentication runs before body extraction.
use std::sync::Arc;

use actionqueue_core::control::HostControlContext;
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

use super::RouterState;
/// Hook implemented by the daemon host (token/session validation is not core policy).
pub type HostAuthenticator = Arc<
    dyn Fn(&HeaderMap, &axum::http::Uri) -> Result<HostControlContext, AuthenticationError>
        + Send
        + Sync,
>;
/// The host could not authenticate this request.
#[derive(Debug, Clone, Copy)]
pub struct AuthenticationError;
/// Authentication middleware for actor/platform/control routes.
pub async fn authenticate(
    State(state): State<RouterState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !state.router_config.control_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    super::maintenance::start(&state);
    let Some(hook) = &state.host_authenticator else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let host = match hook(request.headers(), request.uri()) {
        Ok(host) => host,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    request.extensions_mut().insert(host);
    next.run(request).await
}

/// Authentication for inspection is independent of the control-enable switch.
/// Every store profile fails closed when no host hook is configured.
pub async fn authenticate_inspection(
    State(state): State<RouterState>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(hook) = &state.host_authenticator {
        match hook(request.headers(), request.uri()) {
            Ok(host) => {
                request.extensions_mut().insert(host);
            }
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}
/// Builds a trusted bearer hook from operator-owned configuration bytes. This is
/// configuration ingress, never an HTTP request body. Each token binds exactly
/// one principal/scope. Queue permissions are still checked for every operation.
pub fn bearer_authenticator(bytes: &[u8]) -> Result<HostAuthenticator, AuthenticationError> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        token: String,
        actor_id: Option<actionqueue_core::ids::ActorId>,
        scope: actionqueue_core::control::ControlScope,
        attribution: actionqueue_core::causal::ControlMutationContext,
    }
    let identities: Vec<Identity> =
        serde_json::from_slice(bytes).map_err(|_| AuthenticationError)?;
    if identities.is_empty()
        || identities
            .iter()
            .any(|i| i.token.len() < 32 || !i.token.bytes().all(|b| b.is_ascii_graphic()))
    {
        return Err(AuthenticationError);
    }
    let mut tokens = std::collections::HashSet::new();
    if identities.iter().any(|i| !tokens.insert(&i.token)) {
        return Err(AuthenticationError);
    }
    Ok(Arc::new(move |headers, _uri| {
        let bearer = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or(AuthenticationError)?;
        let identity = identities
            .iter()
            .find(|i| {
                let mismatch = i
                    .token
                    .as_bytes()
                    .iter()
                    .zip(bearer.as_bytes())
                    .fold(0u8, |n, (a, b)| n | (a ^ b));
                i.token.len() == bearer.len() && mismatch == 0
            })
            .ok_or(AuthenticationError)?;
        Ok(HostControlContext {
            actor_id: identity.actor_id,
            scope: identity.scope,
            attribution: identity.attribution.clone(),
        })
    }))
}
