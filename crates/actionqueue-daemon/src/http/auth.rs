//! Host-provided authentication hook and trusted extractor. No body field can
//! supply this extension. Authentication runs before body extraction.
use super::RouterState;
use actionqueue_core::control::HostControlContext;
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;
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
