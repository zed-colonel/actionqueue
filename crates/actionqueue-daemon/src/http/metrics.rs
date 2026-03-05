//! Metrics route module.
//!
//! This module provides `GET /metrics`, which exports daemon-local Prometheus
//! metrics in text exposition format.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;

/// Handles `GET /metrics` export requests.
#[tracing::instrument(skip_all)]
pub async fn handle(state: State<super::RouterState>) -> impl IntoResponse {
    crate::metrics::runs::update(state.0.as_ref());
    crate::metrics::attempts::update(state.0.as_ref());
    crate::metrics::wal::update(state.0.as_ref());
    crate::metrics::recovery::update(state.0.as_ref());

    match state.metrics.encode_text() {
        Ok(encoded) => {
            let mut response = encoded.body.into_response();
            if let Ok(value) = HeaderValue::from_str(&encoded.content_type) {
                response.headers_mut().insert(axum::http::header::CONTENT_TYPE, value);
            }
            response
        }
        Err(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics_encode_failed: failed to encode metrics\n")
                .into_response()
        }
    }
}

/// Registers metrics routes when enabled.
pub fn register_routes(
    router: axum::Router<super::RouterState>,
    metrics_enabled: bool,
) -> axum::Router<super::RouterState> {
    if !metrics_enabled {
        return router;
    }

    router.route("/metrics", axum::routing::get(handle))
}
