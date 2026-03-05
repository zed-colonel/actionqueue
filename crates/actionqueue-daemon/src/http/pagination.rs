//! Shared pagination types and parsing for list endpoints.
//!
//! This module provides the common pagination types (`Pagination`, `PaginationError`,
//! `PaginationErrorResponse`, `PaginationErrorDetails`) and parsing functions
//! (`parse_pagination`, `parse_limit`, `parse_offset`, `parse_non_negative`,
//! `pagination_error`) used by both the tasks list and runs list handlers.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Default limit for list pagination.
pub const DEFAULT_LIMIT: usize = 100;
/// Maximum allowed limit for list pagination.
pub const MAX_LIMIT: usize = 1000;

/// Parsed pagination parameters.
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    /// Maximum number of items to return.
    pub limit: usize,
    /// Number of items to skip before returning results.
    pub offset: usize,
}

/// Pagination parsing error.
#[derive(Debug, Clone)]
pub struct PaginationError {
    /// The field that caused the error.
    pub field: &'static str,
    /// Human-readable error message.
    pub message: String,
}

impl PaginationError {
    /// Creates a new pagination error.
    pub fn new(field: &'static str, message: impl Into<String>) -> Self {
        Self { field, message: message.into() }
    }
}

/// Pagination error response payload.
#[derive(Debug, Clone, Serialize)]
pub struct PaginationErrorResponse {
    /// Error type identifier.
    pub error: &'static str,
    /// Human-readable error message.
    pub message: String,
    /// Structured error details.
    pub details: PaginationErrorDetails,
}

/// Structured pagination error details.
#[derive(Debug, Clone, Serialize)]
pub struct PaginationErrorDetails {
    /// The field that caused the error.
    pub field: &'static str,
}

/// Parses raw query string into validated pagination parameters.
pub fn parse_pagination(raw_query: Option<&str>) -> Result<Pagination, PaginationError> {
    let mut limit = None;
    let mut offset = None;

    if let Some(query) = raw_query {
        for segment in query.split('&') {
            if segment.is_empty() {
                continue;
            }

            let mut parts = segment.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");

            match key {
                "limit" => {
                    limit = Some(parse_limit(value)?);
                }
                "offset" => {
                    offset = Some(parse_offset(value)?);
                }
                _ => {}
            }
        }
    }

    Ok(Pagination { limit: limit.unwrap_or(DEFAULT_LIMIT), offset: offset.unwrap_or(0) })
}

/// Parses and validates the limit parameter.
pub fn parse_limit(value: &str) -> Result<usize, PaginationError> {
    let parsed = parse_non_negative(value, "limit")?;
    if parsed == 0 || parsed > MAX_LIMIT {
        return Err(PaginationError::new(
            "limit",
            format!("limit must be between 1 and {MAX_LIMIT}"),
        ));
    }
    Ok(parsed)
}

/// Parses and validates the offset parameter.
pub fn parse_offset(value: &str) -> Result<usize, PaginationError> {
    parse_non_negative(value, "offset")
}

/// Parses a non-negative integer value from a query parameter string.
pub fn parse_non_negative(value: &str, field: &'static str) -> Result<usize, PaginationError> {
    if value.is_empty() {
        return Err(PaginationError::new(field, format!("{field} must be a non-negative integer")));
    }

    value
        .parse::<usize>()
        .map_err(|_| PaginationError::new(field, format!("{field} must be a non-negative integer")))
}

/// Converts a pagination error into an HTTP 422 Unprocessable Entity response.
pub fn pagination_error(error: PaginationError) -> impl IntoResponse {
    let payload = PaginationErrorResponse {
        error: "invalid_pagination",
        message: error.message,
        details: PaginationErrorDetails { field: error.field },
    };

    (StatusCode::UNPROCESSABLE_ENTITY, Json(payload))
}
