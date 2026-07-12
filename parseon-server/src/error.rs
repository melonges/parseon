use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::dto::ErrorResponse;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AppError {
    fn from(error: anyhow::Error) -> Self {
        if parseon_core::services::is_invalid_command(&error) {
            Self::BadRequest(error.to_string())
        } else {
            Self::Internal(error)
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::BadRequest(error) => (StatusCode::BAD_REQUEST, error),
            Self::Internal(error) => {
                tracing::error!(error = %error, "request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".into())
            }
        };
        (status, Json(ErrorResponse { error })).into_response()
    }
}

pub(crate) type AppResult<T> = Result<T, AppError>;
