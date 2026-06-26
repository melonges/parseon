use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("conflict: {0}")]
    #[allow(dead_code)]
    Conflict(String),

    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("abi error: {0}")]
    Abi(#[from] crate::abi::AbiError),

    #[error("rpc error: {0}")]
    Rpc(#[from] alloy::transports::TransportError),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Db(sqlx::Error::RowNotFound) => StatusCode::NOT_FOUND,
            AppError::Db(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                StatusCode::CONFLICT
            }
            AppError::Db(_) | AppError::Rpc(_) | AppError::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            AppError::Abi(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
