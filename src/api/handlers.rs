use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};

use crate::api::AppState;
use crate::api::dto::{CreateMonitor, ErrorResponse, Health, ResultsQuery, UpdateMonitor};
use crate::db::dyn_table::{self, MonitorResult, SearchParams};
use crate::db::monitor_repo::MonitorRow;
use crate::db::monitor_repo::{self, MonitorInput};
use crate::error::{AppError, AppResult};

// ----- Health -----

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    responses(
        (status = OK, description = "Service is healthy", body = Health),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn healthz(State(state): State<AppState>) -> AppResult<Json<Health>> {
    let monitors = monitor_repo::count(&state.pool).await?;
    Ok(Json(Health {
        status: "ok",
        monitors,
    }))
}

// ----- Monitors -----

#[utoipa::path(
    post,
    path = "/monitors",
    tag = "monitors",
    request_body = CreateMonitor,
    responses(
        (status = OK, description = "Monitor created", body = MonitorRow),
        (status = BAD_REQUEST, description = "Invalid request or ABI signature", body = ErrorResponse),
        (status = CONFLICT, description = "A monitor already exists for the address and selector", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn create_monitor(
    State(state): State<AppState>,
    body: Result<Json<CreateMonitor>, JsonRejection>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let input = MonitorInput {
        address: body.address,
        signature: body.signature,
        start_block: body.start_block,
        end_block: body.end_block,
    };
    let row = monitor_repo::create(&state.pool, &input).await?;
    Ok(Json(row))
}

#[utoipa::path(
    get,
    path = "/monitors",
    tag = "monitors",
    responses(
        (status = OK, description = "Monitors ordered by ID", body = [MonitorRow]),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn list_monitors(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::db::monitor_repo::MonitorRow>>> {
    let rows = monitor_repo::list(&state.pool).await?;
    Ok(Json(rows))
}

#[utoipa::path(
    get,
    path = "/monitors/{id}",
    tag = "monitors",
    params(("id" = i64, Path, description = "Monitor ID")),
    responses(
        (status = OK, description = "Monitor found", body = MonitorRow),
        (status = NOT_FOUND, description = "Monitor not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn get_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let row = monitor_repo::get(&state.pool, id).await?;
    Ok(Json(row))
}

#[utoipa::path(
    patch,
    path = "/monitors/{id}",
    tag = "monitors",
    params(("id" = i64, Path, description = "Monitor ID")),
    request_body = UpdateMonitor,
    responses(
        (status = OK, description = "Monitor updated", body = MonitorRow),
        (status = BAD_REQUEST, description = "Invalid request or block range", body = ErrorResponse),
        (status = NOT_FOUND, description = "Monitor not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn update_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    body: Result<Json<UpdateMonitor>, JsonRejection>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = monitor_repo::update(
        &state.pool,
        id,
        body.start_block,
        body.end_block,
        body.enabled,
    )
    .await?;
    Ok(Json(row))
}

#[utoipa::path(
    delete,
    path = "/monitors/{id}",
    tag = "monitors",
    params(("id" = i64, Path, description = "Monitor ID")),
    responses(
        (status = NO_CONTENT, description = "Monitor deleted"),
        (status = NOT_FOUND, description = "Monitor not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn delete_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::http::StatusCode> {
    monitor_repo::delete(&state.pool, id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ----- Results -----

#[utoipa::path(
    get,
    path = "/monitors/{id}/results",
    tag = "results",
    params(
        ("id" = i64, Path, description = "Monitor ID"),
        ("from_addr" = Option<String>, Query, description = "Filter by sender address (case-insensitive)"),
        ("status" = Option<i16>, Query, description = "Filter by transaction status (1 = success, 0 = reverted)"),
        ("limit" = Option<i64>, Query, description = "Maximum number of results (default 50, max 200)"),
        ("offset" = Option<i64>, Query, description = "Pagination offset (default 0)")
    ),
    responses(
        (status = OK, description = "Decoded results ordered by block_number descending", body = [MonitorResult]),
        (status = BAD_REQUEST, description = "Invalid query parameter", body = ErrorResponse),
        (status = NOT_FOUND, description = "Monitor not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn list_monitor_results(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<ResultsQuery>,
) -> AppResult<Json<Vec<MonitorResult>>> {
    let from_addr = match query.from_addr {
        Some(ref addr) => Some(normalize_addr(addr)?),
        None => None,
    };
    let monitor = monitor_repo::get(&state.pool, id).await?;
    let search = SearchParams {
        from_addr,
        status: query.status,
        limit: query.limit.clamp(1, 200),
        offset: query.offset.max(0),
    };
    let rows = dyn_table::query_results(
        &state.pool,
        &monitor.address,
        &monitor.selector,
        &monitor.param_schema.0,
        &search,
    )
    .await?;
    Ok(Json(rows))
}

/// Validate and normalize an address filter to lowercase hex.
fn normalize_addr(value: &str) -> AppResult<String> {
    use alloy::primitives::Address;
    let address: Address = value
        .parse()
        .map_err(|e| AppError::BadRequest(format!("invalid from_addr: {e}")))?;
    Ok(address.to_string().to_ascii_lowercase())
}
