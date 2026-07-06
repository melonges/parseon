use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};

use crate::api::AppState;
use crate::api::dto::{
    ChainRow, CreateChain, CreateMonitor, ErrorResponse, Health, MonitorResult, MonitorRow,
    ResultsQuery, Status, UpdateChain, UpdateMonitor,
};
use crate::core::Chain;
use crate::db::dyn_table::SearchParams;
use crate::db::monitor_repo::MonitorInput;
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
    let monitors = state.storage.monitor_count().await?;
    Ok(Json(Health {
        status: "ok",
        monitors,
    }))
}

#[utoipa::path(
    get,
    path = "/status",
    tag = "health",
    responses(
        (status = OK, description = "Finalized indexing progress and worker state", body = Status)
    )
)]
pub async fn status(State(state): State<AppState>) -> Json<Status> {
    Json(Status {
        mode: "finalized",
        chains: state
            .runtime_status
            .snapshot()
            .into_iter()
            .map(Into::into)
            .collect(),
    })
}

// ----- Chains -----

#[utoipa::path(
    post,
    path = "/chains",
    tag = "chains",
    request_body = CreateChain,
    responses(
        (status = CREATED, description = "Chain registered", body = ChainRow),
        (status = BAD_REQUEST, description = "Invalid RPC endpoint or finalized support", body = ErrorResponse),
        (status = CONFLICT, description = "Chain already registered", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn create_chain(
    State(state): State<AppState>,
    body: Result<Json<CreateChain>, JsonRejection>,
) -> AppResult<(axum::http::StatusCode, Json<ChainRow>)> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let (chain, _) = crate::core::supervisor::validate_source(
        state.source_factory.as_ref(),
        &body.rpc_url,
        None,
    )
    .await
    .map_err(|message| AppError::BadRequest(message.to_string()))?;
    let row = state
        .storage
        .create_chain(chain, &body.rpc_url, body.enabled)
        .await?;
    Ok((axum::http::StatusCode::CREATED, Json(row.into())))
}

#[utoipa::path(
    get,
    path = "/chains",
    tag = "chains",
    responses(
        (status = OK, description = "Registered chains ordered by chain ID", body = [ChainRow]),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn list_chains(State(state): State<AppState>) -> AppResult<Json<Vec<ChainRow>>> {
    let rows = state.storage.list_chain_records().await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    get,
    path = "/chains/{chain_id}",
    tag = "chains",
    params(("chain_id" = i64, Path, description = "EIP-155 chain ID")),
    responses(
        (status = OK, description = "Chain found", body = ChainRow),
        (status = NOT_FOUND, description = "Chain not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn get_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<i64>,
) -> AppResult<Json<ChainRow>> {
    Ok(Json(state.storage.get_chain(chain_id).await?.into()))
}

#[utoipa::path(
    patch,
    path = "/chains/{chain_id}",
    tag = "chains",
    params(("chain_id" = i64, Path, description = "EIP-155 chain ID")),
    request_body = UpdateChain,
    responses(
        (status = OK, description = "Chain updated", body = ChainRow),
        (status = BAD_REQUEST, description = "Invalid update or RPC endpoint", body = ErrorResponse),
        (status = NOT_FOUND, description = "Chain not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn update_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<i64>,
    body: Result<Json<UpdateChain>, JsonRejection>,
) -> AppResult<Json<ChainRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    if body.rpc_url.is_none() && body.enabled.is_none() {
        return Err(AppError::BadRequest(
            "at least one of rpc_url or enabled is required".to_string(),
        ));
    }
    state.storage.get_chain(chain_id).await?;
    if let Some(rpc_url) = body.rpc_url.as_deref() {
        let expected = Chain::new(chain_id).map_err(AppError::Internal)?;
        crate::core::supervisor::validate_source(
            state.source_factory.as_ref(),
            rpc_url,
            Some(expected),
        )
        .await
        .map_err(|message| AppError::BadRequest(message.to_string()))?;
    }
    let row = state
        .storage
        .update_chain(chain_id, body.rpc_url.as_deref(), body.enabled)
        .await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    delete,
    path = "/chains/{chain_id}",
    tag = "chains",
    params(("chain_id" = i64, Path, description = "EIP-155 chain ID")),
    responses(
        (status = NO_CONTENT, description = "Chain, monitors, and result tables deleted"),
        (status = NOT_FOUND, description = "Chain not found", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn delete_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<i64>,
) -> AppResult<axum::http::StatusCode> {
    state.storage.delete_chain(chain_id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
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
        (status = CONFLICT, description = "A monitor already exists for the kind, address, and signature hash", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn create_monitor(
    State(state): State<AppState>,
    body: Result<Json<CreateMonitor>, JsonRejection>,
) -> AppResult<Json<MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let input = MonitorInput {
        chain_id: body.chain_id,
        address: body.address,
        signature: body.signature,
        start_block: body.start_block,
        end_block: body.end_block,
    };
    let row = state.storage.create_monitor(&input).await?;
    Ok(Json(row.into()))
}

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct MonitorListQuery {
    pub chain_id: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/monitors",
    tag = "monitors",
    params(MonitorListQuery),
    responses(
        (status = OK, description = "Monitors ordered by ID", body = [MonitorRow]),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub async fn list_monitors(
    State(state): State<AppState>,
    Query(query): Query<MonitorListQuery>,
) -> AppResult<Json<Vec<MonitorRow>>> {
    let rows = state.storage.list_monitor_records(query.chain_id).await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
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
) -> AppResult<Json<MonitorRow>> {
    let row = state.storage.get_monitor(id).await?;
    Ok(Json(row.into()))
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
) -> AppResult<Json<MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = state
        .storage
        .update_monitor(id, body.start_block, body.end_block, body.enabled)
        .await?;
    Ok(Json(row.into()))
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
    state.storage.delete_monitor(id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ----- Results -----

#[utoipa::path(
    get,
    path = "/monitors/{id}/results",
    tag = "results",
    params(
        ("id" = i64, Path, description = "Monitor ID"),
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
    let monitor = state.storage.get_monitor(id).await?;
    let search = SearchParams {
        limit: query.limit.clamp(1, 200),
        offset: query.offset.max(0),
    };
    let rows = state.storage.query_results(&monitor, &search).await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}
