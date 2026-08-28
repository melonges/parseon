use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::dto::{
    ChainRow, CreateChain, CreateMonitor, ErrorResponse, FilterPreviewRequest,
    FilterPreviewResponse, Health, MonitorResult, MonitorRow, Readiness, ResultsQuery, Status,
    UpdateChain, UpdateMonitor,
};
use crate::error::{AppError, AppResult};
use parseon_core::commands::{
    CreateChain as CreateChainCommand, CreateMonitor as CreateMonitorCommand, PageLimit,
    PreviewFilter as PreviewFilterCommand, ResultQuery, UpdateChain as UpdateChainCommand,
};
use parseon_core::{Finality, MonitorId};

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    responses(
        (status = OK, description = "Process liveness", body = Health)
    )
)]
pub(crate) async fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    responses(
        (status = OK, description = "Storage and enabled workers are ready", body = Readiness),
        (status = SERVICE_UNAVAILABLE, description = "Storage or an enabled worker is not ready", body = Readiness)
    )
)]
pub(crate) async fn readyz(
    State(state): State<AppState>,
) -> (axum::http::StatusCode, Json<Readiness>) {
    if state.monitors.count().await.is_err() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(Readiness { status: "not_ready", reason: Some("storage unavailable") }),
        );
    }
    let max_age =
        chrono::Duration::from_std(state.readiness_max_age).unwrap_or(chrono::Duration::MAX);
    let ready = state.runtime_status.snapshot().into_iter().all(|chain| {
        !chain.enabled
            || (matches!(chain.worker_state, parseon_core::status::WorkerState::Running)
                && chain
                    .last_successful_poll_at
                    .is_some_and(|at| chrono::Utc::now() - at <= max_age))
    });
    if ready {
        (axum::http::StatusCode::OK, Json(Readiness { status: "ready", reason: None }))
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(Readiness { status: "not_ready", reason: Some("worker unavailable") }),
        )
    }
}

#[utoipa::path(
    get,
    path = "/status",
    tag = "health",
    security(("bearerAuth" = [])),
    responses(
        (status = OK, description = "Canonical and finalized indexing progress and worker state", body = Status)
    )
)]
pub(crate) async fn status(State(state): State<AppState>) -> Json<Status> {
    Json(Status {
        mode: "canonical_with_finality",
        chains: state.runtime_status.snapshot().into_iter().map(Into::into).collect(),
    })
}

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "health",
    responses(
        (status = OK, description = "Prometheus metrics", body = String, content_type = "text/plain"),
        (status = INTERNAL_SERVER_ERROR, description = "Metrics encoding error", body = ErrorResponse)
    )
)]
pub(crate) async fn metrics(State(state): State<AppState>) -> AppResult<axum::response::Response> {
    let body = state.telemetry.render()?;
    Ok(([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body)
        .into_response())
}

#[utoipa::path(
    post,
    path = "/chains",
    tag = "chains",
    security(("bearerAuth" = [])),
    request_body = CreateChain,
    responses(
        (status = OK, description = "Chain registered; an enabled chain's worker starts immediately", body = ChainRow),
        (status = BAD_REQUEST, description = "Invalid RPC endpoint or finalized support", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn create_chain(
    State(state): State<AppState>,
    body: Result<Json<CreateChain>, JsonRejection>,
) -> AppResult<Json<ChainRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = state
        .chains
        .create(CreateChainCommand { rpc_url: body.rpc_url, enabled: body.enabled })
        .await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    get,
    path = "/chains",
    tag = "chains",
    security(("bearerAuth" = [])),
    responses(
        (status = OK, description = "Registered chains ordered by chain ID", body = [ChainRow]),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn list_chains(State(state): State<AppState>) -> AppResult<Json<Vec<ChainRow>>> {
    let rows = state.chains.list().await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    get,
    path = "/chains/{chain_id}",
    tag = "chains",
    security(("bearerAuth" = [])),
    params(("chain_id" = u64, Path, description = "EIP-155 chain ID")),
    responses(
        (status = OK, description = "Chain found", body = ChainRow),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn get_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<u64>,
) -> AppResult<Json<ChainRow>> {
    Ok(Json(state.chains.get(chain_id).await?.into()))
}

#[utoipa::path(
    patch,
    path = "/chains/{chain_id}",
    tag = "chains",
    security(("bearerAuth" = [])),
    params(("chain_id" = u64, Path, description = "EIP-155 chain ID")),
    request_body = UpdateChain,
    responses(
        (status = OK, description = "Chain updated; enable/disable and RPC URL changes apply to the running worker immediately", body = ChainRow),
        (status = BAD_REQUEST, description = "Invalid update or RPC endpoint", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn update_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<u64>,
    body: Result<Json<UpdateChain>, JsonRejection>,
) -> AppResult<Json<ChainRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = state
        .chains
        .update(chain_id, UpdateChainCommand { rpc_url: body.rpc_url, enabled: body.enabled })
        .await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    delete,
    path = "/chains/{chain_id}",
    tag = "chains",
    security(("bearerAuth" = [])),
    params(("chain_id" = u64, Path, description = "EIP-155 chain ID")),
    responses(
        (status = NO_CONTENT, description = "Chain worker stopped and chain data deleted"),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn delete_chain(
    State(state): State<AppState>,
    Path(chain_id): Path<u64>,
) -> AppResult<axum::http::StatusCode> {
    state.chains.delete(chain_id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/monitors",
    tag = "monitors",
    security(("bearerAuth" = [])),
    request_body = CreateMonitor,
    responses(
        (status = OK, description = "Monitor created", body = MonitorRow),
        (status = BAD_REQUEST, description = "Invalid request or ABI signature", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database or RPC error", body = ErrorResponse)
    )
)]
pub(crate) async fn create_monitor(
    State(state): State<AppState>,
    body: Result<Json<CreateMonitor>, JsonRejection>,
) -> AppResult<Json<MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = state
        .monitors
        .create(CreateMonitorCommand {
            chain_id: body.chain_id,
            address: body.address,
            signature: body.signature,
            start_block: body.start_block,
            end_block: body.end_block,
            filter: body.filter,
        })
        .await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    post,
    path = "/filters/preview",
    tag = "filters",
    security(("bearerAuth" = [])),
    request_body = FilterPreviewRequest,
    responses(
        (status = OK, description = "Canonical filter and preview result", body = FilterPreviewResponse),
        (status = BAD_REQUEST, description = "Invalid signature, filter, or sample", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Internal error", body = ErrorResponse)
    )
)]
pub(crate) async fn preview_filter(
    body: Result<Json<FilterPreviewRequest>, JsonRejection>,
) -> AppResult<Json<FilterPreviewResponse>> {
    let Json(body) = body.map_err(|error| AppError::BadRequest(error.body_text()))?;
    Ok(Json(
        parseon_core::services::preview_filter(PreviewFilterCommand {
            signature: body.signature,
            filter: body.filter,
            sample: body.sample.into(),
        })?
        .into(),
    ))
}

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct MonitorListQuery {
    pub chain_id: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/monitors",
    tag = "monitors",
    security(("bearerAuth" = [])),
    params(MonitorListQuery),
    responses(
        (status = OK, description = "Monitors ordered by ID", body = [MonitorRow]),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn list_monitors(
    State(state): State<AppState>,
    Query(query): Query<MonitorListQuery>,
) -> AppResult<Json<Vec<MonitorRow>>> {
    let rows = state.monitors.list(query.chain_id).await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    get,
    path = "/monitors/{id}",
    tag = "monitors",
    security(("bearerAuth" = [])),
    params(("id" = u64, Path, description = "Monitor ID")),
    responses(
        (status = OK, description = "Monitor found", body = MonitorRow),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn get_monitor(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> AppResult<Json<MonitorRow>> {
    let row = state.monitors.get(monitor_id(id)?).await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    patch,
    path = "/monitors/{id}",
    tag = "monitors",
    security(("bearerAuth" = [])),
    params(("id" = u64, Path, description = "Monitor ID")),
    request_body = UpdateMonitor,
    responses(
        (status = OK, description = "Monitor enabled state updated", body = MonitorRow),
        (status = BAD_REQUEST, description = "Invalid request", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn update_monitor(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    body: Result<Json<UpdateMonitor>, JsonRejection>,
) -> AppResult<Json<MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let row = state.monitors.set_enabled(monitor_id(id)?, body.enabled).await?;
    Ok(Json(row.into()))
}

#[utoipa::path(
    delete,
    path = "/monitors/{id}",
    tag = "monitors",
    security(("bearerAuth" = [])),
    params(("id" = u64, Path, description = "Monitor ID")),
    responses(
        (status = NO_CONTENT, description = "Monitor deleted"),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn delete_monitor(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> AppResult<axum::http::StatusCode> {
    state.monitors.delete(monitor_id(id)?).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/monitors/{id}/results",
    tag = "results",
    security(("bearerAuth" = [])),
    params(
        ("id" = u64, Path, description = "Monitor ID"),
        ("limit" = Option<u64>, Query, description = "Maximum number of results (default 50, max 200)"),
        ("offset" = Option<u64>, Query, description = "Pagination offset (default 0, max 1000000)"),
        ("finality" = Option<String>, Query, description = "Result lifecycle: finalized (default), provisional, or all")
    ),
    responses(
        (status = OK, description = "Decoded results ordered by block_number descending", body = [MonitorResult]),
        (status = BAD_REQUEST, description = "Invalid query parameter", body = ErrorResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Database error", body = ErrorResponse)
    )
)]
pub(crate) async fn list_monitor_results(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Query(query): Query<ResultsQuery>,
) -> AppResult<Json<Vec<MonitorResult>>> {
    if query.offset > 1_000_000 {
        return Err(AppError::BadRequest("result offset exceeds 1000000".into()));
    }
    let rows = state
        .monitors
        .results(
            monitor_id(id)?,
            ResultQuery {
                limit: PageLimit::new(query.limit),
                offset: query.offset,
                finality: parse_finality(query.finality.as_deref().unwrap_or("finalized"))?,
            },
        )
        .await?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

fn parse_finality(value: &str) -> AppResult<Option<Finality>> {
    match value {
        "finalized" => Ok(Some(Finality::Finalized)),
        "provisional" => Ok(Some(Finality::Provisional)),
        "all" => Ok(None),
        _ => Err(AppError::BadRequest("finality must be finalized, provisional, or all".into())),
    }
}

fn monitor_id(id: u64) -> AppResult<MonitorId> {
    MonitorId::new(id).map_err(|error| AppError::BadRequest(error.to_string()))
}
