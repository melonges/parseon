use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};

use crate::api::AppState;
use crate::api::dto::{CreateMonitor, Health, UpdateMonitor};
use crate::db::monitor_repo::{self, MonitorInput};
use crate::error::{AppError, AppResult};

// ----- Health -----

pub async fn healthz(State(state): State<AppState>) -> AppResult<Json<Health>> {
    let monitors = monitor_repo::count(&state.pool).await?;
    Ok(Json(Health {
        status: "ok",
        monitors,
    }))
}

// ----- Monitors -----

pub async fn create_monitor(
    State(state): State<AppState>,
    body: Result<Json<CreateMonitor>, JsonRejection>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let Json(body) = body.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let input = MonitorInput {
        address: body.address,
        name: body.name,
        signature: body.signature,
        start_block: body.start_block,
        end_block: body.end_block,
    };
    let row = monitor_repo::create(&state.pool, &input).await?;
    Ok(Json(row))
}

pub async fn list_monitors(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::db::monitor_repo::MonitorRow>>> {
    let rows = monitor_repo::list(&state.pool).await?;
    Ok(Json(rows))
}

pub async fn get_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let row = monitor_repo::get(&state.pool, id).await?;
    Ok(Json(row))
}

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

pub async fn delete_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::http::StatusCode> {
    monitor_repo::delete(&state.pool, id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}
