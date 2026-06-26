use axum::Json;
use axum::extract::{Path, State};

use crate::api::AppState;
use crate::api::dto::{CreateChain, CreateMonitor, Health, UpdateChain, UpdateMonitor};
use crate::db::chain_repo::{self, ChainInput};
use crate::db::monitor_repo::{self, MonitorInput};
use crate::error::AppResult;

// ----- Health -----

pub async fn healthz(State(state): State<AppState>) -> AppResult<Json<Health>> {
    let chains = chain_repo::list(&state.pool).await.unwrap_or_default();
    let monitors = monitor_repo::list(&state.pool).await.unwrap_or_default();
    Ok(Json(Health {
        status: "ok",
        chains: chains.len(),
        monitors: monitors.len(),
    }))
}

// ----- Chains -----

pub async fn create_chain(
    State(state): State<AppState>,
    Json(body): Json<CreateChain>,
) -> AppResult<Json<crate::db::chain_repo::ChainRow>> {
    let input = ChainInput {
        name: body.name,
        chain_id: body.chain_id,
        rpc_url: body.rpc_url,
        start_block: body.start_block,
        poll_interval_ms: body.poll_interval_ms,
        batch_size: body.batch_size,
        enabled: body.enabled,
    };
    let row = chain_repo::create(&state.pool, &input).await?;
    Ok(Json(row))
}

pub async fn list_chains(
    State(state): State<AppState>,
) -> AppResult<Json<Vec<crate::db::chain_repo::ChainRow>>> {
    let rows = chain_repo::list(&state.pool).await?;
    Ok(Json(rows))
}

pub async fn get_chain(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::db::chain_repo::ChainRow>> {
    let row = chain_repo::get(&state.pool, id).await?;
    Ok(Json(row))
}

pub async fn update_chain(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateChain>,
) -> AppResult<Json<crate::db::chain_repo::ChainRow>> {
    let row = chain_repo::set_enabled(&state.pool, id, body.enabled.unwrap_or(true)).await?;
    Ok(Json(row))
}

pub async fn delete_chain(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::http::StatusCode> {
    chain_repo::delete(&state.pool, id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ----- Monitors -----

pub async fn create_monitor(
    State(state): State<AppState>,
    Json(body): Json<CreateMonitor>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    // Default start_block to the chain's start_block if not provided.
    let start_block = match body.start_block {
        Some(s) => Some(s),
        None => Some(
            chain_repo::get_by_chain_id(&state.pool, body.chain_id)
                .await?
                .start_block,
        ),
    };
    let input = MonitorInput {
        chain_id: body.chain_id,
        address: body.address,
        signature: body.signature,
        start_block,
        end_block: body.end_block,
    };
    let row = monitor_repo::create(&state.pool, &input).await?;
    state.registry.reload(&state.pool).await?;
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
    Json(body): Json<UpdateMonitor>,
) -> AppResult<Json<crate::db::monitor_repo::MonitorRow>> {
    let row = monitor_repo::update(
        &state.pool,
        id,
        body.start_block,
        body.end_block,
        body.enabled,
    )
    .await?;
    state.registry.reload(&state.pool).await?;
    Ok(Json(row))
}

pub async fn delete_monitor(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<axum::http::StatusCode> {
    monitor_repo::delete(&state.pool, id).await?;
    state.registry.reload(&state.pool).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}
