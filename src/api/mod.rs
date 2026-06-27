pub mod dto;
pub mod handlers;
pub mod routes;

use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::watcher::registry::Registry;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub registry: Registry,
}

impl AppState {
    pub fn new(pool: PgPool, registry: Registry) -> Self {
        Self { pool, registry }
    }
}

/// Build the axum router.
pub fn router(
    state: AppState,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> axum::Router {
    axum::Router::new()
        .merge(routes::health_routes())
        .merge(routes::monitor_routes())
        .route(
            "/metrics",
            axum::routing::get(move || async move { metrics_handle.render() }),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
