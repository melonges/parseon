pub mod dto;
pub mod handlers;
pub mod routes;

use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Build the axum router.
pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .merge(routes::health_routes())
        .merge(routes::monitor_routes())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
