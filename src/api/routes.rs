use axum::Router;
use axum::routing::{get, post};

use crate::api::AppState;
use crate::api::handlers;

pub fn health_routes() -> Router<AppState> {
    Router::new().route("/healthz", get(handlers::healthz))
}

pub fn chain_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/chains",
            post(handlers::create_chain).get(handlers::list_chains),
        )
        .route(
            "/chains/:id",
            get(handlers::get_chain)
                .patch(handlers::update_chain)
                .delete(handlers::delete_chain),
        )
}

pub fn monitor_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/monitors",
            post(handlers::create_monitor).get(handlers::list_monitors),
        )
        .route(
            "/monitors/:id",
            get(handlers::get_monitor)
                .patch(handlers::update_monitor)
                .delete(handlers::delete_monitor),
        )
}
