use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::api::AppState;
use crate::api::handlers;

pub(crate) fn health_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(handlers::healthz))
        .routes(routes!(handlers::status))
        .routes(routes!(handlers::readyz))
        .routes(routes!(handlers::metrics))
}

pub(crate) fn monitor_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(handlers::create_monitor, handlers::list_monitors))
        .routes(routes!(handlers::get_monitor, handlers::update_monitor, handlers::delete_monitor))
        .routes(routes!(handlers::list_monitor_results))
}

pub(crate) fn filter_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(handlers::preview_filter))
}

pub(crate) fn chain_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(handlers::create_chain, handlers::list_chains))
        .routes(routes!(handlers::get_chain, handlers::update_chain, handlers::delete_chain))
}
