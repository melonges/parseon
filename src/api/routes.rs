use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::api::AppState;
use crate::api::handlers;

pub fn health_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(handlers::healthz))
        .routes(routes!(handlers::status))
}

pub fn monitor_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(handlers::create_monitor, handlers::list_monitors))
        .routes(routes!(
            handlers::get_monitor,
            handlers::update_monitor,
            handlers::delete_monitor
        ))
        .routes(routes!(handlers::list_monitor_results))
}
