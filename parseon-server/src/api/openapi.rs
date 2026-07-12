use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Parseon API",
        description = "HTTP API for managing Parseon EVM chains, monitors, and decoded results"
    ),
    tags(
        (name = "health", description = "Service health, finalized indexing status, and metrics"),
        (name = "chains", description = "EVM chain registry management"),
        (name = "monitors", description = "EVM call and event monitor management"),
        (name = "filters", description = "Stateless monitor filter validation and preview"),
        (name = "results", description = "Search decoded monitor results")
    )
)]
pub struct ApiDoc;
