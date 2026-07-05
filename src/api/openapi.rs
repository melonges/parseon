use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Parseon API",
        description = "HTTP API for managing Parseon EVM call and event monitors"
    ),
    tags(
        (name = "health", description = "Service health and finalized indexing status"),
        (name = "monitors", description = "EVM call and event monitor management"),
        (name = "results", description = "Search decoded monitor results")
    )
)]
pub struct ApiDoc;
