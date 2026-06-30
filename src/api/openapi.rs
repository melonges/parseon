use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Parseon API",
        description = "HTTP API for managing Parseon EVM transaction monitors"
    ),
    tags(
        (name = "health", description = "Service health"),
        (name = "monitors", description = "EVM transaction monitor management")
    )
)]
pub struct ApiDoc;
