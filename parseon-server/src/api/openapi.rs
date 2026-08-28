use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::openapi::{Components, OpenApi};
use utoipa::{Modify, OpenApi as DeriveOpenApi};

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut OpenApi) {
        let components = openapi.components.get_or_insert_with(Components::new);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
    }
}

#[derive(DeriveOpenApi)]
#[openapi(
    info(
        title = "Parseon API",
        description = "HTTP API for managing Parseon EVM chains, monitors, and decoded results"
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "health", description = "Service health, canonical indexing status, and metrics"),
        (name = "chains", description = "EVM chain registry management"),
        (name = "monitors", description = "EVM call and event monitor management"),
        (name = "filters", description = "Stateless monitor filter validation and preview"),
        (name = "results", description = "Search decoded monitor results")
    )
)]
pub(crate) struct ApiDoc;
