pub mod dto;
pub mod handlers;
pub mod openapi;
pub mod routes;

use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::api::openapi::ApiDoc;
use crate::db::storage::PostgresStorage;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: PostgresStorage,
}

impl AppState {
    pub fn new(storage: PostgresStorage) -> Self {
        Self { storage }
    }
}

/// Build the axum router.
pub fn router(state: AppState) -> axum::Router {
    let (router, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(routes::health_routes())
        .merge(routes::monitor_routes())
        .split_for_parts();

    router
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    use super::{AppState, router};
    use crate::db::storage::PostgresStorage;

    fn test_router() -> axum::Router {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/parseon")
            .expect("test database URL should be valid");
        router(AppState::new(PostgresStorage::new(pool)))
    }

    #[tokio::test]
    async fn serves_complete_openapi_document() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/api-docs/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let document: Value = serde_json::from_slice(&body).unwrap();
        let paths = document["paths"].as_object().unwrap();

        let expected_operations = [
            ("/healthz", "get", &["200", "500"][..]),
            ("/monitors", "get", &["200", "500"][..]),
            ("/monitors", "post", &["200", "400", "409", "500"][..]),
            ("/monitors/{id}", "get", &["200", "404", "500"][..]),
            ("/monitors/{id}", "patch", &["200", "400", "404", "500"][..]),
            ("/monitors/{id}", "delete", &["204", "404", "500"][..]),
            (
                "/monitors/{id}/results",
                "get",
                &["200", "400", "404", "500"][..],
            ),
        ];

        for (path, method, expected_statuses) in expected_operations {
            let responses = paths[path][method]["responses"].as_object().unwrap();
            for status in expected_statuses {
                assert!(
                    responses.contains_key(*status),
                    "missing {status} response for {method} {path}"
                );
            }
        }

        let schemas = document["components"]["schemas"].as_object().unwrap();
        for schema in [
            "AbiParamSchema",
            "CreateMonitor",
            "ErrorResponse",
            "Health",
            "MonitorResult",
            "MonitorRow",
            "UpdateMonitor",
        ] {
            assert!(schemas.contains_key(schema), "missing schema {schema}");
        }
        let abi_param_properties = schemas["AbiParamSchema"]["properties"].as_object().unwrap();
        assert_eq!(abi_param_properties.len(), 2);
        assert!(abi_param_properties.contains_key("name"));
        assert!(abi_param_properties.contains_key("sol_type"));
        assert!(!schemas.contains_key("SqlKind"));
    }

    #[tokio::test]
    async fn serves_embedded_swagger_ui() {
        let app = test_router();
        let redirect = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/swagger-ui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(redirect.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            redirect.headers().get(header::LOCATION).unwrap(),
            "/swagger-ui/"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/swagger-ui/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
    }
}
