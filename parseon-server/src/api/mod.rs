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
use parseon_core::ports::Telemetry;
use parseon_core::services::{ChainService, MonitorService};
use parseon_core::status::RuntimeStatus;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub chains: ChainService,
    pub monitors: MonitorService,
    pub runtime_status: RuntimeStatus,
    pub telemetry: std::sync::Arc<dyn Telemetry>,
}

impl AppState {
    pub fn new(
        chains: ChainService,
        monitors: MonitorService,
        runtime_status: RuntimeStatus,
        telemetry: std::sync::Arc<dyn Telemetry>,
    ) -> Self {
        Self {
            chains,
            monitors,
            runtime_status,
            telemetry,
        }
    }
}

/// Build the axum router.
pub fn router(state: AppState) -> axum::Router {
    let (router, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(routes::health_routes())
        .merge(routes::chain_routes())
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
    use parseon_core::services::{ChainService, MonitorService};
    use parseon_core::status::{ChainStatus, RuntimeStatus};
    use parseon_rpc::JsonRpcBlockSourceFactory;

    fn test_router() -> axum::Router {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/parseon")
            .expect("test database URL should be valid");
        let statuses = RuntimeStatus::default();
        let running = ChainStatus::starting(42, None);
        running.record_success(20_000_000);
        statuses.replace(running);
        statuses.replace(ChainStatus::disabled(8453));
        let storage = std::sync::Arc::new(parseon_postgres::PostgresStorage::new(pool));
        let sources = std::sync::Arc::new(JsonRpcBlockSourceFactory::default());
        router(AppState::new(
            ChainService::new(storage.clone(), sources.clone()),
            MonitorService::new(storage.clone(), storage.clone(), storage, sources),
            statuses,
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        ))
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
            ("/status", "get", &["200"][..]),
            ("/metrics", "get", &["200", "500"][..]),
            ("/chains", "get", &["200", "500"][..]),
            ("/chains", "post", &["201", "400", "500"][..]),
            ("/chains/{chain_id}", "get", &["200", "500"][..]),
            (
                "/chains/{chain_id}",
                "patch",
                &["200", "400", "500"][..],
            ),
            ("/chains/{chain_id}", "delete", &["204", "500"][..]),
            ("/monitors", "get", &["200", "500"][..]),
            ("/monitors", "post", &["200", "400", "500"][..]),
            ("/monitors/{id}", "get", &["200", "500"][..]),
            ("/monitors/{id}", "patch", &["200", "400", "500"][..]),
            ("/monitors/{id}", "delete", &["204", "500"][..]),
            (
                "/monitors/{id}/results",
                "get",
                &["200", "400", "500"][..],
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
            "CallMonitorResult",
            "ChainRow",
            "ChainStatusRow",
            "CreateChain",
            "CreateMonitor",
            "ErrorResponse",
            "Health",
            "MonitorResult",
            "MonitorRow",
            "EventMonitorResult",
            "Status",
            "UpdateMonitor",
            "UpdateChain",
        ] {
            assert!(schemas.contains_key(schema), "missing schema {schema}");
        }
        let abi_param_properties = schemas["AbiParamSchema"]["properties"].as_object().unwrap();
        assert_eq!(abi_param_properties.len(), 3);
        assert!(abi_param_properties.contains_key("name"));
        assert!(abi_param_properties.contains_key("sol_type"));
        assert!(abi_param_properties.contains_key("indexed"));
        assert!(!schemas.contains_key("SqlKind"));
        assert!(schemas["ChainRow"]["properties"].get("rpc_url").is_none());
        assert_eq!(
            schemas["CreateChain"]["properties"]["rpc_url"]["writeOnly"],
            true
        );
        assert_eq!(schemas["CreateChain"]["properties"]["rpc_url"]["format"], "uri");
        assert!(
            schemas["MonitorRow"]["properties"]
                .get("chain_id")
                .is_some()
        );
        assert!(
            schemas["CreateMonitor"]["properties"]
                .get("chain_id")
                .is_some()
        );

        let monitor_parameters = paths["/monitors"]["get"]["parameters"].as_array().unwrap();
        assert_eq!(monitor_parameters[0]["name"], "chain_id");

        let result_parameters = paths["/monitors/{id}/results"]["get"]["parameters"]
            .as_array()
            .unwrap();
        let parameter_names = result_parameters
            .iter()
            .map(|parameter| parameter["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(parameter_names, ["id", "limit", "offset"]);
    }

    #[tokio::test]
    async fn reports_finalized_runtime_status() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["mode"], "finalized");
        assert_eq!(status["chains"].as_array().unwrap().len(), 2);
        assert_eq!(status["chains"][0]["chain_id"], 42);
        assert_eq!(status["chains"][0]["enabled"], true);
        assert_eq!(status["chains"][0]["finalized_head"], 20_000_000);
        assert_eq!(status["chains"][0]["worker_state"], "running");
        assert!(status["chains"][0]["last_successful_poll_at"].is_string());
        assert!(status["chains"][0]["last_error"].is_null());
        assert_eq!(status["chains"][1]["chain_id"], 8453);
        assert_eq!(status["chains"][1]["worker_state"], "disabled");
        assert!(status["chains"][1].get("finalized_head").is_none());
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

    #[tokio::test]
    async fn serves_prometheus_metrics() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("parseon_rpc_operations"));
        assert!(!body.contains("rpc_url"));
    }
}
