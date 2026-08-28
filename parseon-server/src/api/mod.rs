pub(crate) mod dto;
pub(crate) mod handlers;
pub(crate) mod openapi;
pub(crate) mod routes;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
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
pub(crate) struct AppState {
    pub chains: ChainService,
    pub monitors: MonitorService,
    pub runtime_status: RuntimeStatus,
    pub telemetry: std::sync::Arc<dyn Telemetry>,
    pub api_token: Arc<str>,
    pub cors_origins: Arc<str>,
    pub max_body_bytes: usize,
    pub readiness_max_age: Duration,
}

impl AppState {
    pub(crate) fn new(
        chains: ChainService,
        monitors: MonitorService,
        runtime_status: RuntimeStatus,
        telemetry: std::sync::Arc<dyn Telemetry>,
        api_token: String,
        cors_origins: String,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            chains,
            monitors,
            runtime_status,
            telemetry,
            api_token: Arc::from(api_token),
            cors_origins: Arc::from(cors_origins),
            max_body_bytes,
            readiness_max_age: Duration::from_secs(30),
        }
    }
}

pub(crate) fn router(state: AppState) -> axum::Router {
    let (router, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(routes::health_routes())
        .merge(routes::chain_routes())
        .merge(routes::monitor_routes())
        .merge(routes::filter_routes())
        .split_for_parts();

    let cors = cors_layer(&state.cors_origins);
    router
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(RequestBodyLimitLayer::new(state.max_body_bytes))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn cors_layer(origins: &str) -> CorsLayer {
    let mut layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    let origins = origins
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .filter_map(|origin| origin.parse().ok())
        .collect::<Vec<header::HeaderValue>>();
    if !origins.is_empty() {
        layer = layer.allow_origin(origins);
    }
    layer
}

async fn authenticate(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if matches!(path, "/healthz" | "/readyz" | "/metrics")
        || path == "/swagger-ui"
        || path.starts_with("/swagger-ui/")
        || path == "/api-docs/openapi.json"
    {
        return next.run(request).await;
    }
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| constant_time_equal(value.as_bytes(), state.api_token.as_bytes()));
    if !authorized {
        return (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE, "Bearer")], "unauthorized")
            .into_response();
    }
    next.run(request).await
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        diff |= usize::from(u8::from(left.get(index) != right.get(index)));
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use serde_json::{Value, json};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    use super::{AppState, router};
    use parseon_core::ports::NoopSink;
    use parseon_core::services::{ChainService, MonitorService};
    use parseon_core::status::{ChainStatus, RuntimeStatus};
    use parseon_core::supervisor::{Supervisor, SupervisorConfig, SupervisorDependencies};
    use parseon_rpc::JsonRpcBlockSourceFactory;

    fn test_router() -> axum::Router {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/parseon")
            .expect("test database URL should be valid");
        let statuses = RuntimeStatus::default();
        let running = ChainStatus::starting(42, None);
        running.record_success(20_000_000);
        statuses.replace(running);
        statuses.replace(ChainStatus::disabled(8453));
        let storage = Arc::new(parseon_postgres::PostgresStorage::new(pool));
        let sources = Arc::new(JsonRpcBlockSourceFactory::default());
        let telemetry = Arc::new(crate::metrics::Metrics::default());
        let supervisor = Supervisor::new(
            SupervisorConfig {
                batch_size: NonZeroU64::new(10).expect("non-zero"),
                poll_interval: Duration::from_secs(1),
                block_concurrency: NonZeroUsize::new(2).expect("non-zero"),
                storage_write_concurrency: NonZeroUsize::new(2).expect("non-zero"),
                confirmations: NonZeroU64::new(64).expect("non-zero"),
                rollback_retention: NonZeroU64::new(256).expect("non-zero"),
            },
            Vec::new(),
            SupervisorDependencies {
                storage: storage.clone(),
                sink: Arc::new(NoopSink),
                source_factory: sources.clone(),
                cache_factory: Arc::new(parseon_memory_cache::MemoryBlockCacheFactory::new(0)),
                statuses: statuses.clone(),
                telemetry: telemetry.clone(),
            },
        );
        router(AppState::new(
            ChainService::new(storage.clone(), sources.clone(), supervisor.handle()),
            MonitorService::new(storage, sources),
            statuses,
            telemetry,
            "test-token".into(),
            String::new(),
            1024 * 1024,
        ))
    }

    #[tokio::test]
    async fn protects_api_routes_but_keeps_liveness_public() {
        let unauthorized = test_router()
            .oneshot(Request::builder().uri("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let health = test_router()
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn serves_complete_openapi_document() {
        let response = test_router()
            .oneshot(Request::builder().uri("/api-docs/openapi.json").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::CONTENT_TYPE).unwrap(), "application/json");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let document: Value = serde_json::from_slice(&body).unwrap();
        let paths = document["paths"].as_object().unwrap();

        let expected_operations = [
            ("/healthz", "get", &["200"][..]),
            ("/status", "get", &["200"][..]),
            ("/readyz", "get", &["200", "503"][..]),
            ("/metrics", "get", &["200", "500"][..]),
            ("/chains", "get", &["200", "500"][..]),
            ("/chains", "post", &["200", "400", "500"][..]),
            ("/chains/{chain_id}", "get", &["200", "500"][..]),
            ("/chains/{chain_id}", "patch", &["200", "400", "500"][..]),
            ("/chains/{chain_id}", "delete", &["204", "500"][..]),
            ("/monitors", "get", &["200", "500"][..]),
            ("/monitors", "post", &["200", "400", "500"][..]),
            ("/filters/preview", "post", &["200", "400", "500"][..]),
            ("/monitors/{id}", "get", &["200", "500"][..]),
            ("/monitors/{id}", "patch", &["200", "400", "500"][..]),
            ("/monitors/{id}", "delete", &["204", "500"][..]),
            ("/monitors/{id}/results", "get", &["200", "400", "500"][..]),
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
        for (path, method) in [
            ("/status", "get"),
            ("/chains", "get"),
            ("/chains", "post"),
            ("/chains/{chain_id}", "get"),
            ("/chains/{chain_id}", "patch"),
            ("/chains/{chain_id}", "delete"),
            ("/monitors", "get"),
            ("/monitors", "post"),
            ("/filters/preview", "post"),
            ("/monitors/{id}", "get"),
            ("/monitors/{id}", "patch"),
            ("/monitors/{id}", "delete"),
            ("/monitors/{id}/results", "get"),
        ] {
            assert_eq!(paths[path][method]["security"], json!([{ "bearerAuth": [] }]));
        }
        for (path, method) in [("/healthz", "get"), ("/readyz", "get"), ("/metrics", "get")] {
            assert!(paths[path][method].get("security").is_none());
        }
        assert_eq!(document["components"]["securitySchemes"]["bearerAuth"]["scheme"], "bearer");

        let schemas = document["components"]["schemas"].as_object().unwrap();
        for schema in [
            "AbiParamSchema",
            "CallMonitorResult",
            "ChainRow",
            "ChainStatusRow",
            "CreateChain",
            "CreateMonitor",
            "ErrorResponse",
            "FilterPreviewRequest",
            "FilterPreviewResponse",
            "FilterSampleInput",
            "Health",
            "Readiness",
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
        assert_eq!(schemas["CreateChain"]["properties"]["rpc_url"]["writeOnly"], true);
        assert_eq!(schemas["CreateChain"]["properties"]["rpc_url"]["format"], "uri");
        assert!(schemas["MonitorRow"]["properties"].get("chain_id").is_some());
        assert!(schemas["CreateMonitor"]["properties"].get("chain_id").is_some());

        let monitor_parameters = paths["/monitors"]["get"]["parameters"].as_array().unwrap();
        assert_eq!(monitor_parameters[0]["name"], "chain_id");

        let result_parameters =
            paths["/monitors/{id}/results"]["get"]["parameters"].as_array().unwrap();
        let parameter_names = result_parameters
            .iter()
            .map(|parameter| parameter["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(parameter_names, ["id", "limit", "offset", "finality"]);
    }

    #[tokio::test]
    async fn reports_finalized_runtime_status() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .header(header::AUTHORIZATION, "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["mode"], "canonical_with_finality");
        assert_eq!(status["chains"].as_array().unwrap().len(), 2);
        assert_eq!(status["chains"][0]["chain_id"], 42);
        assert_eq!(status["chains"][0]["enabled"], true);
        assert_eq!(status["chains"][0]["latest_head"], 20_000_000);
        assert_eq!(status["chains"][0]["canonical_head"], 20_000_000);
        assert_eq!(status["chains"][0]["finalized_head"], 20_000_000);
        assert_eq!(status["chains"][0]["promotion_height"], 20_000_000);
        assert_eq!(status["chains"][0]["worker_state"], "running");
        assert!(status["chains"][0]["last_successful_poll_at"].is_string());
        assert!(status["chains"][0]["last_error"].is_null());
        assert_eq!(status["chains"][1]["chain_id"], 8453);
        assert_eq!(status["chains"][1]["worker_state"], "disabled");
        assert!(status["chains"][1].get("finalized_head").is_none());
    }

    #[tokio::test]
    async fn serves_liveness_without_storage_access() {
        let response = test_router()
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["status"], "ok");
    }

    #[tokio::test]
    async fn serves_embedded_swagger_ui() {
        let app = test_router();
        let redirect = app
            .clone()
            .oneshot(Request::builder().uri("/swagger-ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(redirect.status(), StatusCode::SEE_OTHER);
        assert_eq!(redirect.headers().get(header::LOCATION).unwrap(), "/swagger-ui/");

        let response = app
            .oneshot(Request::builder().uri("/swagger-ui/").body(Body::empty()).unwrap())
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
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
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
