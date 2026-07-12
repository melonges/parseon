use parseon_core::Url;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(database_url: &Url) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(16).connect(database_url.as_str()).await?;
    sqlx::migrate!("./src/migrations").run(&pool).await?;
    tracing::info!("database migrations applied");
    Ok(pool)
}
