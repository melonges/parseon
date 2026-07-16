use parseon_core::Url;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(storage_url: &Url) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new().connect(storage_url.as_str()).await?;
    sqlx::migrate!("./src/migrations").run(&pool).await?;
    tracing::info!("PostgreSQL migrations applied");
    Ok(pool)
}
