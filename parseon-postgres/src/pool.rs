use std::num::NonZeroU32;

use parseon_core::Url;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(
    database_url: &Url,
    max_connections: NonZeroU32,
) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections.get())
        .connect(database_url.as_str())
        .await?;
    sqlx::migrate!("./src/migrations").run(&pool).await?;
    tracing::info!("database migrations applied");
    Ok(pool)
}
