use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// Connect using caller-supplied options, ten connections and other SQLx defaults.
/// No dotenv loading, URL parsing, custom retries or logger installation occurs here.
/// The original SQLx error is returned; diagnostics omit its arbitrary text.
pub async fn connect_pool(options: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
        .inspect_err(|error| crate::diagnostics::log_sqlx_failure("connect", error))
}
