use super::*;
use cqrs_runtime::postgres::connect_pool;

#[tokio::test]
async fn connection_helper_preserves_ten_connection_limit_and_returns_authentication_error() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    let pool = connect_pool(options.clone()).await.unwrap();
    assert_eq!(pool.options().get_max_connections(), 10);
    assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool).await.unwrap(), 1);
    pool.close().await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        connect_pool(options.password("private_incorrect_password")),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    assert!(matches!(error, sqlx::Error::Database(_)));
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("28P01"));
    db.close().await;
}
