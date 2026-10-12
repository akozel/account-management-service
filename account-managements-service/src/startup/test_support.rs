//! Private executable test fixtures and process isolation for global setup.
use std::process::Command;

use cqrs_runtime::postgres::{EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA};
use sqlx::PgPool;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

pub(crate) fn scenario() -> Option<String> {
    std::env::var("ACCOUNT_SERVICE_TEST_SCENARIO").ok()
}

pub(crate) fn isolated(test: &str, scenario: &str, configure: impl FnOnce(&mut Command)) {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args([test, "--exact", "--nocapture"])
        .env("ACCOUNT_SERVICE_TEST_SCENARIO", scenario);
    configure(&mut child);
    let output = child.output().unwrap();
    assert!(
        output.status.success(),
        "isolated scenario {scenario} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

pub(super) struct Database {
    pub(super) url: String,
    pub(super) pool: PgPool,
    container: ContainerAsync<GenericImage>,
}

impl Database {
    pub(super) async fn new() -> Self {
        let container = GenericImage::new("docker.io/library/postgres", "18.6-alpine")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections").with_times(2),
            ))
            .with_env_var("POSTGRES_DB", "service_test")
            .with_env_var("POSTGRES_USER", "service_test")
            .with_env_var("POSTGRES_PASSWORD", "service_test")
            .start()
            .await
            .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let url = format!("postgresql://service_test:service_test@{host}:{port}/service_test");
        let pool = PgPool::connect(&url).await.unwrap();
        sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&pool).await.unwrap();
        sqlx::raw_sql(TRANSACTIONAL_OUTBOX_SCHEMA).execute(&pool).await.unwrap();
        Self { url, pool, container }
    }

    pub(super) async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}
