//! Fixed diagnostic context, independent of error Display/Debug and payloads.

pub(crate) fn safe_type(value: &str) -> &str {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        value
    } else {
        "unknown"
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn sqlx_kind(error: &sqlx::Error) -> &'static str {
    match error {
        sqlx::Error::Io(_) => "io",
        sqlx::Error::Tls(_) => "tls",
        sqlx::Error::Database(_) => "database",
        sqlx::Error::PoolTimedOut => "pool_timeout",
        sqlx::Error::PoolClosed => "pool_closed",
        sqlx::Error::Decode(_) | sqlx::Error::ColumnDecode { .. } => "decode",
        sqlx::Error::Configuration(_) => "configuration",
        _ => "other",
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn sqlstate(error: &sqlx::Error) -> Option<std::borrow::Cow<'_, str>> {
    let code = error.as_database_error()?.code()?;
    (code.len() == 5 && code.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())).then_some(code)
}

#[cfg(feature = "postgres")]
pub(crate) fn log_sqlx_failure(operation: &'static str, error: &sqlx::Error) {
    tracing::error!(component = "postgres", operation, category = sqlx_kind(error), sqlstate = ?sqlstate(error), "storage operation failed");
}

pub(crate) fn log_command_failure(aggregate_type: &str, category: &'static str, source: &(dyn std::error::Error + 'static)) {
    // These details are inspected before CommandGatewayError intentionally discards
    // technical sources. They never change the outward error or retry policy.
    #[cfg(feature = "postgres")]
    {
        let mut current = Some(source);
        while let Some(error) = current {
            if let Some(error) = error.downcast_ref::<sqlx::Error>() {
                tracing::error!(component = "command", operation = "execute", aggregate_type = safe_type(aggregate_type), category,
                    storage_category = sqlx_kind(error), sqlstate = ?sqlstate(error), "command execution failed");
                return;
            }
            current = error.source();
        }
    }
    #[cfg(not(feature = "postgres"))]
    let _ = source;
    tracing::error!(
        component = "command",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        category,
        "command execution failed"
    );
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        future::Future,
        io,
        sync::{Arc, Mutex},
    };
    use tracing::instrument::WithSubscriber;

    #[derive(Clone, Default)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    pub(crate) async fn capture<T>(work: impl Future<Output = T>) -> (T, String) {
        let writer = Writer::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_writer(writer.clone())
                .with_max_level(tracing::Level::ERROR)
                .with_ansi(false)
                .without_time()
                .finish(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            // Prime shared callsites that parallel tests may register without a subscriber.
            super::log_command_failure("prime", "unexpected", &io::Error::other("prime"));
            #[cfg(feature = "postgres")]
            {
                super::log_command_failure("prime", "unexpected", &sqlx::Error::RowNotFound);
                super::log_sqlx_failure("prime", &sqlx::Error::RowNotFound);
            }
            tracing::callsite::rebuild_interest_cache();
            writer.0.lock().unwrap().clear();
        });
        let result = work.with_subscriber(dispatch).await;
        let text = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        (result, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framework_diagnostics_keep_category_and_sanitize_type_without_source_text() {
        let (_, log) = test_support::capture(async {
            for category in ["connection", "deserialization", "unexpected"] {
                log_command_failure("Aggregate_1", category, &std::io::Error::other("private_source"));
            }
            log_command_failure(
                "private_type\nmail@example.com",
                "unexpected",
                &std::io::Error::other("private_source"),
            );
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 4);
        for category in ["connection", "deserialization", "unexpected"] {
            assert!(log.contains(category));
        }
        assert!(log.contains("aggregate_type=\"Aggregate_1\""));
        assert!(log.contains("aggregate_type=\"unknown\""));
        assert!(log.contains("operation=\"execute\""));
        assert!(!log.contains("private_"));
        assert_eq!(safe_type(""), "unknown");
    }

    #[cfg(feature = "postgres")]
    #[derive(Debug, thiserror::Error)]
    #[error("private_database_message_payload_query_constraint")]
    struct DatabaseFault {
        code: Option<&'static str>,
    }

    #[cfg(feature = "postgres")]
    impl sqlx::error::DatabaseError for DatabaseFault {
        fn message(&self) -> &str {
            "private_database_message"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code.map(Into::into)
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn sqlx_classification_uses_fixed_categories_and_valid_sqlstate_only() {
        use std::io;
        let errors = [
            (sqlx::Error::Io(io::Error::other("private_io")), "io"),
            (sqlx::Error::Tls(Box::new(io::Error::other("private_tls"))), "tls"),
            (sqlx::Error::PoolTimedOut, "pool_timeout"),
            (sqlx::Error::PoolClosed, "pool_closed"),
            (sqlx::Error::Decode(Box::new(io::Error::other("private_decode"))), "decode"),
            (
                sqlx::Error::ColumnDecode {
                    index: "private_column".into(),
                    source: Box::new(io::Error::other("private_decode")),
                },
                "decode",
            ),
            (
                sqlx::Error::Configuration(Box::new(io::Error::other("private_url"))),
                "configuration",
            ),
            (sqlx::Error::Protocol("private_protocol".into()), "other"),
            (
                sqlx::Error::Database(Box::new(DatabaseFault { code: Some("42P01") })),
                "database",
            ),
        ];
        let (_, log) = test_support::capture(async {
            for (error, category) in &errors {
                assert_eq!(sqlx_kind(error), *category);
                log_sqlx_failure("claim", error);
                log_command_failure("Invoice", "unexpected", error);
            }
            for code in [None, Some("private_code"), Some("42p01"), Some("1234"), Some("123456")] {
                let error = sqlx::Error::Database(Box::new(DatabaseFault { code }));
                assert!(sqlstate(&error).is_none());
                log_sqlx_failure("settle", &error);
            }
            #[derive(Debug, thiserror::Error)]
            #[error("private_wrapper")]
            struct Wrapper(#[source] sqlx::Error);
            log_command_failure("Invoice", "unexpected", &Wrapper(sqlx::Error::PoolClosed));
        })
        .await;
        assert!(log.contains("42P01"));
        assert!(log.contains("storage_category=\"pool_closed\""));
        assert!(!log.contains("private_"));
        for (_, category) in errors {
            assert!(log.contains(category));
        }
    }
}
