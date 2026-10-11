//! Shared integer parsing; each configuration owns its units, validation and errors.

pub(crate) fn read_u64(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64, std::num::ParseIntError> {
    lookup(name)
        .map(|value| value.parse())
        .transpose()
        .map(|value| value.unwrap_or(default))
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::process::Command;

    pub(crate) fn scenario() -> Option<String> {
        std::env::var("CQRS_RUNTIME_TEST_ENV_SCENARIO").ok()
    }

    // Run one inline unit test with its own environment. No process-global
    // set_var/remove_var calls or locks against unrelated parallel tests.
    pub(crate) fn isolated(test: &str, scenario: &str, configure: impl FnOnce(&mut Command)) {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child.args(["--exact", test.split_once("::").unwrap().1, "--nocapture"]);
        for name in [
            "OUTBOX_STORAGE_TIMEOUT_MS",
            "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS",
            "OUTBOX_WORKER_CONCURRENCY",
            "OUTBOX_QUEUE_CAPACITY",
            "OUTBOX_POLL_INTERVAL_MS",
            "OUTBOX_HANDLER_TIMEOUT_SECS",
            "OUTBOX_LEASE_SECS",
        ] {
            child.env_remove(name);
        }
        child.env("CQRS_RUNTIME_TEST_ENV_SCENARIO", scenario);
        configure(&mut child);
        let result = child.output().unwrap();
        assert!(
            result.status.success(),
            "isolated {scenario} test failed:\n{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("1 passed"),
            "the requested unit test must run"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_parsing_preserves_absence_aliases_and_overflow_behavior() {
        assert_eq!(read_u64(&mut |_| None, "KEY", 42).unwrap(), 42);
        for value in ["0", "+0", "18446744073709551615"] {
            assert_eq!(
                read_u64(&mut |_| Some(value.into()), "KEY", 42).unwrap(),
                value.parse::<u64>().unwrap()
            );
        }
        for value in ["", "-1", " 1", "garbage", "18446744073709551616"] {
            assert!(read_u64(&mut |_| Some(value.into()), "KEY", 42).is_err());
        }
    }
}
