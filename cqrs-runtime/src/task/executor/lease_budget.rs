use crate::task::queue::{error::TaskProcessingError, lease::LEASE_SAFETY_MARGIN};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ExecutionPlan {
    Run {
        handler_deadline: Instant,
        settlement_deadline: Instant,
    },
    RetryOnly {
        settlement_deadline: Instant,
    },
    RecoverOnly,
}
pub(super) fn execution_plan(
    now: Instant,
    lease_deadline: Instant,
    handler_timeout: Duration,
    storage_timeout: Duration,
) -> Result<ExecutionPlan, TaskProcessingError> {
    if handler_timeout.is_zero() {
        return Err(TaskProcessingError::InvalidExecutionBudget);
    }
    let required = handler_timeout
        .checked_add(storage_timeout)
        .ok_or(TaskProcessingError::InvalidExecutionBudget)?;
    let Some(settlement_deadline) = lease_deadline.checked_sub(LEASE_SAFETY_MARGIN) else {
        return Ok(ExecutionPlan::RecoverOnly);
    };
    if settlement_deadline <= now {
        return Ok(ExecutionPlan::RecoverOnly);
    }
    if settlement_deadline.duration_since(now) < required {
        return Ok(ExecutionPlan::RetryOnly { settlement_deadline });
    }
    let handler_deadline = now
        .checked_add(handler_timeout)
        .ok_or(TaskProcessingError::InvalidExecutionBudget)?;
    Ok(ExecutionPlan::Run {
        handler_deadline,
        settlement_deadline,
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_near_the_platform_clock_minimum_is_left_for_recovery() {
        let now = Instant::now();
        let mut seconds = 0u64;
        for bit in (0..64).rev() {
            let candidate = seconds | (1u64 << bit);
            if now.checked_sub(Duration::from_secs(candidate)).is_some() {
                seconds = candidate;
            }
        }
        let deadline = now.checked_sub(Duration::from_secs(seconds)).unwrap();
        assert!(deadline.checked_sub(LEASE_SAFETY_MARGIN).is_none());
        assert_eq!(
            execution_plan(now, deadline, Duration::from_secs(1), Duration::from_secs(1)),
            Ok(ExecutionPlan::RecoverOnly)
        );
    }
    #[test]
    fn exact_budget_runs_and_shorter_budgets_retry_or_recover() {
        let now = Instant::now();
        let handler = Duration::from_secs(30);
        let storage = Duration::from_secs(5);
        let deadline = now + handler + storage + LEASE_SAFETY_MARGIN;
        assert_eq!(
            execution_plan(now, deadline, handler, storage),
            Ok(ExecutionPlan::Run {
                handler_deadline: now + handler,
                settlement_deadline: deadline - LEASE_SAFETY_MARGIN
            })
        );
        assert!(matches!(
            execution_plan(now, deadline - Duration::from_nanos(1), handler, storage),
            Ok(ExecutionPlan::RetryOnly { .. })
        ));
        assert_eq!(
            execution_plan(now, now + LEASE_SAFETY_MARGIN, handler, storage),
            Ok(ExecutionPlan::RecoverOnly)
        );
        assert_eq!(execution_plan(now, now, handler, storage), Ok(ExecutionPlan::RecoverOnly));
        assert_eq!(
            execution_plan(now, deadline, Duration::MAX, storage),
            Err(TaskProcessingError::InvalidExecutionBudget)
        );
        assert_eq!(
            execution_plan(now, deadline, Duration::ZERO, storage),
            Err(TaskProcessingError::InvalidExecutionBudget)
        );
    }
}
