//! Task execution within fixed leases and bounded storage operations.
//!
//! Local retries/backoff share one handler invocation budget. Before execution,
//! the task must retain that full budget plus settlement time and the fixed
//! safety margin. A stale budget triggers retry without running the handler,
//! or durable recovery if even safe settlement no longer fits. Absolute local
//! deadlines complement the database's token/expiry fencing; they do not renew
//! ownership or cancel a side effect already accepted by a remote system.

pub mod default_executor;
mod lease_budget;
pub mod port;
