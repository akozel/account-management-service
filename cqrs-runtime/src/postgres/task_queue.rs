//! PostgreSQL task ownership with transaction-local statement timeouts.
//!
//! Claims, recovery, and settlement use the canonical outbox schema. Each queue
//! transaction applies its own statement timeout, including lock waiting, and
//! restores the shared connection's previous settings on commit/rollback.
//! The poller and executor bound pool acquisition, SQL, commit, and responses.
//! Timeout does not establish rollback or cancel an already accepted remote effect.

mod claim;
pub mod config;
pub mod queue;
mod recovery;
mod settlement;
