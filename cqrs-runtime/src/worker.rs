//! Inbound polling adapter: bounded async-channel delivery queue and permanent Tokio workers supervised by JoinSet.
//!
//! Workers execute eligible tasks concurrently without serializing source
//! aggregates or interpreting event sequence as delivery order. Aggregate
//! phases and accepted transitions establish business prerequisites before
//! application publishes dependent work. Settlement concerns one task's
//! operation, not advancement of aggregate state or completion of a workflow.
//!
//! Defaults are 128 registry-owning worker tasks, 128 queued deliveries, and a
//! 120-second lease. Concurrency is process-wide and independent of CPU threads.
//! Startup validates lease > 2*S + H + ceil(Q/C)*(H+S) + 5-second margin,
//! where S is storage timeout, H is handler timeout, Q is queue capacity and C
//! is concurrency. Per-delivery deadlines preserve the original remaining
//! ownership through queue waiting; lease renewal is not enabled.
//! One poller owns the sender; each worker has a cloned receiver. Claim limit is
//! Q minus current queue length; consumers can only free space during claim.
//! Shutdown sets internal stop before closing the channel.
//! Shutdown and restart-factory errors drain only started handler/settlement
//! operations, then abort and join workers at the common H+S deadline.

mod config;
mod supervisor;
mod worker_task;
pub use config::{OutboxWorkerPoolConfig, OutboxWorkerPoolConfigError};
pub use supervisor::{HandlerRegistryFactory, WorkerPoolError, run};
