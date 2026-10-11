//! Inbound polling adapter: bounded async-channel delivery queue and permanent Tokio workers supervised by JoinSet.
//!
//! Prefer [`preparation::prepare`] before opening inbound listeners: checked [`config::TaskRuntimeConfig`]
//! and all C dispatchers are assembled without storage work or background tasks.
//! [`preparation::TaskRuntime::run`] consumes those same handlers. The low-level [`supervisor::run`]
//! accepts custom executors and shares the execution engine.
//!
//! Workers execute eligible tasks concurrently without serializing source
//! aggregates or interpreting event sequence as delivery order. Aggregate
//! phases and accepted transitions establish business prerequisites before
//! application publishes dependent work. Settlement concerns one task's
//! operation, not advancement of aggregate state or completion of a workflow.
//!
//! Defaults are 128 dispatcher-owning worker tasks, 128 queued deliveries, and a
//! 60-second lease. Concurrency is process-wide and independent of CPU threads.
//! Startup validates lease > 2*S + H + ceil(Q/C)*(H+S) + 5-second margin,
//! where S is storage timeout, H is handler timeout, Q is queue capacity and C
//! is concurrency. Per-delivery deadlines preserve the original remaining
//! ownership through queue waiting; lease renewal is not enabled.
//! One poller owns the sender; each worker has a cloned receiver. Claim limit is
//! Q minus current queue length; consumers can only free space during claim.
//! The host shares a CancellationToken with the supervisor and its workers.
//! Fatal supervisor errors cancel that token before drain, notifying other host
//! adapters immediately. Shutdown cancels the token before closing the channel.
//! Shutdown and restart-factory errors drain only started handler/settlement
//! operations, then abort and join workers at the common H+S deadline (21 seconds with default H=15s/S=6s).
//! Dropping/aborting a polled supervisor cancels its token through DropGuard and
//! aborts owned workers; graceful drain requires cancellation followed by await.

pub mod config;
pub mod dispatcher_factory;
mod poller;
pub mod preparation;
pub mod supervisor;
mod worker;
mod worker_pool;
pub mod worker_pool_config;
