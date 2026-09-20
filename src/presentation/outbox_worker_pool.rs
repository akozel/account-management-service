//! Inbound polling adapter: bounded worker mailboxes, supervised by one dispatcher.

use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tracing::error;
use uuid::Uuid;

use crate::application::outbox::{OutboxTaskCoordinator, TaskFormat, TaskHandlerRegistry};
mod worker;
use worker::{log_failed, spawn_worker};

pub type TaskHandlerFactory = Arc<dyn Fn() -> TaskHandlerRegistry + Send + Sync>;

fn checked_handler(
    factory: &TaskHandlerFactory,
    formats: &[TaskFormat],
) -> Result<TaskHandlerRegistry, OutboxWorkerPoolConfigError> {
    let handler = factory();
    if handler.formats() == formats {
        Ok(handler)
    } else {
        Err(OutboxWorkerPoolConfigError("handler and dispatcher formats must match"))
    }
}

#[derive(Clone, Debug)]
pub struct OutboxWorkerPoolConfig {
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub handler_timeout: Duration,
    pub lease: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid outbox worker configuration: {0}")]
pub struct OutboxWorkerPoolConfigError(pub &'static str);

impl Default for OutboxWorkerPoolConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            poll_interval: Duration::from_secs(1),
            handler_timeout: Duration::from_secs(30),
            lease: Duration::from_secs(60),
        }
    }
}

impl OutboxWorkerPoolConfig {
    pub fn from_env() -> Result<Self, OutboxWorkerPoolConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, OutboxWorkerPoolConfigError> {
        let mut number = |name, default| match lookup(name) {
            Some(value) => value.parse::<u64>().map_err(|_| OutboxWorkerPoolConfigError(name)),
            None => Ok(default),
        };
        let config = Self {
            concurrency: usize::try_from(number("OUTBOX_WORKER_CONCURRENCY", 4)?)
                .map_err(|_| OutboxWorkerPoolConfigError("OUTBOX_WORKER_CONCURRENCY"))?,
            poll_interval: Duration::from_millis(number("OUTBOX_POLL_INTERVAL_MS", 1000)?),
            handler_timeout: Duration::from_secs(number("OUTBOX_HANDLER_TIMEOUT_SECS", 30)?),
            lease: Duration::from_secs(number("OUTBOX_LEASE_SECS", 60)?),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), OutboxWorkerPoolConfigError> {
        if self.concurrency == 0
            || self.poll_interval.is_zero()
            || self.handler_timeout.is_zero()
            || self.handler_timeout >= self.lease
            || self.lease.as_millis() > i64::MAX as u128
            || self.poll_interval.as_millis() > i64::MAX as u128
        {
            return Err(OutboxWorkerPoolConfigError(
                "positive values and handler_timeout < lease are required",
            ));
        }
        Ok(())
    }
}

pub async fn run(
    service: Arc<dyn OutboxTaskCoordinator>,
    factory: TaskHandlerFactory,
    config: OutboxWorkerPoolConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), OutboxWorkerPoolConfigError> {
    config.validate()?;
    let formats = service.formats();
    let owner = Uuid::new_v4().to_string();
    let (completed, mut completions) = mpsc::channel(config.concurrency);
    let mut workers = Vec::with_capacity(config.concurrency);
    for i in 0..config.concurrency {
        workers.push(spawn_worker(
            i,
            checked_handler(&factory, &formats)?,
            service.clone(),
            completed.clone(),
            config.handler_timeout,
        ));
    }
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    while !*shutdown.borrow() {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = interval.tick() => {
                for (i, worker) in workers.iter_mut().enumerate() {
                    if worker.task.is_finished() {
                        error!(worker = i, owner = %owner, "outbox worker stopped; restarting");
                        *worker = spawn_worker(i, checked_handler(&factory, &formats)?, service.clone(), completed.clone(), config.handler_timeout);
                    }
                }
                let recovery = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    result = service.recover_expired(config.concurrency) => result,
                };
                match recovery {
                    Ok(result) => for failed in result.failed { log_failed(&failed); },
                    Err(error) => error!(error = %error, "outbox lease recovery failed"),
                }
            }
            Some(index) = completions.recv() => workers[index].busy = false,
        }
        if *shutdown.borrow() {
            break;
        }
        let free: Vec<_> = workers
            .iter()
            .enumerate()
            .filter_map(|(i, worker)| (!worker.busy).then_some(i))
            .collect();
        if free.is_empty() {
            continue;
        }
        let claimed = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            result = service.claim(&owner, free.len(), config.lease) => result,
        };
        match claimed {
            Ok(tasks) => {
                for (index, task) in free.into_iter().zip(tasks) {
                    let worker = &mut workers[index];
                    if worker.mailbox.as_ref().is_some_and(|mailbox| mailbox.try_send(task).is_ok()) {
                        worker.busy = true;
                    }
                    // If the worker died after claim, lease recovery will preserve this work.
                }
            }
            Err(error) => error!(error = %error, "outbox claim failed"),
        }
    }
    for worker in &mut workers {
        worker.mailbox.take();
    }
    // Draining acknowledgements must not block workers on the completion channel.
    drop(completions);
    let _ = tokio::time::timeout(config.handler_timeout + Duration::from_secs(5), async {
        for worker in &mut workers {
            let _ = (&mut worker.task).await;
        }
    })
    .await;
    // OutboxWorker::drop aborts tasks remaining after the drain deadline (or cancellation).
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::email_reservation::tasks::send_verification_code::send_verification_code_task_handler;
    use crate::application::outbox::{test_support::*, *};
    use async_trait::async_trait;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::watch;

    #[test]
    fn worker_config_supports_defaults_overrides_and_rejects_invalid_values() {
        let default = OutboxWorkerPoolConfig::from_lookup(|_| None).unwrap();
        assert_eq!(default.concurrency, 4);
        assert_eq!(default.lease, Duration::from_secs(60));
        let custom = OutboxWorkerPoolConfig::from_lookup(|name| {
            Some(
                match name {
                    "OUTBOX_WORKER_CONCURRENCY" => "2",
                    "OUTBOX_POLL_INTERVAL_MS" => "50",
                    "OUTBOX_HANDLER_TIMEOUT_SECS" => "10",
                    "OUTBOX_LEASE_SECS" => "20",
                    _ => unreachable!(),
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(custom.concurrency, 2);
        assert_eq!(custom.poll_interval, Duration::from_millis(50));
        for (key, value) in [
            ("OUTBOX_WORKER_CONCURRENCY", "garbage"),
            ("OUTBOX_WORKER_CONCURRENCY", "0"),
            ("OUTBOX_POLL_INTERVAL_MS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "60"),
            ("OUTBOX_LEASE_SECS", "0"),
            ("OUTBOX_LEASE_SECS", "18446744073709551615"),
            ("OUTBOX_POLL_INTERVAL_MS", "18446744073709551615"),
        ] {
            assert!(
                OutboxWorkerPoolConfig::from_lookup(|name| (name == key).then(|| value.into())).is_err(),
                "{key}={value}"
            );
        }
    }

    #[tokio::test]
    async fn supervisor_recovers_workers_and_survives_storage_errors_and_lost_acknowledgements() {
        let queue = Arc::new(Queue::default());
        queue.claim_error.store(true, Ordering::SeqCst);
        queue.recovery_error.store(true, Ordering::SeqCst);
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let calls = Arc::new(AtomicUsize::new(0));
        struct PanicOnce {
            calls: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl TaskHandler for PanicOnce {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
                assert_ne!(self.calls.fetch_add(1, Ordering::SeqCst), 0, "injected worker failure");
                Ok(())
            }
        }
        let observed = calls.clone();
        let factory: TaskHandlerFactory =
            Arc::new(move || TaskHandlerRegistry::try_new(vec![Box::new(PanicOnce { calls: observed.clone() })]).unwrap());
        let config = OutboxWorkerPoolConfig {
            concurrency: 1,
            poll_interval: Duration::from_millis(2),
            ..Default::default()
        };
        let (stop, shutdown) = watch::channel(false);
        let worker = tokio::spawn(super::run(service, factory, config, shutdown));
        wait_until(|| queue.recovered.load(Ordering::SeqCst) >= 2).await;
        queue.claim_error.store(false, Ordering::SeqCst);
        queue.recovery_error.store(false, Ordering::SeqCst);
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        wait_until(|| calls.load(Ordering::SeqCst) >= 2 && !queue.outcomes.lock().unwrap().is_empty()).await;
        // An acknowledgement failure leaves durable recovery to the queue's lease mechanism.
        queue.settle_error.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        wait_until(|| queue.outcomes.lock().unwrap().len() >= 2).await;
        queue.settle_error.store(false, Ordering::SeqCst);
        queue.lost_owner.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        wait_until(|| queue.outcomes.lock().unwrap().len() >= 3).await;
        stop.send(true).unwrap();
        worker.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn shutdown_drains_in_flight_work_and_invalid_worker_wiring_is_rejected() {
        let queue = Arc::new(Queue::default());
        let service = outbox_task_coordinator(queue.clone(), vec![format()]);
        let sender = sender();
        let seen = sender.records.clone();
        let factory: TaskHandlerFactory = Arc::new(move || {
            TaskHandlerRegistry::try_new(vec![send_verification_code_task_handler(Box::new(sender.clone()))]).unwrap()
        });
        let (stop, shutdown) = watch::channel(false);
        let invalid = OutboxWorkerPoolConfig {
            concurrency: 0,
            ..Default::default()
        };
        assert!(
            super::run(service.clone(), factory.clone(), invalid, shutdown.clone())
                .await
                .is_err()
        );
        assert!(
            super::run(
                outbox_task_coordinator(queue.clone(), vec![]),
                factory.clone(),
                OutboxWorkerPoolConfig::default(),
                shutdown.clone()
            )
            .await
            .is_err()
        );
        queue.incoming.lock().unwrap().push_back(claimed());
        let worker = tokio::spawn(super::run(service, factory, OutboxWorkerPoolConfig::default(), shutdown));
        wait_until(|| !seen.lock().unwrap().is_empty()).await;
        stop.send(true).unwrap();
        worker.await.unwrap().unwrap();
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
    }

    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_stalled_polling_and_bounds_stalled_acknowledgements() {
        for stage in ["claim", "recovery", "settle"] {
            let queue = Arc::new(Queue::default());
            queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
            queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
            queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
            queue.incoming.lock().unwrap().push_back(claimed());
            let service = outbox_task_coordinator(queue.clone(), vec![format()]);
            let factory: TaskHandlerFactory =
                Arc::new(|| TaskHandlerRegistry::try_new(vec![send_verification_code_task_handler(Box::new(sender()))]).unwrap());
            let (stop, shutdown) = watch::channel(false);
            let worker = tokio::spawn(super::run(service, factory, OutboxWorkerPoolConfig::default(), shutdown));
            wait_until(|| match stage {
                "claim" => !queue.requested.lock().unwrap().is_empty(),
                "recovery" => queue.recovered.load(Ordering::SeqCst) > 0,
                "settle" => !queue.outcomes.lock().unwrap().is_empty(),
                _ => unreachable!(),
            })
            .await;
            let start = tokio::time::Instant::now();
            stop.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(36), worker)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(start.elapsed() <= Duration::from_secs(35));
        }
    }

    #[tokio::test]
    async fn validates_every_workers_registry_before_polling() {
        struct OtherHandler;

        #[async_trait]
        impl TaskHandler for OtherHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![TaskFormat {
                    task_type: "other".into(),
                    task_version: 1,
                }]
            }

            async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
                Ok(())
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let factory: TaskHandlerFactory = Arc::new({
            let calls = calls.clone();
            move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    TaskHandlerRegistry::try_new(vec![send_verification_code_task_handler(Box::new(sender()))]).unwrap()
                } else {
                    TaskHandlerRegistry::try_new(vec![Box::new(OtherHandler)]).unwrap()
                }
            }
        });
        let service = outbox_task_coordinator(Arc::new(Queue::default()), vec![format()]);
        let (_, shutdown) = watch::channel(false);
        let result = super::run(
            service,
            factory,
            OutboxWorkerPoolConfig {
                concurrency: 2,
                ..Default::default()
            },
            shutdown,
        )
        .await;
        assert_eq!(
            result,
            Err(OutboxWorkerPoolConfigError("handler and dispatcher formats must match"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
