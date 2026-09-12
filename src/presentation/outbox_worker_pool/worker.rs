use crate::application::outbox::{ClaimedTask, FailedTask, OutboxTaskCoordinator, SettleResult, TaskHandlerRegistry};
use std::{io::Write, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};

pub(super) struct OutboxWorker {
    pub(super) mailbox: Option<mpsc::Sender<ClaimedTask>>,
    pub(super) task: JoinHandle<()>,
    pub(super) busy: bool,
}

impl Drop for OutboxWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) fn spawn_worker(
    index: usize,
    mut handler: TaskHandlerRegistry,
    service: Arc<dyn OutboxTaskCoordinator>,
    completed: mpsc::Sender<usize>,
    timeout: Duration,
) -> OutboxWorker {
    let (mailbox, mut inbox) = mpsc::channel::<ClaimedTask>(1);
    let task = tokio::spawn(async move {
        while let Some(task) = inbox.recv().await {
            let id = task.id;
            match service.execute_and_settle(&mut handler, task, timeout).await {
                Ok(SettleResult::Settled { failed: Some(failed) }) => log_failed(&failed),
                Ok(SettleResult::Settled { failed: None }) => {}
                Ok(SettleResult::OwnershipLost) => eprintln!("outbox ownership lost; task_id={id}"),
                Err(error) => eprintln!("outbox acknowledgement failed; task_id={id}; error={error}"),
            }
            if completed.send(index).await.is_err() {
                break;
            }
        }
    });
    OutboxWorker {
        mailbox: Some(mailbox),
        task,
        busy: false,
    }
}

pub(super) fn log_failed(task: &FailedTask) {
    let _ = write_failed(std::io::stderr().lock(), task);
}

fn write_failed(mut output: impl Write, task: &FailedTask) -> std::io::Result<()> {
    let task_type = &task.format.task_type;
    let safe_type = if task_type.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        task_type.as_str()
    } else {
        "unknown"
    };
    writeln!(
        output,
        "outbox task failed; task_id={}; task_type={safe_type}; task_version={}; attempt={}; reason={}",
        task.id, task.format.task_version, task.attempt, task.reason
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::application::outbox::{
        FailedTask, OutboxError, TaskExecutionError, TaskFormat, TaskHandler, TaskHandlerRegistry, test_support::claimed,
    };

    struct RecordingCoordinator(AtomicUsize);

    #[async_trait]
    impl OutboxTaskCoordinator for RecordingCoordinator {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![]
        }

        async fn claim(&self, _: &str, _: usize, _: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
            Ok(vec![])
        }

        async fn recover_expired(&self, _: usize) -> Result<crate::application::outbox::RecoveryResult, OutboxError> {
            Ok(Default::default())
        }

        async fn execute_and_settle(
            &self,
            _: &mut dyn TaskHandler,
            _: ClaimedTask,
            _: Duration,
        ) -> Result<SettleResult, OutboxError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(SettleResult::Settled { failed: None })
        }
    }

    struct Handler;

    #[async_trait]
    impl TaskHandler for Handler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![claimed().format]
        }

        async fn handle(&mut self, _: &ClaimedTask) -> Result<(), TaskExecutionError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn receives_work_and_reports_completion() {
        let coordinator = Arc::new(RecordingCoordinator(AtomicUsize::new(0)));
        let handler = TaskHandlerRegistry::try_new(vec![Box::new(Handler)]).unwrap();
        let (completed, mut completion) = mpsc::channel(1);
        let mut worker = spawn_worker(3, handler, coordinator.clone(), completed, Duration::from_secs(1));
        worker.mailbox.as_ref().unwrap().send(claimed()).await.unwrap();
        assert_eq!(completion.recv().await, Some(3));
        assert_eq!(coordinator.0.load(Ordering::SeqCst), 1);
        worker.mailbox.take();
        (&mut worker.task).await.unwrap();
    }

    #[tokio::test]
    async fn stops_after_completion_receiver_closes() {
        let coordinator = Arc::new(RecordingCoordinator(AtomicUsize::new(0)));
        let handler = TaskHandlerRegistry::try_new(vec![Box::new(Handler)]).unwrap();
        let (completed, completion) = mpsc::channel(1);
        let mut worker = spawn_worker(0, handler, coordinator.clone(), completed, Duration::from_secs(1));
        drop(completion);
        worker.mailbox.as_ref().unwrap().send(claimed()).await.unwrap();
        (&mut worker.task).await.unwrap();
        assert_eq!(coordinator.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn confirmed_failure_lines_contain_identity_and_safe_reason_only() {
        for reason in ["permanent failure", "retry limit reached", "worker lease expired"] {
            let task = FailedTask {
                id: uuid::Uuid::from_u128(1),
                format: TaskFormat {
                    task_type: "create_account".into(),
                    task_version: 1,
                },
                attempt: 2,
                reason,
            };
            let mut output = Vec::new();
            write_failed(&mut output, &task).unwrap();
            let line = String::from_utf8(output).unwrap();
            assert!(line.contains("task_id=00000000-0000-0000-0000-000000000001"));
            assert!(line.contains("task_type=create_account; task_version=1; attempt=2"));
            assert!(line.contains(reason));
            assert!(!line.contains("alice@example.com"));
            assert!(!line.contains("token"));
        }
        let task = FailedTask {
            id: uuid::Uuid::from_u128(2),
            format: TaskFormat {
                task_type: "secret\nalice@example.com".into(),
                task_version: 1,
            },
            attempt: 1,
            reason: "worker lease expired",
        };
        let mut output = Vec::new();
        write_failed(&mut output, &task).unwrap();
        assert!(String::from_utf8(output).unwrap().contains("task_type=unknown"));
    }
}
