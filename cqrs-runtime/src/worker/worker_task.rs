//! One mutable registry per permanent Tokio task; receiver locking never covers execution.
use crate::outbox::{ClaimedTask, OutboxTaskCoordinator, SettlementResult, TaskFailure, TaskHandlerRegistry};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify, mpsc, watch};
use tracing::error;

pub(super) struct WorkerInbox {
    receiver: Mutex<mpsc::Receiver<ClaimedTask>>,
    pub(super) available: Notify,
}
impl WorkerInbox {
    pub(super) fn new(receiver: mpsc::Receiver<ClaimedTask>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            available: Notify::new(),
        }
    }
}

/// True or sender closure requests shutdown; false updates do not.
pub(super) async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}

pub(super) async fn run_worker(
    index: usize,
    mut handler: TaskHandlerRegistry,
    coordinator: Arc<dyn OutboxTaskCoordinator>,
    inbox: Arc<WorkerInbox>,
    handler_timeout: Duration,
    mut stop: watch::Receiver<bool>,
) -> usize {
    loop {
        let task = tokio::select! {
            biased;
            _ = stop_requested(&mut stop) => break,
            task = async {
                let mut receiver = inbox.receiver.lock().await;
                receiver.recv().await
            } => task,
        };
        let Some(task) = task else {
            break;
        };
        inbox.available.notify_one();
        if *stop.borrow() {
            break;
        }
        let id = task.delivery().id();
        // Graceful stop does not cancel the current invocation or settlement.
        match coordinator.execute_and_settle(&mut handler, task, handler_timeout).await {
            Ok(SettlementResult::Failed(failed)) => log_failed(&failed),
            Ok(SettlementResult::Completed | SettlementResult::RetryScheduled) => {}
            Ok(SettlementResult::OwnershipLost) => error!(task_id = %id, "outbox ownership lost"),
            Err(error) => error!(task_id = %id, error = %error, "outbox acknowledgement failed"),
        }
    }
    index
}

pub(super) fn log_failed(task: &TaskFailure) {
    let task_type = task.format.task_type();
    let safe_type = if task_type.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        task_type
    } else {
        "unknown"
    };
    error!(
        task_id = %task.id,
        task_type = %safe_type,
        task_version = task.format.task_version(),
        attempt = task.attempt,
        reason = %task.cause.as_str(),
        "outbox task failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{TaskFailureCause, TaskFormat};
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::fmt::MakeWriter;
    #[derive(Clone, Default)]
    struct RecordingWriter(Arc<Mutex<Vec<u8>>>);

    impl RecordingWriter {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for RecordingWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn confirmed_failure_events_contain_identity_and_safe_reason_only() {
        let writer = RecordingWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for cause in [
                TaskFailureCause::HandlerPermanent,
                TaskFailureCause::AttemptLimitReached,
                TaskFailureCause::LeaseExpired,
            ] {
                let task = TaskFailure {
                    id: uuid::Uuid::from_u128(1),
                    format: TaskFormat::new("create_account", 1).unwrap(),
                    attempt: 2,
                    cause,
                };
                log_failed(&task);
            }
            let task = TaskFailure {
                id: uuid::Uuid::from_u128(2),
                format: TaskFormat::new("secret\nalice@example.com", 1).unwrap(),
                attempt: 1,
                cause: TaskFailureCause::LeaseExpired,
            };
            log_failed(&task);
        });

        let log = writer.text();
        assert_eq!(log.lines().count(), 4);
        assert!(log.contains("task_id=00000000-0000-0000-0000-000000000001"));
        assert!(log.contains("task_type=create_account task_version=1 attempt=2"));
        for reason in ["permanent failure", "retry limit reached", "worker lease expired"] {
            assert!(log.contains(reason));
        }
        assert!(log.contains("task_type=unknown"));
        assert!(!log.contains("secret"));
        assert!(!log.contains("alice@example.com"));
        assert!(!log.contains("token"));
    }

    mod performance {
        use super::*;
        use crate::outbox::{
            ClaimLease, LeaseRecoveryReport, OutboxError, TaskDelivery, TaskFormat, TaskHandler, TaskHandlerError,
        };
        use async_trait::async_trait;
        use std::{
            hint::black_box,
            sync::{
                Mutex as StdMutex,
                atomic::{AtomicU64, AtomicUsize, Ordering},
            },
            time::Instant,
        };
        use tokio::{runtime::Builder, task::JoinSet};
        use uuid::Uuid;

        const SAMPLE_EVERY: usize = 32;
        const REPETITIONS: usize = 8;

        #[derive(Clone, Copy, Debug)]
        enum Backend {
            Current,
            AsyncChannel,
            Flume,
            Kanal,
        }
        const BACKENDS: [Backend; 4] = [Backend::Current, Backend::AsyncChannel, Backend::Flume, Backend::Kanal];

        fn backend_order(repetition: usize) -> [Backend; 4] {
            let mut order = BACKENDS;
            if repetition >= BACKENDS.len() {
                order.reverse();
            }
            order.rotate_left(repetition % BACKENDS.len());
            order
        }
        impl Backend {
            fn name(self) -> &'static str {
                match self {
                    Self::Current => "tokio_mutex",
                    Self::AsyncChannel => "async_channel",
                    Self::Flume => "flume",
                    Self::Kanal => "kanal",
                }
            }
        }
        #[derive(Clone, Copy, Debug)]
        enum Workload {
            Fast,
            Yield,
            Io(Duration),
        }
        impl Workload {
            fn name(self) -> &'static str {
                match self {
                    Self::Fast => "fast",
                    Self::Yield => "yield",
                    Self::Io(_) => "io",
                }
            }
        }
        #[derive(Clone, Copy)]
        struct Case {
            workers: usize,
            capacity: usize,
            messages: usize,
            workload: Workload,
        }

        #[derive(Default)]
        struct WorkerStats {
            count: usize,
            checksum: u128,
            xor: u128,
            last_completion: Option<Instant>,
            // Samples stay worker-local; no shared statistics lock per delivery.
            queue_ns: Vec<u64>,
            delivery_ns: Vec<u64>,
        }
        struct Measurement {
            elapsed: Duration,
            queue_ns: Vec<u64>,
            delivery_ns: Vec<u64>,
        }
        struct State {
            epoch: Instant,
            warmup: usize,
            warmed: AtomicUsize,
            enqueued_ns: Vec<AtomicU64>,
            finished: StdMutex<Vec<WorkerStats>>,
        }
        struct MeasuredHandler {
            state: Arc<State>,
            workload: Workload,
            stats: WorkerStats,
        }
        impl Drop for MeasuredHandler {
            fn drop(&mut self) {
                // Once per worker, after its receive loop exits; excluded from elapsed.
                self.state.finished.lock().unwrap().push(std::mem::take(&mut self.stats));
            }
        }
        #[async_trait]
        impl TaskHandler for MeasuredHandler {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            async fn handle(&mut self, task: &TaskDelivery) -> Result<(), TaskHandlerError> {
                let index = task.id().as_u128() as usize;
                let measured = index >= self.state.warmup;
                let sampled = measured && (index - self.state.warmup).is_multiple_of(SAMPLE_EVERY);
                let enqueued = if sampled {
                    let enqueued = self.state.enqueued_ns[index].load(Ordering::Acquire);
                    self.stats
                        .queue_ns
                        .push(self.state.epoch.elapsed().as_nanos() as u64 - enqueued);
                    enqueued
                } else {
                    0
                };
                match self.workload {
                    Workload::Fast => {
                        black_box(task.payload());
                    }
                    Workload::Yield => tokio::task::yield_now().await,
                    Workload::Io(delay) => tokio::time::sleep(delay).await,
                }
                if measured {
                    self.stats.count += 1;
                    self.stats.checksum += index as u128;
                    self.stats.xor ^= index as u128;
                    let completed = Instant::now();
                    self.stats.last_completion = Some(completed);
                    if sampled {
                        self.stats
                            .delivery_ns
                            .push(completed.duration_since(self.state.epoch).as_nanos() as u64 - enqueued);
                    }
                } else {
                    self.state.warmed.fetch_add(1, Ordering::Release);
                }
                Ok(())
            }
        }

        // Identical coordinator/handler dispatch in both variants. No PostgreSQL,
        // timeout/lease calculations or remote HTTP: this isolates the local queue path.
        struct ImmediateCoordinator;
        #[async_trait]
        impl OutboxTaskCoordinator for ImmediateCoordinator {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![format()]
            }
            fn storage_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }
            async fn claim(&self, _: &str, _: usize, _: Duration) -> Result<Vec<ClaimedTask>, OutboxError> {
                unreachable!("synthetic producer supplies deliveries")
            }
            async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, OutboxError> {
                unreachable!("no database ownership in queue benchmark")
            }
            async fn execute_and_settle(
                &self,
                handler: &mut dyn TaskHandler,
                task: ClaimedTask,
                _: Duration,
            ) -> Result<SettlementResult, OutboxError> {
                handler.handle(task.delivery()).await.unwrap();
                Ok(SettlementResult::Completed)
            }
        }
        fn format() -> TaskFormat {
            TaskFormat::new("queue_benchmark", 1).unwrap()
        }

        // Static dispatch keeps adapter overhead equal and avoids boxed recv futures.
        trait BenchmarkReceiver: Clone + Send + Sync + 'static {
            fn receive(&self) -> impl std::future::Future<Output = Option<ClaimedTask>> + Send;
        }
        impl BenchmarkReceiver for async_channel::Receiver<ClaimedTask> {
            async fn receive(&self) -> Option<ClaimedTask> {
                self.recv().await.ok()
            }
        }
        impl BenchmarkReceiver for flume::Receiver<ClaimedTask> {
            async fn receive(&self) -> Option<ClaimedTask> {
                self.recv_async().await.ok()
            }
        }
        impl BenchmarkReceiver for kanal::AsyncReceiver<ClaimedTask> {
            async fn receive(&self) -> Option<ClaimedTask> {
                self.recv().await.ok()
            }
        }

        // Candidate changes only the receiver and its closed-channel result. Keep
        // shutdown checks, Notify and result classification aligned with run_worker.
        async fn run_mpmc_worker<R: BenchmarkReceiver>(
            index: usize,
            mut handler: TaskHandlerRegistry,
            coordinator: Arc<dyn OutboxTaskCoordinator>,
            receiver: R,
            available: Arc<Notify>,
            mut stop: watch::Receiver<bool>,
        ) -> usize {
            loop {
                let task = tokio::select! {
                    biased;
                    _ = stop_requested(&mut stop) => break,
                    task = receiver.receive() => task,
                };
                let Some(task) = task else {
                    break;
                };
                available.notify_one();
                if *stop.borrow() {
                    break;
                }
                let id = task.delivery().id();
                match coordinator
                    .execute_and_settle(&mut handler, task, Duration::from_secs(30))
                    .await
                {
                    Ok(SettlementResult::Failed(failed)) => log_failed(&failed),
                    Ok(SettlementResult::Completed | SettlementResult::RetryScheduled) => {}
                    Ok(SettlementResult::OwnershipLost) => error!(task_id = %id, "outbox ownership lost"),
                    Err(error) => error!(task_id = %id, error = %error, "outbox acknowledgement failed"),
                }
            }
            index
        }

        enum Producer {
            Current {
                sender: mpsc::Sender<ClaimedTask>,
                inbox: Arc<WorkerInbox>,
            },
            AsyncChannel {
                sender: async_channel::Sender<ClaimedTask>,
                available: Arc<Notify>,
                capacity: usize,
            },
            Flume {
                sender: flume::Sender<ClaimedTask>,
                available: Arc<Notify>,
                capacity: usize,
            },
            Kanal {
                sender: kanal::AsyncSender<ClaimedTask>,
                available: Arc<Notify>,
                capacity: usize,
            },
        }
        impl Producer {
            async fn publish(&self, tasks: impl IntoIterator<Item = ClaimedTask>, state: &State) {
                let mut tasks = tasks.into_iter().peekable();
                while tasks.peek().is_some() {
                    match self {
                        Self::Current { sender, inbox } => {
                            let free = sender.capacity();
                            if free == 0 {
                                inbox.available.notified().await;
                                continue;
                            }
                            let permits = sender.try_reserve_many(free).unwrap();
                            for permit in permits {
                                let Some(task) = tasks.next() else {
                                    break;
                                };
                                stamp(&task, state);
                                permit.send(task);
                            }
                        }
                        Self::AsyncChannel {
                            sender,
                            available,
                            capacity,
                        } => {
                            // Exactly one producer: consumers can only make more room
                            // after this snapshot. There is no concurrent claim/send.
                            let free = capacity - sender.len();
                            if free == 0 {
                                available.notified().await;
                                continue;
                            }
                            for _ in 0..free {
                                let Some(task) = tasks.next() else {
                                    break;
                                };
                                stamp(&task, state);
                                sender.try_send(task).unwrap();
                            }
                        }
                        Self::Flume {
                            sender,
                            available,
                            capacity,
                        } => {
                            let free = capacity - sender.len();
                            if free == 0 {
                                available.notified().await;
                                continue;
                            }
                            for _ in 0..free {
                                let Some(task) = tasks.next() else {
                                    break;
                                };
                                stamp(&task, state);
                                sender.try_send(task).unwrap();
                            }
                        }
                        Self::Kanal {
                            sender,
                            available,
                            capacity,
                        } => {
                            let free = capacity - sender.len();
                            if free == 0 {
                                available.notified().await;
                                continue;
                            }
                            for _ in 0..free {
                                let Some(task) = tasks.next() else {
                                    break;
                                };
                                stamp(&task, state);
                                // Ok(false) is a failed send that drops the message, not success.
                                assert!(
                                    sender.try_send(task).unwrap(),
                                    "reserved free capacity must accept the message"
                                );
                            }
                        }
                    }
                }
            }
        }
        fn stamp(task: &ClaimedTask, state: &State) {
            let index = task.delivery().id().as_u128() as usize;
            if index >= state.warmup && (index - state.warmup).is_multiple_of(SAMPLE_EVERY) {
                state.enqueued_ns[index].store(state.epoch.elapsed().as_nanos() as u64, Ordering::Release);
            }
        }

        fn spawn_mpmc_workers<R: BenchmarkReceiver>(
            workers: &mut JoinSet<usize>,
            handlers: Vec<TaskHandlerRegistry>,
            receiver: R,
            available: Arc<Notify>,
            shutdown: watch::Receiver<bool>,
        ) {
            let coordinator: Arc<dyn OutboxTaskCoordinator> = Arc::new(ImmediateCoordinator);
            for (index, handler) in handlers.into_iter().enumerate() {
                workers.spawn(run_mpmc_worker(
                    index,
                    handler,
                    coordinator.clone(),
                    receiver.clone(),
                    available.clone(),
                    shutdown.clone(),
                ));
            }
        }

        fn start_pool(
            backend: Backend,
            case: Case,
            state: Arc<State>,
            shutdown: watch::Receiver<bool>,
        ) -> (Producer, JoinSet<usize>) {
            let coordinator: Arc<dyn OutboxTaskCoordinator> = Arc::new(ImmediateCoordinator);
            let mut workers = JoinSet::new();
            let handlers = (0..case.workers)
                .map(|_| {
                    TaskHandlerRegistry::try_new(vec![Box::new(MeasuredHandler {
                        state: state.clone(),
                        workload: case.workload,
                        stats: WorkerStats {
                            queue_ns: Vec::with_capacity(32),
                            delivery_ns: Vec::with_capacity(32),
                            ..Default::default()
                        },
                    })])
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let producer = match backend {
                Backend::Current => {
                    let (sender, receiver) = mpsc::channel(case.capacity);
                    let inbox = Arc::new(WorkerInbox::new(receiver));
                    for (index, handler) in handlers.into_iter().enumerate() {
                        workers.spawn(run_worker(
                            index,
                            handler,
                            coordinator.clone(),
                            inbox.clone(),
                            Duration::from_secs(30),
                            shutdown.clone(),
                        ));
                    }
                    Producer::Current { sender, inbox }
                }
                Backend::AsyncChannel => {
                    let (sender, receiver) = async_channel::bounded(case.capacity);
                    let available = Arc::new(Notify::new());
                    spawn_mpmc_workers(&mut workers, handlers, receiver, available.clone(), shutdown.clone());
                    Producer::AsyncChannel {
                        sender,
                        available,
                        capacity: case.capacity,
                    }
                }
                Backend::Flume => {
                    let (sender, receiver) = flume::bounded(case.capacity);
                    let available = Arc::new(Notify::new());
                    spawn_mpmc_workers(&mut workers, handlers, receiver, available.clone(), shutdown.clone());
                    Producer::Flume {
                        sender,
                        available,
                        capacity: case.capacity,
                    }
                }
                Backend::Kanal => {
                    let (sender, receiver) = kanal::bounded_async(case.capacity);
                    let available = Arc::new(Notify::new());
                    spawn_mpmc_workers(&mut workers, handlers, receiver, available.clone(), shutdown.clone());
                    Producer::Kanal {
                        sender,
                        available,
                        capacity: case.capacity,
                    }
                }
            };
            (producer, workers)
        }

        async fn measure(backend: Backend, case: Case) -> Measurement {
            let warmup = case.workers * 2;
            let total = warmup + case.messages;
            // DTO/JSON/UUID construction and registry allocation precede measurement.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            let tasks: Vec<_> = (0..total)
                .map(|index| {
                    ClaimedTask::new(
                        TaskDelivery::new(
                            Uuid::from_u128(index as u128),
                            format(),
                            serde_json::json!({"body": "x".repeat(256)}),
                            serde_json::json!({}),
                            1,
                        )
                        .unwrap(),
                        ClaimLease::new(Uuid::from_u128(index as u128), deadline),
                    )
                })
                .collect();
            let state = Arc::new(State {
                epoch: Instant::now(),
                warmup,
                warmed: AtomicUsize::new(0),
                enqueued_ns: (0..total).map(|_| AtomicU64::new(0)).collect(),
                finished: StdMutex::new(Vec::with_capacity(case.workers)),
            });
            let (stop, shutdown) = watch::channel(false);
            let (producer, mut workers) = start_pool(backend, case, state.clone(), shutdown);
            let mut tasks = tasks.into_iter();
            producer.publish(tasks.by_ref().take(warmup), &state).await;
            while state.warmed.load(Ordering::Acquire) != warmup {
                tokio::task::yield_now().await;
            }
            let started = Instant::now();
            producer.publish(tasks, &state).await;
            // End-of-input drains messages; no shutdown signal cancels the finite batch.
            drop(producer);
            while let Some(exit) = workers.join_next().await {
                exit.unwrap();
            }
            drop(stop);
            let stats = state.finished.lock().unwrap();
            assert_eq!(stats.len(), case.workers);
            assert_eq!(stats.iter().map(|worker| worker.count).sum::<usize>(), case.messages);
            let expected_checksum: u128 = (warmup..total).map(|index| index as u128).sum();
            assert_eq!(stats.iter().map(|worker| worker.checksum).sum::<u128>(), expected_checksum);
            assert_eq!(
                stats.iter().fold(0, |sum, worker| sum ^ worker.xor),
                (warmup..total).fold(0, |sum, index| sum ^ index as u128)
            );
            let completed = stats.iter().filter_map(|worker| worker.last_completion).max().unwrap();
            Measurement {
                elapsed: completed.duration_since(started),
                queue_ns: stats.iter().flat_map(|worker| worker.queue_ns.iter().copied()).collect(),
                delivery_ns: stats.iter().flat_map(|worker| worker.delivery_ns.iter().copied()).collect(),
            }
        }
        fn percentile(samples: &[u64], percent: usize) -> f64 {
            assert!(!samples.is_empty());
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)] as f64 / 1000.0
        }

        #[test]
        fn comparison_smoke_checks_both_backends_and_all_workloads() {
            for threads in [1, 4] {
                let runtime = runtime(threads);
                runtime.block_on(async {
                    for workload in [Workload::Fast, Workload::Yield, Workload::Io(Duration::from_millis(2))] {
                        for backend in BACKENDS {
                            let case = Case {
                                workers: 2,
                                capacity: 3,
                                messages: 128,
                                workload,
                            };
                            let measurement = measure(backend, case).await;
                            assert_eq!(measurement.queue_ns.len(), 4);
                            assert_eq!(measurement.delivery_ns.len(), 4);
                            assert!(measurement.elapsed > Duration::ZERO);
                            assert!(percentile(&measurement.queue_ns, 50) <= percentile(&measurement.queue_ns, 99));
                            assert!(percentile(&measurement.delivery_ns, 95) >= percentile(&measurement.queue_ns, 50));
                        }
                    }
                });
            }
        }
        #[test]
        fn idle_workers_cancel_pending_receives_on_shutdown() {
            for threads in [1, 4] {
                runtime(threads).block_on(async {
                    for backend in BACKENDS {
                        let case = Case {
                            workers: 2,
                            capacity: 1,
                            messages: 0,
                            workload: Workload::Fast,
                        };
                        let state = Arc::new(State {
                            epoch: Instant::now(),
                            warmup: 0,
                            warmed: AtomicUsize::new(0),
                            enqueued_ns: Vec::new(),
                            finished: StdMutex::new(Vec::new()),
                        });
                        let (stop, shutdown) = watch::channel(false);
                        let (producer, mut workers) = start_pool(backend, case, state.clone(), shutdown);
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        assert_eq!(workers.len(), 2);
                        stop.send(true).unwrap();
                        tokio::time::timeout(Duration::from_secs(1), async {
                            while let Some(exit) = workers.join_next().await {
                                exit.unwrap();
                            }
                        })
                        .await
                        .unwrap();
                        assert_eq!(state.finished.lock().unwrap().len(), 2);
                        assert!(state.finished.lock().unwrap().iter().all(|stats| stats.count == 0));
                        drop(producer);
                    }
                });
            }
        }

        #[test]
        fn eight_orders_balance_positions_and_pairwise_precedence() {
            let mut positions = [[0; 4]; 4];
            let mut precedes = [[0; 4]; 4];
            for repetition in 0..REPETITIONS {
                let order = backend_order(repetition);
                for (position, backend) in order.into_iter().enumerate() {
                    let index = BACKENDS
                        .iter()
                        .position(|candidate| candidate.name() == backend.name())
                        .unwrap();
                    positions[index][position] += 1;
                    for other in order.iter().skip(position + 1) {
                        let other_index = BACKENDS
                            .iter()
                            .position(|candidate| candidate.name() == other.name())
                            .unwrap();
                        precedes[index][other_index] += 1;
                    }
                }
            }
            assert!(positions.iter().flatten().all(|&count| count == 2));
            for (index, row) in precedes.iter().enumerate() {
                for (other, &count) in row.iter().enumerate() {
                    if index != other {
                        assert_eq!(count, 4);
                    }
                }
            }
        }

        fn runtime(threads: usize) -> tokio::runtime::Runtime {
            if threads == 1 {
                Builder::new_current_thread().enable_all().build().unwrap()
            } else {
                Builder::new_multi_thread()
                    .worker_threads(threads)
                    .enable_all()
                    .build()
                    .unwrap()
            }
        }

        fn run_comparison(runtime_threads: &[usize], workloads: &[Workload], io_deliveries_per_worker: usize) {
            if cfg!(debug_assertions) {
                panic!("run this performance test with --release");
            }
            println!(
                "\nBENCH_META,os={},arch={},available_parallelism={},sample_every={},repetitions={},io_deliveries_per_worker={},backends=tokio_mutex|async_channel|flume|kanal,async_channel=2.5.0,flume=0.12.0-default,kanal=0.1.1-default",
                std::env::consts::OS,
                std::env::consts::ARCH,
                std::thread::available_parallelism().unwrap().get(),
                SAMPLE_EVERY,
                REPETITIONS,
                io_deliveries_per_worker
            );
            println!(
                "BENCH,runtime_threads,workers,capacity,workload,io_ms,backend,iteration,order_position,messages,elapsed_ms,msg_per_second,queue_p50_us,queue_p95_us,queue_p99_us,delivery_p95_us"
            );
            for &threads in runtime_threads {
                let runtime = runtime(threads);
                runtime.block_on(async {
                    for c in [128, 1000] {
                        for &workload in workloads {
                            let messages = match workload { Workload::Fast => 50_000, Workload::Yield => 20_000, Workload::Io(_) => c * io_deliveries_per_worker };
                            let case = Case { workers: c, capacity: c, messages, workload };
                            for backend in BACKENDS { black_box(measure(backend, case).await); }
                            for repetition in 0..REPETITIONS {
                                for (position, backend) in backend_order(repetition).into_iter().enumerate() {
                                    let measurement = measure(backend, case).await;
                                    let io_ms = match workload { Workload::Io(delay) => delay.as_millis(), _ => 0 };
                                    println!("BENCH,{threads},{c},{c},{},{io_ms},{},{repetition},{position},{messages},{:.3},{:.0},{:.3},{:.3},{:.3},{:.3}",
                                        workload.name(), backend.name(), measurement.elapsed.as_secs_f64() * 1000.0,
                                        messages as f64 / measurement.elapsed.as_secs_f64(),
                                        percentile(&measurement.queue_ns, 50), percentile(&measurement.queue_ns, 95),
                                        percentile(&measurement.queue_ns, 99), percentile(&measurement.delivery_ns, 95));
                                }
                            }
                        }
                    }
                });
            }
        }

        /// Run explicitly with release optimization and --ignored --nocapture --test-threads=1.
        /// No performance threshold: timings depend on host scheduling and CPU quotas.
        #[test]
        #[ignore = "manual release-mode queue comparison; see doc/queue_performance.md"]
        fn compare_current_and_mpmc() {
            run_comparison(
                &[1, 4],
                &[Workload::Fast, Workload::Yield, Workload::Io(Duration::from_millis(100))],
                4,
            );
        }

        /// Longer batches give useful latency sample counts for short simulated HTTP waits.
        #[test]
        #[ignore = "manual release-mode 5ms I/O comparison on four runtime threads"]
        fn compare_http_5ms_four_threads() {
            run_comparison(&[4], &[Workload::Io(Duration::from_millis(5))], 100);
        }
    }
}
