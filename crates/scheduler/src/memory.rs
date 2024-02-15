use crate::{
    worker::{Worker, WorkerCommand, WorkerError, WriteableWorkerStore},
    ExecutionStatusInfo, FinishedExecutionError, FinishedExecutionResult,
};
use async_channel::{Receiver, Sender};
use chrono::{DateTime, TimeDelta, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
use indexmap::IndexMap;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use tracing_unwrap::{OptionExt, ResultExt};

/// Executor disconnect detection:
/// This implementation relies on oneshot channels, which simulate a TCP connection in a way.
/// When the oneshot channel is closed, we know we need to re-enqueue the execution.
/// An alternate implementation might exist where we would use
/// versioning, health checks and timeouts to detect when an executor got stuck or disconnected.

// Done:
// Execution insertion, enqueueing, executing on a worker
// Timeouts (permanent)
// Handling executor abortion (re-enqueueing)
// Enqueueing in the background when a queue is full during insertion
// Handling of panics in workers
// Partial execution results, i.e. worker requesting to be reenqueued
// Worker store
// Scheduling - delaying during insertion, inflight requested by the worker
// Retries on timeouts
// Dependent executions: WorkerCommand::ExecuteBlocking

// TODO:
// * refactor: remove old db, runtime, event history
// * refactor: Make store wrap Arc<Mutex>, investigate row based locking instead of store cloning
// * refactor: Remove store from public insert, schedule
// * feat: Dependent executions: async executions
// * fix: Consistency between `inflight_executions` and `finished_executions`
// * feat: Run id, regenerate between retries
// * feat: Execution journal - 1.pending, 2. enqueued, timed out, retried, etc.
// * resil: Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
// * resil: Fairness: reenqueue should push the execution to the end of `inflight_executions`
// * perf: Wake up the listener by insertion, executor emitting an event (+ waking up parent) by pushing a workflow id to an mpsc channel.
// * tracing: Add task names
// * feat: Retries on UncategorizedError - respawn executor? Create new worker using a factory?
// * feat: Retries on SupportedFunctionResult::Single(Result::Err) - configurable by worker
// * feat: Configure db

const LISTENER_TICK_MICROS: u64 = 10;
const TIMEOUT_DELAY: TimeDelta = TimeDelta::milliseconds(100);
const TIMEOUT_MAX_RETRY_COUNT: u16 = 5;
struct QueueEntry<S, E: ExecutionId> {
    execution_id: E,
    params: Params,
    store: S,
    executor_to_db_sender: oneshot::Sender<Result<WorkerCommand<E>, WorkerError>>,
}

struct InflightExecution<S: WriteableWorkerStore<E>, E: ExecutionId> {
    ffqn: FunctionFqn,
    params: Params,
    store: S,
    db_to_client_sender: Option<oneshot::Sender<FinishedExecutionResult>>, // Hack: Always present so it can be taken and used by the listener.
    status: InflightExecutionStatus<E>,
}

enum InflightExecutionTransition<E: ExecutionId> {
    MoveToFinished(FinishedExecutionResult),
    ExecuteBlocking {
        child_execution_id: E,
        ffqn: FunctionFqn,
        params: Params,
    },
    KeepInflight,
}

impl<S: WriteableWorkerStore<E>, E: ExecutionId> InflightExecution<S, E> {
    // Set the status to Pending or Enqueued, unless the execution needs to wait until `delay`.
    fn delayed_attempt_to_enqueue(
        &mut self,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
        execution_id: E,
    ) {
        if matches!(self.status,
            InflightExecutionStatus::IntermittentTimeout { delay, .. } |
            InflightExecutionStatus::DelayedUntil { delay, .. } if delay > Utc::now())
        {
            return;
        }
        match self.status {
            InflightExecutionStatus::DelayedUntil {
                delay_command_identifier: Some(delay_command_identifier),
                ..
            } => self.store.persist_delay_passed(delay_command_identifier),
            _ => {}
        }
        // Attempt to submit the execution to the mpmc channel.
        let db_to_executor_mpmc_sender = db_to_executor_mpmc_queues
            .lock()
            .unwrap_or_log()
            .get(&self.ffqn)
            .expect_or_log("must be created in `insert`")
            .0
            .clone();
        self.status = {
            let (executor_to_db_sender, executor_to_db_receiver) = oneshot::channel();
            let entry = QueueEntry {
                execution_id,
                params: self.params.clone(),
                store: self.store.clone(),
                executor_to_db_sender,
            };
            let send_res = db_to_executor_mpmc_sender.try_send(entry);
            let retry_index = self.status.retry_index();
            let new_status = match send_res {
                Ok(()) => InflightExecutionStatus::Enqueued {
                    executor_to_db_receiver,
                    retry_index,
                },
                Err(async_channel::TrySendError::Full(_)) => {
                    InflightExecutionStatus::Pending { retry_index }
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    unreachable!("database holds a receiver")
                }
            };
            new_status
        };
    }

    fn transition(
        &mut self,
        execution_id: &E,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >, // read write
        finished_executions: &std::sync::Mutex<HashMap<E, FinishedExecutionResult>>, // read only
    ) -> InflightExecutionTransition<E> {
        match self {
            InflightExecution {
                status:
                    InflightExecutionStatus::Enqueued {
                        executor_to_db_receiver,
                        ..
                    },
                ..
            } => {
                match executor_to_db_receiver.try_recv() {
                    Ok(res) => {
                        match res {
                            Ok(WorkerCommand::PublishResult(supported_res)) => {
                                let finished_status = Ok(supported_res);
                                info!("Execution finished successfuly");
                                // Attempt to notify the client.
                                let db_client_sender = self
                                    .db_to_client_sender
                                    .take()
                                    .expect_or_log("db_client_sender must have been set in insert");
                                let _ = db_client_sender.send(finished_status.clone());
                                InflightExecutionTransition::MoveToFinished(finished_status)
                            }
                            Err(WorkerError::Uncategorized) => {
                                let finished_status =
                                    Err(FinishedExecutionError::UncategorizedError);
                                warn!("Execution finished with {finished_status:?}");
                                // Attempt to notify the client.
                                let db_client_sender = self
                                    .db_to_client_sender
                                    .take()
                                    .expect_or_log("db_client_sender must have been set in insert");
                                let _ = db_client_sender.send(finished_status.clone());
                                InflightExecutionTransition::MoveToFinished(finished_status)
                            }
                            Err(WorkerError::NonDeterminismDetected(reason)) => {
                                let finished_status =
                                    Err(FinishedExecutionError::NonDeterminismDetected(reason));
                                warn!("Execution finished with {finished_status:?}");
                                // Attempt to notify the client.
                                let db_client_sender = self
                                    .db_to_client_sender
                                    .take()
                                    .expect_or_log("db_client_sender must have been set in insert");
                                let _ = db_client_sender.send(finished_status.clone());
                                InflightExecutionTransition::MoveToFinished(finished_status)
                            }
                            Err(WorkerError::Timeout) => {
                                let retry_index = self.status.retry_index() + 1;
                                if retry_index > TIMEOUT_MAX_RETRY_COUNT {
                                    // PermanentTimeout
                                    let finished_status =
                                        Err(FinishedExecutionError::PermanentTimeout);
                                    info!("Execution finished with PermanentTimeout");
                                    // Attempt to notify the client.
                                    let db_client_sender =
                                        self.db_to_client_sender.take().expect_or_log(
                                            "db_client_sender must have been set in insert",
                                        );
                                    let _ = db_client_sender.send(finished_status.clone());
                                    InflightExecutionTransition::MoveToFinished(finished_status)
                                } else {
                                    // IntermittentTimeout
                                    let duration =
                                        TIMEOUT_DELAY * 2_i32.pow(u32::from(retry_index) - 1);
                                    let delay = Utc::now() + duration;
                                    debug!("Retry {retry_index} after timeout is scheduled after {duration:?} at `{delay}`");
                                    self.status = InflightExecutionStatus::IntermittentTimeout {
                                        delay,
                                        retry_index,
                                    };
                                    InflightExecutionTransition::KeepInflight
                                }
                            }
                            Ok(WorkerCommand::DelayFor(duration)) => {
                                let delay = Utc::now() + duration;
                                debug!(
                                    "Worker issued DelayFor({duration:?}) expiring at `{delay}`"
                                );
                                self.status = InflightExecutionStatus::DelayedUntil {
                                    delay,
                                    retry_index: self.status.retry_index(),
                                    delay_command_identifier: Some(duration),
                                };
                                InflightExecutionTransition::KeepInflight
                            }
                            Ok(WorkerCommand::ExecuteBlocking {
                                ffqn,
                                params,
                                child_execution_id,
                            }) => {
                                debug!(%child_execution_id, "Worker issued ExecuteBlocking({ffqn})");
                                self.status = InflightExecutionStatus::Blocked {
                                    child_execution_id: child_execution_id.clone(),
                                    retry_index: self.status.retry_index(),
                                };
                                InflightExecutionTransition::ExecuteBlocking {
                                    child_execution_id,
                                    ffqn,
                                    params,
                                }
                            }
                        }
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {
                        // No response yet, keep the entry.
                        InflightExecutionTransition::KeepInflight
                    }
                    Err(oneshot::error::TryRecvError::Closed) => {
                        // The executor was aborted while running the execution.
                        self.delayed_attempt_to_enqueue(
                            db_to_executor_mpmc_queues,
                            execution_id.clone(),
                        );
                        InflightExecutionTransition::KeepInflight
                    }
                }
            }
            InflightExecution {
                status: InflightExecutionStatus::Pending { .. },
                ..
            } => {
                self.delayed_attempt_to_enqueue(db_to_executor_mpmc_queues, execution_id.clone());
                InflightExecutionTransition::KeepInflight
            }
            // Wait until the delay has passed, then attempt to enqueue
            InflightExecution {
                status: InflightExecutionStatus::DelayedUntil { .. },
                ..
            }
            | InflightExecution {
                status: InflightExecutionStatus::IntermittentTimeout { .. },
                ..
            } => {
                self.delayed_attempt_to_enqueue(db_to_executor_mpmc_queues, execution_id.clone());
                InflightExecutionTransition::KeepInflight
            }
            InflightExecution {
                status:
                    InflightExecutionStatus::Blocked {
                        child_execution_id, ..
                    },
                ..
            } => {
                // child execution is finished
                if let Some(res) = finished_executions
                    .lock()
                    .unwrap_or_log()
                    .get(&child_execution_id)
                {
                    let child_execution_id = child_execution_id.clone();
                    self.store
                        .persist_child_result(child_execution_id, res.clone());
                    self.delayed_attempt_to_enqueue(
                        db_to_executor_mpmc_queues,
                        execution_id.clone(),
                    );
                    InflightExecutionTransition::KeepInflight
                } else {
                    InflightExecutionTransition::KeepInflight
                }
            }
        }
    }
}

#[derive(Debug, derive_more::Display)]
enum InflightExecutionStatus<E: ExecutionId> {
    #[display(fmt = "Pending({retry_index})")]
    Pending { retry_index: u16 },
    #[display(fmt = "Enqueued({retry_index})")]
    Enqueued {
        executor_to_db_receiver: oneshot::Receiver<Result<WorkerCommand<E>, WorkerError>>,
        retry_index: u16,
    },
    #[display(fmt = "DelayedUntil(`{delay}`,{retry_index})")]
    DelayedUntil {
        delay: DateTime<Utc>,
        delay_command_identifier: Option<Duration>,
        retry_index: u16,
    },
    #[display(fmt = "IntermittentTimeout(`{delay}`,{retry_index})")]
    IntermittentTimeout {
        delay: DateTime<Utc>,
        retry_index: u16,
    },
    #[display(fmt = "IntermittentTimeout(`{child_execution_id}`,{retry_index})")]
    Blocked {
        child_execution_id: E,
        retry_index: u16,
    },
}

impl<E: ExecutionId> InflightExecutionStatus<E> {
    fn retry_index(&self) -> u16 {
        match self {
            Self::Pending { retry_index } => *retry_index,
            Self::Enqueued { retry_index, .. } => *retry_index,
            Self::DelayedUntil { retry_index, .. } => *retry_index,
            Self::IntermittentTimeout { retry_index, .. } => *retry_index,
            Self::Blocked { retry_index, .. } => *retry_index,
        }
    }
}

pub struct InMemoryDatabase<S: WriteableWorkerStore<E>, E: ExecutionId> {
    queue_capacity: usize,
    // Single writer: `insert`. Read by db listener(reenqueue) and `spawn_executor`(receiver).
    db_to_executor_mpmc_queues: Arc<
        std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
    >,
    // Written by both `insert` and the listener (update status).
    inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S, E>>>>,
    // Written by the listener.
    finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>>,
    listener: AbortHandle,
}

impl<S: WriteableWorkerStore<E>, E: ExecutionId> InMemoryDatabase<S, E> {
    pub fn spawn_new(queue_capacity: usize) -> Self {
        let inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S, E>>>> =
            Default::default();
        let db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<
                HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
            >,
        > = Default::default();
        let finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>> =
            Default::default();
        let listener = Self::spawn_listener(
            inflight_executions.clone(),
            db_to_executor_mpmc_queues.clone(),
            finished_executions.clone(),
            queue_capacity,
        );
        Self {
            queue_capacity,
            db_to_executor_mpmc_queues,
            inflight_executions,
            finished_executions,
            listener,
        }
    }

    pub fn get_execution_status(&self, execution_id: &E) -> Option<ExecutionStatusInfo> {
        if let Some(res) = self
            .finished_executions
            .lock()
            .unwrap_or_log()
            .get(execution_id)
        {
            return Some(ExecutionStatusInfo::Finished(res.clone()));
        };
        match self
            .inflight_executions
            .lock()
            .unwrap_or_log()
            .get(execution_id)
            .map(|found| &found.status)
        {
            Some(InflightExecutionStatus::Pending { .. }) => Some(ExecutionStatusInfo::Pending),
            Some(InflightExecutionStatus::Enqueued { .. }) => Some(ExecutionStatusInfo::Enqueued),
            Some(InflightExecutionStatus::DelayedUntil { delay, .. }) => {
                Some(ExecutionStatusInfo::DelayedUntil(delay.clone()))
            }
            Some(InflightExecutionStatus::IntermittentTimeout { delay, .. }) => {
                Some(ExecutionStatusInfo::IntermittentTimeout(delay.clone()))
            }
            Some(InflightExecutionStatus::Blocked { .. }) => Some(ExecutionStatusInfo::Blocked),
            None => None,
        }
    }

    pub fn insert(
        &self,
        ffqn: FunctionFqn,
        execution_id: E,
        params: Params,
        store: S,
    ) -> oneshot::Receiver<FinishedExecutionResult> {
        Self::insert_or_schedule(
            &mut self.inflight_executions.lock().unwrap_or_log(),
            &mut self.db_to_executor_mpmc_queues.lock().unwrap_or_log(),
            self.queue_capacity,
            ffqn,
            execution_id,
            params,
            store,
            None,
        )
    }

    pub fn schedule(
        &self,
        ffqn: FunctionFqn,
        execution_id: E,
        params: Params,
        store: S,
        delay: DateTime<Utc>,
    ) -> oneshot::Receiver<FinishedExecutionResult> {
        Self::insert_or_schedule(
            &mut self.inflight_executions.lock().unwrap_or_log(),
            &mut self.db_to_executor_mpmc_queues.lock().unwrap_or_log(),
            self.queue_capacity,
            ffqn,
            execution_id,
            params,
            store,
            Some(delay),
        )
    }

    #[instrument(skip_all, fields(%ffqn, %execution_id))]
    fn insert_or_schedule(
        inflight_executions: &mut std::sync::MutexGuard<IndexMap<E, InflightExecution<S, E>>>,
        db_to_executor_mpmc_queues: &mut std::sync::MutexGuard<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
        queue_capacity: usize,
        ffqn: FunctionFqn,
        execution_id: E,
        params: Params,
        store: S,
        delay: Option<DateTime<Utc>>,
    ) -> oneshot::Receiver<FinishedExecutionResult> {
        trace!(?delay, ?params, "insert_or_schedule");
        // make sure the mpmc channel is created, obtain its sender
        db_to_executor_mpmc_queues
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(queue_capacity));

        let (db_to_client_sender, db_to_client_receiver) = oneshot::channel();
        // Attempt to enqueue the execution.
        let status = if let Some(delay) = delay {
            InflightExecutionStatus::DelayedUntil {
                delay,
                retry_index: 0,
                delay_command_identifier: None,
            }
        } else {
            InflightExecutionStatus::Pending { retry_index: 0 }
        };
        info!(%status, "Persisting execution");
        let submitted = InflightExecution {
            ffqn,
            params,
            status,
            db_to_client_sender: Some(db_to_client_sender),
            store,
        };
        // Save the execution
        inflight_executions.insert(execution_id, submitted);
        db_to_client_receiver
    }

    fn spawn_executor<W: Worker<S, E> + Send + Sync + 'static>(
        &self,
        ffqn: FunctionFqn,
        worker: W,
        max_tasks: u32,
        max_task_duration: Option<Duration>,
    ) -> Executor<S, E> {
        let receiver = self
            .db_to_executor_mpmc_queues
            .lock()
            .unwrap_or_log()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .1
            .clone();
        Executor::spawn_new(ffqn, receiver, worker, max_tasks, max_task_duration)
    }

    fn spawn_listener(
        inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S, E>>>>,
        db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<
                HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
            >,
        >,
        finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>>,
        queue_capacity: usize,
    ) -> AbortHandle {
        tokio::spawn(
            async move {
                loop {
                    Self::listener_tick(
                        &inflight_executions,
                        &db_to_executor_mpmc_queues,
                        &finished_executions,
                        queue_capacity,
                    );
                    tokio::time::sleep(Duration::from_micros(LISTENER_TICK_MICROS)).await;
                }
            }
            .instrument(info_span!("db_listener")),
        )
        .abort_handle()
    }

    #[instrument(skip_all)]
    /// Responsible for
    /// * purging finished executions from `inflight_executions`
    /// * (re)enqueueing executions to the mpmc queue.
    fn listener_tick(
        inflight_executions: &std::sync::Mutex<IndexMap<E, InflightExecution<S, E>>>,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
        finished_executions: &std::sync::Mutex<HashMap<E, FinishedExecutionResult>>,
        queue_capacity: usize,
    ) {
        let mut finished = Vec::new();
        let mut inserted = Vec::new();
        let mut inflight_executions = inflight_executions.lock().unwrap_or_log();

        inflight_executions.retain(|execution_id, inflight_execution| {
            info_span!("listener_tick", %execution_id).in_scope(|| {
                match inflight_execution.transition(
                    execution_id,
                    &db_to_executor_mpmc_queues,
                    finished_executions,
                ) {
                    InflightExecutionTransition::MoveToFinished(finished_execution) => {
                        finished.push((execution_id.clone(), finished_execution));
                        false
                    }
                    InflightExecutionTransition::KeepInflight => true,
                    InflightExecutionTransition::ExecuteBlocking {
                        child_execution_id,
                        ffqn,
                        params,
                    } => {
                        debug!(%child_execution_id, "Requested blocking execution");
                        inserted.push((ffqn, params, child_execution_id));
                        true
                    }
                }
            })
        });
        let mut db_to_executor_mpmc_queues = db_to_executor_mpmc_queues.lock().unwrap_or_log();
        for (ffqn, params, child_execution_id) in inserted {
            InMemoryDatabase::insert_or_schedule(
                &mut inflight_executions,
                &mut db_to_executor_mpmc_queues,
                queue_capacity,
                ffqn,
                child_execution_id,
                params,
                S::default(),
                None,
            );
        }

        finished_executions
            .lock()
            .unwrap_or_log()
            .extend(finished.into_iter());
    }

    pub async fn close(self) {
        todo!()
    }
}

impl<S: WriteableWorkerStore<E>, E: ExecutionId> Drop for InMemoryDatabase<S, E> {
    fn drop(&mut self) {
        if !self.listener.is_finished() {
            warn!("Aborting the database listener task");
            self.listener.abort();
        }
    }
}

pub struct Executor<S, E: ExecutionId> {
    ffqn: FunctionFqn,
    executor_task: AbortHandle,
    receiver: Receiver<QueueEntry<S, E>>,
}

impl<S: WriteableWorkerStore<E>, E: ExecutionId> Executor<S, E> {
    #[instrument(skip_all, fields(%ffqn))]
    fn spawn_new<W: Worker<S, E> + Send + Sync + 'static>(
        ffqn: FunctionFqn,
        receiver: Receiver<QueueEntry<S, E>>,
        worker: W,
        max_tasks: u32,
        max_task_duration: Option<Duration>,
    ) -> Executor<S, E> {
        assert!(max_tasks > 0, "`max_tasks` must be greater than zero");
        let worker = Arc::new(worker);
        let executor_task = {
            let receiver = receiver.clone();
            tokio::spawn(
            async move {
                info!("Spawned executor");
                let mut worker_set = JoinSet::new(); // All worker tasks are aborted when this task exits.
                // Add a dummy task so that worker_set.join never returns None
                worker_set.spawn(async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(u64::MAX)).await
                    }
                });

                let semaphore = Arc::new(tokio::sync::Semaphore::new(
                    usize::try_from(max_tasks).expect_or_log("usize from u32 must not fail"),
                ));
                let mut worker_ids_to_worker_task_vals = IndexMap::new();
                loop {
                    trace!(
                        "Available permits: {permits}",
                        permits = semaphore.available_permits()
                    );

                    let permit = loop {
                        let joined = tokio::select! {
                            joined = worker_set.join_next_with_id() => {
                                joined.expect_or_log("dummy task never finishes")
                            },
                            permit = semaphore.clone().acquire_owned() => {
                                // The permit to be moved to the new task or to grace shutdown.
                                break permit.unwrap_or_log()
                            },
                        };
                        Self::handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                    };
                    trace!("Got permit to receive");
                    let recv = loop {
                        let joined = tokio::select!(
                            joined = worker_set.join_next_with_id() => {
                                joined.expect_or_log("dummy task never finishes")
                            },
                            recv = receiver.recv() => break recv,
                        );
                        Self::handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                    };
                    let Ok(QueueEntry {
                        execution_id,
                        params,
                        mut store,
                        executor_to_db_sender,
                    }) = recv
                    else {
                        info!("Graceful shutdown detected, waiting for inflight workers");
                        // Drain the worker set, except for the dummy task.
                        while worker_set.len() > 1 {
                            let joined = worker_set.join_next_with_id().await.unwrap_or_log();
                            Self::handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                        }
                        trace!("All workers have finished");
                        return;
                    };
                    let worker = worker.clone();
                    let worker_span = info_span!("worker", %execution_id);
                    worker_span.in_scope(|| trace!("Spawning worker"));
                    let worker_id = {
                        let execution_id = execution_id.clone();
                        worker_set.spawn(
                            async move {
                                debug!("Spawned worker");
                                store.restart();
                                let execution_result_fut = worker.run(execution_id, params, store);
                                let execution_result: Result<WorkerCommand<E>, WorkerError> =
                                    if let Some(max_task_duration) = max_task_duration {
                                        tokio::select! {
                                            res = execution_result_fut => res,
                                            _ = tokio::time::sleep(max_task_duration) => Err(WorkerError::Timeout)
                                        }
                                    } else {
                                        execution_result_fut.await
                                    };
                                if tracing::enabled!(tracing::Level::TRACE) {
                                    trace!("Finished: {execution_result:?}");
                                } else {
                                    debug!("Finished");
                                }
                                drop(permit);
                                execution_result
                            }
                            .instrument(worker_span)
                        ).id()
                    };
                    worker_ids_to_worker_task_vals.insert(worker_id, (executor_to_db_sender, execution_id));
                }
            }
            .instrument(info_span!("executor")),
        )
        .abort_handle()
        };
        Executor {
            ffqn,
            executor_task,
            receiver,
        }
    }

    fn handle_joined(
        worker_ids_to_oneshot_senders: &mut IndexMap<
            tokio::task::Id,
            (oneshot::Sender<Result<WorkerCommand<E>, WorkerError>>, E),
        >,
        joined: Result<
            (tokio::task::Id, Result<WorkerCommand<E>, WorkerError>),
            tokio::task::JoinError,
        >,
    ) {
        match joined {
            Ok((worker_id, execution_result)) => {
                let (executor_to_db_sender, execution_id): (
                    oneshot::Sender<Result<WorkerCommand<E>, WorkerError>>,
                    E,
                ) = worker_ids_to_oneshot_senders
                    .swap_remove(&worker_id)
                    .unwrap_or_log();
                info_span!("joined", %execution_id).in_scope(|| {
                    let send_res = executor_to_db_sender.send(execution_result);
                    if send_res.is_err() {
                        debug!("Cannot send the result back to db");
                    }
                });
            }
            Err(join_error) => {
                let (executor_to_db_sender, execution_id): (
                    oneshot::Sender<Result<WorkerCommand<E>, WorkerError>>,
                    E,
                ) = worker_ids_to_oneshot_senders
                    .swap_remove(&join_error.id())
                    .unwrap_or_log();
                info_span!("joined_error", %execution_id).in_scope(|| {
                    error!(
                        "Got uncategorized worker join error. Panic: {panic}",
                        panic = join_error.is_panic()
                    );
                    let send_res = executor_to_db_sender.send(Err(WorkerError::Uncategorized));
                    if send_res.is_err() {
                        debug!("Cannot send the worker failure back to db");
                    }
                });
            }
        }
    }

    /// Graceful shutdown. Waits until all workers terminate.
    ///
    /// # Panics
    ///
    /// All senders must be closed, otherwise this function will panic.
    #[instrument(skip_all, fields(ffqn = %self.ffqn))]
    pub async fn close(self) {
        // Signal to the executor task.
        self.receiver.close();
        debug!("Gracefully closing");
        while !self.executor_task.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl<S, E: ExecutionId> Drop for Executor<S, E> {
    #[instrument(skip_all, fields(ffqn = %self.ffqn))]
    fn drop(&mut self) {
        if !self.executor_task.is_finished() {
            warn!("Aborting the executor task");
            self.executor_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        worker::{MaybeReplayResponse, NonDeterminismError, ReplayResponse, Worker, WorkerStore},
        ExecutionStatusInfo, FinishedExecutionError,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, SupportedFunctionResult};
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Instant,
    };
    use tracing_unwrap::OptionExt;

    static INIT: std::sync::Once = std::sync::Once::new();
    fn set_up() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_ansi(false))
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[derive(Default, Clone)]
    struct DummyStore {}

    impl<E: ExecutionId> WorkerStore<E> for DummyStore {
        fn next_id(&mut self) -> Result<E, NonDeterminismError> {
            unimplemented!()
        }

        fn next_event(
            &mut self,
            _command: &WorkerCommand<E>,
        ) -> Result<MaybeReplayResponse<E>, NonDeterminismError> {
            unimplemented!()
        }
    }

    impl<E: ExecutionId> WriteableWorkerStore<E> for DummyStore {
        fn restart(&mut self) {}

        fn persist_child_result(
            &mut self,
            _child_execution_id: E,
            _result: FinishedExecutionResult,
        ) {
            unimplemented!()
        }

        fn persist_delay_passed(&mut self, _delay: Duration) {}
    }

    #[tokio::test]
    async fn test_simple_workflow() {
        set_up();

        struct SimpleWorker;

        #[async_trait]
        impl Worker<DummyStore, WorkflowId> for SimpleWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: DummyStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
        );
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        let resp = execution.await.unwrap_or_log();
        assert_eq!(Ok(SupportedFunctionResult::None), resp);
        assert_eq!(
            Some(ExecutionStatusInfo::Finished(Ok(
                SupportedFunctionResult::None
            ))),
            db.get_execution_status(&execution_id)
        );
    }

    #[tokio::test]
    async fn test_semaphore_check_that_no_more_than_max_tasks_are_inflight() {
        set_up();

        struct SemaphoreWorker(tokio::sync::Semaphore);

        #[async_trait]
        impl Worker<DummyStore, WorkflowId> for SemaphoreWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: DummyStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                trace!("acquiring");
                let _permit = self.0.try_acquire().unwrap_or_log();
                trace!("sleeping");
                tokio::time::sleep(Duration::from_millis(100)).await;
                trace!("done!");
                Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
            }
        }

        let db = InMemoryDatabase::spawn_new(10);
        let max_tasks = 3;
        let executions = (0..max_tasks * 2)
            .map(|_| {
                db.insert(
                    SOME_FFQN.to_owned(),
                    WorkflowId::generate(),
                    Params::default(),
                    Default::default(),
                )
            })
            .collect::<Vec<_>>();
        let workflow_worker = SemaphoreWorker(tokio::sync::Semaphore::new(
            usize::try_from(max_tasks).unwrap_or_log(),
        ));
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), workflow_worker, max_tasks, None);
        for execution in executions {
            assert_eq!(
                Ok(SupportedFunctionResult::None),
                execution.await.unwrap_or_log()
            );
        }
    }

    struct SleepyWorker(Option<Arc<AtomicBool>>);

    #[async_trait]
    impl Worker<DummyStore, WorkflowId> for SleepyWorker {
        #[instrument(skip_all)]
        async fn run(
            &self,
            _execution_id: WorkflowId,
            params: Params,
            _store: DummyStore,
        ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
            assert_eq!(params.len(), 1);
            let millis = params[0].clone();
            let millis = assert_matches!(millis, wasmtime::component::Val::U64(millis) => millis);
            trace!("sleeping for {millis} ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            trace!("done!");
            if let Some(finished_check) = &self.0 {
                assert_eq!(false, finished_check.swap(true, Ordering::SeqCst));
            }
            Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
        }
    }

    #[tokio::test]
    async fn long_execution_should_timeout() {
        const LEEWAY: Duration = Duration::from_secs(1);
        const MAX_EXECUTION_DURATION: Duration = Duration::from_millis(100);
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let finished_check = Arc::new(AtomicBool::new(false));
        let _executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            Some(MAX_EXECUTION_DURATION),
        );
        let execution_id = WorkflowId::generate();
        let stopwatch = Instant::now();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::from([wasmtime::component::Val::U64(
                MAX_EXECUTION_DURATION.as_millis() as u64 * 2,
            )]),
            Default::default(),
        );
        wait_for_status(&db, &execution_id, |status| {
            matches!(status, ExecutionStatusInfo::IntermittentTimeout(..))
        })
        .await;
        let execution = execution.await.unwrap_or_log();
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert_eq!(false, finished_check.load(Ordering::SeqCst));
        assert_eq!(Err(FinishedExecutionError::PermanentTimeout), execution);
        assert_eq!(
            Some(ExecutionStatusInfo::Finished(Err(
                FinishedExecutionError::PermanentTimeout
            ))),
            db.get_execution_status(&execution_id)
        );
        let expected = TIMEOUT_DELAY * (2_i32.pow(u32::from(TIMEOUT_MAX_RETRY_COUNT)) - 1); // expected backoff
        let expected = expected
            + TimeDelta::from_std(MAX_EXECUTION_DURATION).unwrap_or_log()
                * i32::from(TIMEOUT_MAX_RETRY_COUNT);
        assert_between(stopwatch, expected.to_std().unwrap_or_log(), LEEWAY);
    }

    #[tokio::test]
    async fn two_executors_should_work_in_parallel() {
        set_up();
        let db = InMemoryDatabase::spawn_new(2);
        let sleep_millis = 100;
        let _executor_1 = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let _executor_2 = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let stopwatch = Instant::now();
        let fut_1 = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
        );
        let fut_2 = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            fut_1.await.unwrap_or_log()
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            fut_2.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert!(stopwatch < Duration::from_millis(sleep_millis) * 2,);
    }

    #[tokio::test]
    async fn inflight_execution_of_aborted_executor_should_restart_on_a_new_executor() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let finished_check = Arc::new(AtomicBool::new(false));

        let executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );

        let sleep_millis = 100;
        let execution_id = WorkflowId::generate();
        let mut execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id,
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
        );
        // Drop the executor after 10ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(executor_abort_handle);
        });
        // Make sure that the worker task was aborted and did not mark the execution as finished.
        tokio::time::sleep(Duration::from_millis(sleep_millis * 2)).await;
        assert_eq!(false, finished_check.load(Ordering::SeqCst));
        assert_eq!(
            oneshot::error::TryRecvError::Empty,
            execution.try_recv().unwrap_err_or_log()
        );
        // Abandoned execution should be picked by another worker spawned the new executor.
        let _executor = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        assert_eq!(true, finished_check.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let sleep_millis = 100;
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
        );
        let finished_check = Arc::new(AtomicBool::new(false));
        let executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );
        tokio::time::sleep(Duration::from_millis(sleep_millis / 2)).await;
        // Close should block until the execution is done.
        executor_abort_handle.close().await;
        assert_eq!(true, finished_check.load(Ordering::SeqCst));
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn execution_added_to_full_mpmc_queue_should_finish() {
        const QUEUE_SIZE: usize = 1;
        set_up();
        let db = InMemoryDatabase::spawn_new(QUEUE_SIZE);
        let max_duration_millis = 100;
        let _executor = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let execute = |execution_id: WorkflowId| {
            (
                execution_id.clone(),
                db.insert(
                    SOME_FFQN.to_owned(),
                    execution_id,
                    Params::from([wasmtime::component::Val::U64(max_duration_millis)]),
                    Default::default(),
                ),
            )
        };
        let executions = (0..QUEUE_SIZE + 1)
            .map(|idx| execute(WorkflowId::new(idx.to_string())))
            .collect::<Vec<_>>();
        for execution in executions {
            assert_eq!(
                Ok(SupportedFunctionResult::None),
                execution.1.await.unwrap_or_log()
            );
            assert_eq!(
                Some(ExecutionStatusInfo::Finished(Ok(
                    SupportedFunctionResult::None
                ))),
                db.get_execution_status(&execution.0)
            );
        }
    }

    #[tokio::test]
    async fn test_panic_in_worker() {
        set_up();
        struct PanicingWorker;
        #[async_trait]
        impl Worker<DummyStore, WorkflowId> for PanicingWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: DummyStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                panic!();
            }
        }
        let db = InMemoryDatabase::spawn_new(10);
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::default(),
            Default::default(),
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PanicingWorker, 1, None);
        let execution = execution.await.unwrap_or_log();
        assert_matches!(execution, Err(FinishedExecutionError::UncategorizedError));
    }

    #[tokio::test]
    async fn test_partial_progress() {
        set_up();
        struct PartialProgressWorker {
            is_waiting: Arc<AtomicBool>,
            should_finish: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Worker<DummyStore, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: DummyStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                if self.should_finish.load(Ordering::SeqCst) {
                    trace!("Worker finished");
                    Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
                } else {
                    trace!("Worker waiting");
                    self.is_waiting.store(true, Ordering::SeqCst);
                    Ok(WorkerCommand::DelayFor(Duration::from_millis(100)))
                }
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
        );
        let is_waiting = Arc::new(AtomicBool::new(false));
        let should_finish = Arc::new(AtomicBool::new(false));
        let _executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            PartialProgressWorker {
                is_waiting: is_waiting.clone(),
                should_finish: should_finish.clone(),
            },
            1,
            None,
        );
        loop {
            if is_waiting.load(Ordering::SeqCst) {
                break;
            }
            trace!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!db
            .get_execution_status(&execution_id)
            .unwrap_or_log()
            .is_finished());

        should_finish.store(true, Ordering::SeqCst);
        let status = wait_for_status(&db, &execution_id, |status| status.is_finished()).await;
        assert_eq!(
            ExecutionStatusInfo::Finished(Ok(SupportedFunctionResult::None)),
            status
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
    }

    #[derive(Default)]
    struct StoreWrapper<T>(Arc<tokio::sync::Mutex<T>>);
    impl<T> Clone for StoreWrapper<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<T> WorkerStore<WorkflowId> for StoreWrapper<T> {
        fn next_id(&mut self) -> Result<WorkflowId, NonDeterminismError> {
            unimplemented!()
        }

        fn next_event(
            &mut self,
            _command: &WorkerCommand<WorkflowId>,
        ) -> Result<MaybeReplayResponse<WorkflowId>, NonDeterminismError> {
            unimplemented!()
        }
    }

    impl<T: Default + Send + 'static> WriteableWorkerStore<WorkflowId> for StoreWrapper<T> {
        fn restart(&mut self) {}

        fn persist_child_result(
            &mut self,
            _child_execution_id: WorkflowId,
            _result: FinishedExecutionResult,
        ) {
            unimplemented!()
        }

        fn persist_delay_passed(&mut self, delay: Duration) {}
    }

    #[tokio::test]
    async fn test_partial_progress_with_shared_arc_mutex_store() {
        set_up();
        struct PartialProgressWorker;

        #[derive(Default)]
        struct PartialStore {
            is_waiting: bool,
            should_finish: bool,
        }
        let store = Arc::new(tokio::sync::Mutex::new(PartialStore::default()));

        #[async_trait]
        impl Worker<StoreWrapper<PartialStore>, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                store: StoreWrapper<PartialStore>,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                let mut store = store.0.lock().await;
                if store.should_finish {
                    trace!("Worker finished");
                    return Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None));
                } else {
                    trace!("Worker waiting");
                    store.is_waiting = true;
                    Ok(WorkerCommand::DelayFor(Duration::from_millis(100)))
                }
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            StoreWrapper(store.clone()),
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PartialProgressWorker, 1, None);
        loop {
            let store = store.lock().await;
            if store.is_waiting {
                break;
            }
            debug!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!db
            .get_execution_status(&execution_id)
            .unwrap_or_log()
            .is_finished());

        store.lock().await.should_finish = true;
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        assert_eq!(
            ExecutionStatusInfo::Finished(Ok(SupportedFunctionResult::None)),
            db.get_execution_status(&execution_id).unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn test_simple_scheduled_workflow() {
        const DELAY: Duration = Duration::from_millis(500);
        const LEEWAY_DELAY: Duration = Duration::from_millis(100);
        set_up();
        struct SimpleWorker;
        #[async_trait]
        impl Worker<DummyStore, WorkflowId> for SimpleWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: DummyStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let now = Utc::now();
        let delay = now + DELAY;
        let execution = db.schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
            delay, // Testing this feature
        );
        assert_matches!(db.get_execution_status(&execution_id).unwrap_or_log(), ExecutionStatusInfo::DelayedUntil(found_delay) if found_delay == delay);
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        let stopwatch = Instant::now();
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert_between(stopwatch, DELAY, LEEWAY_DELAY);
    }

    async fn wait_for_status<S: WriteableWorkerStore<E>, E: ExecutionId>(
        db: &InMemoryDatabase<S, E>,
        execution_id: &E,
        predicate: fn(&ExecutionStatusInfo) -> bool,
    ) -> ExecutionStatusInfo {
        loop {
            if let Some(status) = db.get_execution_status(execution_id) {
                if predicate(&status) {
                    return status;
                }
            }
            trace!("Waiting for status change");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn assert_between(actual: Duration, expected: Duration, leeway: Duration) {
        assert!(
            actual <= expected + leeway,
            "exceeded accepted delay. Got: {actual:?}, expected <= {:?}",
            expected + leeway
        );
        assert!(
            actual >= expected - leeway,
            "below accepted delay. Got: {actual:?}, expected <= {:?}",
            expected - leeway
        );
    }

    #[tokio::test]
    async fn worker_requesting_delay() {
        const DELAY: Duration = Duration::from_millis(500);
        const LEEWAY: Duration = Duration::from_millis(100);
        set_up();
        struct PartialProgressWorker;

        #[derive(Default)]
        struct StoreInner {
            was_delayed: bool,
        }

        #[async_trait]
        impl Worker<StoreWrapper<StoreInner>, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                store: StoreWrapper<StoreInner>,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                let mut store = store.0.lock().await;
                if !store.was_delayed {
                    store.was_delayed = true;
                    trace!("Worker is requesting a delay");
                    Ok(WorkerCommand::DelayFor(DELAY))
                } else {
                    trace!("Worker is done");
                    Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
                }
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PartialProgressWorker, 1, None);
        wait_for_status(&db, &execution_id, |status| {
            matches!(status, ExecutionStatusInfo::DelayedUntil(_))
        })
        .await;
        let stopwatch = Instant::now();
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert_between(stopwatch, DELAY, LEEWAY);
    }

    #[derive(Clone, Debug)]
    enum VecStoreEvent {
        IdGenerated(WorkflowId),
        Command(WorkerCommand<WorkflowId>),
        GotChildResult {
            child_execution_id: WorkflowId,
            result: FinishedExecutionResult,
        },
        DelayPassed(Duration),
    }

    #[derive(Default)]
    struct VecStoreInner {
        idx: usize,
        events: Vec<VecStoreEvent>,
        corrupted: Option<()>,
    }

    #[derive(Default, Clone)]
    struct VecStore(Arc<std::sync::Mutex<VecStoreInner>>);

    impl VecStore {
        fn assert_not_corrupted(store: &VecStoreInner) {
            store
                .corrupted
                .expect_none_or_log("store consitency is corrupted");
        }

        fn non_determinism_detected(
            corrupted: &mut Option<()>,
            reason: String,
        ) -> NonDeterminismError {
            *corrupted = Some(());
            NonDeterminismError(reason)
        }

        fn push(
            store: &mut std::sync::MutexGuard<VecStoreInner>,
            event: VecStoreEvent,
        ) -> Result<(), NonDeterminismError> {
            Self::assert_not_corrupted(&store);
            if store.idx == store.events.len() {
                store.idx += 1;
                store.events.push(event);
                Ok(())
            } else {
                Err(Self::non_determinism_detected(
                    &mut store.corrupted,
                    "not all events were replayed".to_string(),
                ))
            }
        }

        fn next_event(store: &mut std::sync::MutexGuard<VecStoreInner>) -> Option<VecStoreEvent> {
            Self::assert_not_corrupted(&store);
            match store.events.get(store.idx) {
                Some(result) => {
                    let result = Some(result.clone());
                    store.idx += 1;
                    result
                }
                None => None,
            }
        }

        /// Get the child execution result from the next event.
        /// If found matching GotChildResult, increment `idx` and return the result.
        /// If there is nothing in the history, return `None`.
        fn get_child_result(
            store: &mut std::sync::MutexGuard<VecStoreInner>,
            requested_id: &WorkflowId,
        ) -> Result<Option<FinishedExecutionResult>, NonDeterminismError> {
            match store.events.get(store.idx) {
                Some(VecStoreEvent::GotChildResult {
                    child_execution_id: found,
                    result,
                }) if requested_id == found => {
                    let result = result.clone();
                    store.idx += 1;
                    Ok(Some(result))
                }
                None => return Ok(None),
                Some(other) => {
                    let reason = format!(
                        "unexpected event, requested GotChildResult({requested_id}), history contains {other:?}"
                    );
                    return Err(Self::non_determinism_detected(&mut store.corrupted, reason));
                }
            }
        }

        /// Get the delay result from the next event.
        fn get_delay_passed(
            store: &mut std::sync::MutexGuard<VecStoreInner>,
            requested_duration: Duration,
        ) -> Result<bool, NonDeterminismError> {
            match store.events.get(store.idx) {
                Some(VecStoreEvent::DelayPassed(found)) if requested_duration == *found => {
                    store.idx += 1;
                    Ok(true)
                }
                None => return Ok(false),
                Some(other) => {
                    let reason = format!(
                        "unexpected event, expected DelayPassed(`{requested_duration:?}`), history contains {other:?}"
                    );
                    return Err(Self::non_determinism_detected(&mut store.corrupted, reason));
                }
            }
        }
    }

    impl WorkerStore<WorkflowId> for VecStore {
        #[instrument(skip(self))]
        fn next_id(&mut self) -> Result<WorkflowId, NonDeterminismError> {
            let mut store = self.0.lock().unwrap_or_log();
            match Self::next_event(&mut store) {
                Some(VecStoreEvent::IdGenerated(id)) => Ok(id),
                Some(other) => Err(Self::non_determinism_detected(
                    &mut store.corrupted,
                    format!("unexpected event during id generation, expected {other:?}"),
                )),
                None => {
                    let id = WorkflowId::generate();
                    Self::push(&mut store, VecStoreEvent::IdGenerated(id.clone()))
                        .expect_or_log("this must be a new event");
                    Ok(id)
                }
            }
        }

        #[instrument(skip(self))]
        fn next_event(
            &mut self,
            request: &WorkerCommand<WorkflowId>,
        ) -> Result<MaybeReplayResponse<WorkflowId>, NonDeterminismError> {
            let mut store = self.0.lock().unwrap_or_log();
            match Self::next_event(&mut store) {
                Some(VecStoreEvent::Command(found)) if *request == found => {
                    match request {
                        WorkerCommand::ExecuteBlocking {
                            child_execution_id, ..
                        } => {
                            // do we have the child execution result next?
                            if let Some(result) =
                                Self::get_child_result(&mut store, &child_execution_id)?
                            {
                                Ok(MaybeReplayResponse::ReplayResponse(
                                    ReplayResponse::CompletedWithResult {
                                        child_execution_id: child_execution_id.clone(),
                                        result,
                                    },
                                ))
                            } else {
                                // reissue the request, wait for response, same as if the request was not recorded
                                ///////////Self::push(&mut store, VecStoreEvent::Command(request.clone()))?;
                                Ok(MaybeReplayResponse::MissingResponse)
                            }
                        }
                        WorkerCommand::DelayFor(duration) => {
                            // do we have the delay done next?
                            if Self::get_delay_passed(&mut store, *duration)? {
                                Ok(MaybeReplayResponse::ReplayResponse(
                                    ReplayResponse::Completed,
                                ))
                            } else {
                                Ok(MaybeReplayResponse::MissingResponse)
                            }
                        }
                        WorkerCommand::PublishResult(_) => Ok(MaybeReplayResponse::ReplayResponse(
                            ReplayResponse::Completed,
                        )),
                    }
                }
                None => {
                    Self::push(&mut store, VecStoreEvent::Command(request.clone()))?;
                    Ok(MaybeReplayResponse::MissingResponse)
                }
                Some(other) => Err(Self::non_determinism_detected(
                    &mut store.corrupted,
                    format!("unexpected event during handling of {request:?}, expected: {other:?}"),
                )),
            }
        }
    }

    impl WriteableWorkerStore<WorkflowId> for VecStore {
        fn restart(&mut self) {
            let mut store = self.0.lock().unwrap_or_log();
            store.idx = 0;
        }

        #[instrument(skip(self))]
        fn persist_child_result(
            &mut self,
            child_execution_id: WorkflowId,
            result: FinishedExecutionResult,
        ) {
            let mut store = self.0.lock().unwrap_or_log();
            let last_event = store
                .events
                .last()
                .expect_or_log("persist_delay_passed must be called on a non-empty history");
            if !matches!(
                last_event,
                VecStoreEvent::Command(WorkerCommand::ExecuteBlocking { child_execution_id: found, .. } ) if child_execution_id == *found
            ) {
                error!(%child_execution_id, "Broken contract: `persist_child_result` does not match the last event: {last_event:?}");
                panic!("Broken contract: `persist_child_result`(`{child_execution_id}`) does not match the last event: {last_event:?}");
            }

            Self::push(
                &mut store,
                VecStoreEvent::GotChildResult {
                    child_execution_id,
                    result,
                },
            )
            .expect_or_log("Broken contract: unprocessed events")
        }

        #[instrument(skip(self))]
        fn persist_delay_passed(&mut self, duration: Duration) {
            let mut store = self.0.lock().unwrap_or_log();
            let last_event = store
                .events
                .last()
                .expect_or_log("persist_delay_passed must be called on a non-empty history");
            if !matches!(
                last_event,
                VecStoreEvent::Command(WorkerCommand::DelayFor(found) ) if duration == *found
            ) {
                error!("Broken contract: `persist_delay_passed`(`{duration:?}`) does not match the last event: {last_event:?}");
                panic!("Broken contract: `persist_delay_passed`(`{duration:?}`) does not match the last event: {last_event:?}");
            }
            Self::push(&mut store, VecStoreEvent::DelayPassed(duration))
                .expect_or_log("Broken contract: unprocessed events")
        }
    }

    #[tokio::test]
    async fn replay_worker_requesting_delay() {
        const DELAY: Duration = Duration::from_millis(500);
        const LEEWAY_DELAY: Duration = Duration::from_millis(100);
        set_up();
        struct PartialProgressWorker;

        #[async_trait]
        impl Worker<VecStore, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                mut store: VecStore,
            ) -> Result<WorkerCommand<WorkflowId>, WorkerError> {
                let command = WorkerCommand::DelayFor(DELAY);
                match store.next_event(&command)? {
                    MaybeReplayResponse::MissingResponse => {
                        trace!("Delay was missing in the history, issuing");
                        return Ok(command);
                    }
                    MaybeReplayResponse::ReplayResponse(ReplayResponse::Completed) => {
                        trace!("Delay is already completed");
                    }
                    other => {
                        error!("Broken contract: Unexpected store response {other:?} to request {command:?}");
                        panic!("Broken contract: Unexpected store response {other:?} to request {command:?}")
                    }
                }
                trace!("Worker is done");
                let command = WorkerCommand::PublishResult(SupportedFunctionResult::None);
                match store.next_event(&command)? {
                    MaybeReplayResponse::MissingResponse => {
                        trace!("Final response was missing in the history, issuing");
                        return Ok(command);
                    }
                    MaybeReplayResponse::ReplayResponse(ReplayResponse::Completed) => {
                        trace!("Final result was acked");
                        return Ok(command);
                    }
                    other => {
                        error!("Broken contract: Unexpected store response {other:?} to request {command:?}");
                        panic!("Broken contract: Unexpected store response {other:?} to request {command:?}")
                    }
                }
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PartialProgressWorker, 1, None);
        wait_for_status(&db, &execution_id, |status| {
            matches!(status, ExecutionStatusInfo::DelayedUntil(_))
        })
        .await;
        let stopwatch = Instant::now();
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert_between(stopwatch, DELAY, LEEWAY_DELAY);
    }
}
