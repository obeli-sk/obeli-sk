use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{
    AppendRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use executor::worker::{FatalError, WorkerError};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace};
use utils::time::ClockFn;
use wasmtime::component::Val;

const DB_LATENCY_MILLIS: u32 = 10; // do not interrupt if requested to sleep for less time.

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum FunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("child request")]
    ChildExecutionRequest,
    #[error("delay request")]
    DelayRequest,
    #[error(transparent)]
    DbError(#[from] DbError),
}

impl From<FunctionError> for WorkerError {
    fn from(value: FunctionError) -> Self {
        match value {
            FunctionError::NonDeterminismDetected(reason) => {
                WorkerError::FatalError(FatalError::NonDeterminismDetected(reason.clone()))
            }

            FunctionError::ChildExecutionRequest => WorkerError::ChildExecutionRequest,
            FunctionError::DelayRequest => WorkerError::DelayRequest,
            FunctionError::DbError(db_error) => WorkerError::DbError(db_error),
        }
    }
}

pub(crate) struct WorkflowCtx<C: ClockFn, DB: DbConnection> {
    execution_id: ExecutionId,
    events: Vec<HistoryEvent>,
    events_idx: usize,
    rng: StdRng,
    pub(crate) clock_fn: C,
    db_connection: DB,
    version: Version,
    execution_deadline: DateTime<Utc>,
    child_retry_exp_backoff: Duration,
    child_max_retries: u32,
}

impl<C: ClockFn, DB: DbConnection> WorkflowCtx<C, DB> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        events: Vec<HistoryEvent>,
        seed: u64,
        clock_fn: C,
        db_connection: DB,
        version: Version,
        execution_deadline: DateTime<Utc>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    ) -> Self {
        Self {
            execution_id,
            events,
            events_idx: 0,
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            // join_next_blocking_strategy,
            db_connection,
            version,
            execution_deadline,
            child_retry_exp_backoff: retry_exp_backoff,
            child_max_retries: max_retries,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn replay_or_interrupt(
        &mut self,
        ffqn: FunctionFqn,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());

        let new_child_execution_id =
            ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());

        trace!(child_execution_id = %new_child_execution_id,
            join_set_id = %new_join_set_id,
            "Querying history for child result, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );
        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { join_set_id } if *join_set_id == new_join_set_id => {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                } if *join_set_id == new_join_set_id
                    && *child_execution_id == new_child_execution_id =>
                {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched ChildExecutionRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at: _,
                } if *join_set_id == new_join_set_id => {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetResponse {
                    response:
                        JoinSetResponse::ChildExecutionFinished {
                            child_execution_id,
                            result,
                        },
                    ..
                } if *child_execution_id == new_child_execution_id => {
                    debug!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Found response in history: {found:?}");
                    self.events_idx += 1;
                    // TODO: Map FinishedExecutionError somehow
                    return Ok(result.clone().expect("FIXME"));
                }
                unexpected => {
                    return Err(FunctionError::NonDeterminismDetected(StrVariant::Arc(
                        Arc::from(format!(
                            "sleep: unexpected event {unexpected:?} at index {}",
                            self.events_idx
                        )),
                    )));
                }
            }
        }
        // not found in the history, persisting the request
        let created_at = (self.clock_fn)();
        let interrupt = true;

        let join_set = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSet {
                    join_set_id: new_join_set_id,
                },
            },
        };
        let child_exec_req = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id: new_join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: new_child_execution_id,
                    },
                },
            },
        };
        let join_next = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinNext {
                    join_set_id: new_join_set_id,
                    lock_expires_at: if interrupt {
                        created_at
                    } else {
                        self.execution_deadline
                    },
                },
            },
        };
        debug!(child_execution_id = %new_child_execution_id, join_set_id = %new_join_set_id, "Interrupted, scheduling child execution");

        // FIXME: Both writes should be in the same tx, otherwise if second one fails, the first one will keep failing on retries.
        self.version = self
            .db_connection
            .append_batch(
                vec![join_set, child_exec_req, join_next],
                self.execution_id,
                Some(self.version),
            )
            .await?;

        self.db_connection
            .create(
                created_at,
                new_child_execution_id,
                ffqn,
                Params::Empty,
                Some((self.execution_id, new_join_set_id)),
                None,
                self.child_retry_exp_backoff,
                self.child_max_retries,
            )
            .await?;

        Err(FunctionError::ChildExecutionRequest)
    }

    pub(crate) fn next_u128(&mut self) -> u128 {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::workflow_ctx::WorkflowCtx;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{journal::PendingState, DbConnection, HistoryEvent, JoinSetRequest, Version},
        FinishedExecutionResult,
    };
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
    use db::inmemory_dao::DbTask;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerError, WorkerResult},
    };
    use std::{fmt::Debug, sync::Arc, time::Duration};
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::{debug, info};
    use utils::time::{now, ClockFn};

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    const MOCK_FFQN_PTR: &FunctionFqn = &MOCK_FFQN;

    #[derive(Debug, Clone, arbitrary::Arbitrary)]
    #[allow(dead_code)]
    enum WorkflowStep {
        Call { ffqn: FunctionFqn },
    }

    #[derive(Clone, Debug)]
    struct WorkflowWorkerMock<C: ClockFn, DB: DbConnection> {
        steps: Vec<WorkflowStep>,
        clock_fn: C,
        db_connection: DB,
    }

    impl<C: ClockFn, DB: DbConnection> valuable::Valuable for WorkflowWorkerMock<C, DB> {
        fn as_value(&self) -> valuable::Value<'_> {
            "WorkflowWorkerMock".as_value()
        }

        fn visit(&self, _visit: &mut dyn valuable::Visit) {}
    }

    #[async_trait]
    impl<C: ClockFn + 'static, DB: DbConnection> Worker for WorkflowWorkerMock<C, DB> {
        async fn run(
            &self,
            execution_id: ExecutionId,
            _ffqn: FunctionFqn,
            _params: Params,
            events: Vec<HistoryEvent>,
            version: Version,
            execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            let seed = execution_id.random_part();
            let mut ctx = WorkflowCtx::new(
                execution_id,
                events,
                seed,
                self.clock_fn.clone(),
                self.db_connection.clone(),
                version,
                execution_deadline,
                Duration::ZERO,
                0,
            );
            for step in &self.steps {
                match step {
                    WorkflowStep::Call { ffqn } => ctx.replay_or_interrupt(ffqn.clone()).await,
                }
                .map_err(|err| (WorkerError::from(err), ctx.version))?;
            }
            Ok((SupportedFunctionResult::None, ctx.version))
        }

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
            Some(MOCK_FFQN_PTR).into_iter()
        }
    }

    // FIXME: verify non-determinism detection:
    // Start WorkflowWorkerMock, wait until it completes.
    // Copy its execution history to a new database
    // A. Swap two event history items
    // B. Swap two steps in WorkflowWorkerMock
    // C. Add new event history item
    // D. Add new step - needs whole execution history, must be done on another layer
    // E. Remove a step
    // F. Change the final result

    #[tokio::test]
    async fn check_determinism() {
        let _guard = test_utils::set_up();
        let unstructured_holder = UnstructuredHolder::new();
        let mut unstructured = unstructured_holder.unstructured();
        let steps = {
            unstructured
                .arbitrary_iter()
                .unwrap()
                .map(std::result::Result::unwrap)
                .collect::<Vec<_>>()
        };
        let created_at = now();
        info!(now = %created_at, "Generated steps: {steps:?}");
        let execution_id = ExecutionId::generate();
        info!("first execution");
        let first = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        info!("second execution");
        let second = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        assert_eq!(first, second);
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_steps(
        execution_id: ExecutionId,
        steps: Vec<WorkflowStep>,
        sim_clock: SimClock,
    ) -> (Vec<HistoryEvent>, FinishedExecutionResult) {
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let mut child_execution_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Call { .. }))
            .count();

        let timers_watcher_task = expired_timers_watcher::Task::spawn_new(
            db_connection.clone(),
            expired_timers_watcher::Config {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        );
        let workflow_exec_task = {
            let worker = Arc::new(WorkflowWorkerMock {
                steps,
                clock_fn: sim_clock.clock_fn(),
                db_connection: db_connection.clone(),
            });
            let exec_config = ExecConfig {
                ffqns: vec![MOCK_FFQN],
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            };
            ExecTask::spawn_new(db_connection.clone(), worker, exec_config, None)
        };
        // Create an execution.
        let created_at = sim_clock.now();
        db_connection
            .create(
                created_at,
                execution_id,
                MOCK_FFQN,
                Params::from([]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap();

        let mut processed = Vec::new();
        let mut spawned_child_executors = Vec::new();
        while let Some((join_set_id, req)) = db_connection
            .wait_for_pending_state_fn(
                execution_id,
                |execution_history| match &execution_history.pending_state {
                    PendingState::BlockedByJoinSet { join_set_id } => Some(Some((
                        *join_set_id,
                        execution_history
                            .join_set_requests(*join_set_id)
                            .cloned()
                            .collect::<Vec<_>>(),
                    ))),
                    PendingState::Finished => Some(None),
                    _ => None,
                },
                None,
            )
            .await
            .unwrap()
        {
            if processed.contains(&join_set_id) {
                continue;
            }
            assert_eq!(1, req.len());
            match req.first().unwrap() {
                JoinSetRequest::DelayRequest { .. } => {
                    unreachable!("unreachable")
                }
                JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                    assert!(child_execution_count > 0);
                    let child_request = db_connection.get(*child_execution_id).await.unwrap();
                    assert_eq!(Some((execution_id, join_set_id)), child_request.parent());
                    // execute
                    let child_exec_task = {
                        let worker = Arc::new(WorkflowWorkerMock {
                            steps: vec![],
                            clock_fn: sim_clock.clock_fn(),
                            db_connection: db_connection.clone(),
                        });
                        let exec_config = ExecConfig {
                            ffqns: vec![child_request.ffqn().clone()],
                            batch_size: 1,
                            lock_expiry: Duration::from_secs(1),
                            tick_sleep: TICK_SLEEP,
                            clock_fn: sim_clock.clock_fn(),
                        };
                        ExecTask::spawn_new(db_connection.clone(), worker, exec_config, None)
                    };
                    spawned_child_executors.push(child_exec_task);
                    child_execution_count -= 1;
                }
            }
            processed.push(join_set_id);
        }
        // must be finished at this point
        let execution_history = db_connection.get(execution_id).await.unwrap();
        assert_eq!(PendingState::Finished, execution_history.pending_state);
        drop(db_connection);
        for child_task in spawned_child_executors {
            child_task.close().await;
        }
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
        (
            execution_history.event_history().collect(),
            execution_history.finished_result().unwrap().clone(),
        )
    }
}
