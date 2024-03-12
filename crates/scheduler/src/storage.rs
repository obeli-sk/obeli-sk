pub mod inmemory_dao;

use self::journal::PendingState;
use crate::ExecutionHistory;
use crate::FinishedExecutionResult;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::Params;
use concepts::SupportedFunctionResult;
use concepts::{ExecutionId, FunctionFqn};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tracing_unwrap::OptionExt;

pub type Version = usize;

pub type ExecutorName = Arc<String>;

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq)]
#[display(fmt = "{event}")]
pub struct ExecutionEvent<ID: ExecutionId> {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner<ID>,
}

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq, arbitrary::Arbitrary)]
pub enum ExecutionEventInner<ID: ExecutionId> {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    #[display(fmt = "Created({ffqn}, `{scheduled_at:?}`)")]
    Created {
        ffqn: FunctionFqn,
        #[arbitrary(default)]
        params: Params,
        parent: Option<ID>,
        scheduled_at: Option<DateTime<Utc>>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display(fmt = "Locked(`{lock_expires_at}`, `{executor_name}`)")]
    Locked {
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure(`{expires_at}`)")]
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        #[arbitrary(value = Cow::Borrowed("reason"))]
        reason: Cow<'static, str>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentTimeout(`{expires_at}`)")]
    IntermittentTimeout { expires_at: DateTime<Utc> }, // TODO: Add executor name
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display(fmt = "Finished")]
    Finished {
        #[arbitrary(value = Ok(SupportedFunctionResult::None))]
        result: FinishedExecutionResult<ID>,
    },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    #[display(fmt = "CancelRequest")]
    CancelRequest,

    #[display(fmt = "HistoryEvent({event})")]
    HistoryEvent { event: HistoryEvent<ID> },
}

impl<ID: ExecutionId> ExecutionEventInner<ID> {
    fn appendable_only_in_lock(&self) -> bool {
        match self {
            Self::Locked { .. }
            | Self::IntermittentFailure { .. }
            | Self::IntermittentTimeout { .. }
            | Self::Finished { .. } => true,
            Self::HistoryEvent { event } => event.appendable_only_in_lock(),
            _ => false,
        }
    }

    pub fn is_retry(&self) -> bool {
        matches!(
            self,
            Self::IntermittentFailure { .. } | Self::IntermittentTimeout { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary)]
pub enum HistoryEvent<ID: ExecutionId> {
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Returns execution to `PotentiallyPending::PendingNow` state.
    Yield,
    #[display(fmt = "Persist")]
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    Persist {
        value: Vec<u8>,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinSet {
        joinset_id: ID,
    },
    // Joinset entry that will be unblocked by DelayFinishedAsyncResponse.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "DelayedUntilAsyncRequest({joinset_id})")]
    DelayedUntilAsyncRequest {
        joinset_id: ID,
        delay_id: ID,
        expires_at: DateTime<Utc>,
    },
    // Joinset entry that will be unblocked by ChildExecutionRequested.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "ChildExecutionAsyncRequest({joinset_id})")]
    ChildExecutionAsyncRequest {
        joinset_id: ID,
        child_execution_id: ID,
        ffqn: FunctionFqn,
        #[arbitrary(default)]
        params: Params,
    },
    // Execution continues without blocking as the next pending response is in the journal.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinNextFetched {
        joinset_id: ID,
    },
    // Moves the execution to `PotentiallyPending::PendingNow` if it is currently blocked on `JoinNextBlocking`.
    #[display(fmt = "AsyncResponse({joinset_id})")]
    AsyncResponse {
        joinset_id: ID,
        response: AsyncResponse<ID>,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Execution is `PotentiallyPending::BlockedByJoinSet` until the next response of the joinset arrives.
    JoinNextBlocking {
        joinset_id: ID,
    },
}

impl<ID: ExecutionId> HistoryEvent<ID> {
    fn appendable_only_in_lock(&self) -> bool {
        !matches!(self, Self::AsyncResponse { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, arbitrary::Arbitrary)]
pub enum AsyncResponse<ID: ExecutionId> {
    // Created by a scheduler sometime after DelayedUntilAsyncRequest.
    DelayFinishedAsyncResponse {
        delay_id: ID,
    },
    // Created by a scheduler sometime after ChildExecutionRequested.
    ChildExecutionAsyncResponse {
        child_execution_id: ID,
        #[arbitrary(value = Ok(SupportedFunctionResult::None))]
        result: FinishedExecutionResult<ID>,
    },
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum DbConnectionError {
    #[error("send error")]
    SendError,
    #[error("receive error")]
    RecvError,
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum RowSpecificError {
    #[error("validation failed: {0}")]
    ValidationFailed(Cow<'static, str>),
    #[error("version mismatch")]
    VersionMismatch,
    #[error("not found")]
    NotFound,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum DbError {
    #[error(transparent)]
    Connection(DbConnectionError),
    #[error(transparent)]
    RowSpecific(RowSpecificError),
}

pub type AppendResponse = Version;
pub type PendingExecution<ID> = (ID, Version, Params, Option<DateTime<Utc>>);
pub type LockResponse<ID> = (Vec<HistoryEvent<ID>>, Version);

#[derive(Debug, Clone)]
pub struct LockedExecution<ID: ExecutionId> {
    pub execution_id: ID,
    pub version: Version,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent<ID>>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub retry_exp_backoff: Duration,
    pub max_retries: u32,
}
pub type LockPendingResponse<ID> = Vec<LockedExecution<ID>>;
pub type CleanupExpiredLocks = usize; // number of expired locks

#[async_trait]
pub trait DbConnection<ID: ExecutionId>: Send + 'static + Clone {
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse<ID>, DbConnectionError>;

    /// Specialized `append` which does not require a version.
    async fn create(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        parent: Option<ID>,
        scheduled_at: Option<DateTime<Utc>>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    ) -> Result<AppendResponse, DbError> {
        let event = ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        };
        self.append(created_at, execution_id, Version::default(), event)
            .await
    }

    /// Specialized `append` which returns the event history.
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse<ID>, DbError>;

    async fn append(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<AppendResponse, DbError>;

    async fn get(&self, execution_id: ID) -> Result<ExecutionHistory<ID>, DbError>;

    async fn obtain_finished_result(
        &self,
        execution_id: ID,
    ) -> Result<FinishedExecutionResult<ID>, DbError> {
        let mut execution_events = loop {
            let execution_history = self.get(execution_id.clone()).await?;
            if execution_history.pending_state == PendingState::Finished {
                break execution_history.execution_events;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        Ok(
            assert_matches!(execution_events.pop().expect_or_log("must not be empty"),
            ExecutionEvent { event: ExecutionEventInner::Finished { result, .. } ,..}=> result),
        )
    }

    async fn cleanup_expired_locks(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CleanupExpiredLocks, DbConnectionError>;
}

pub mod journal {
    use super::{ExecutionEvent, ExecutionEventInner, ExecutorName, HistoryEvent};
    use crate::storage::{ExecutionHistory, RowSpecificError, Version};
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::{ExecutionId, FunctionFqn, Params};
    use std::{borrow::Cow, collections::VecDeque, time::Duration};
    use tracing_unwrap::OptionExt;

    #[derive(Debug)]
    pub(crate) struct ExecutionJournal<ID: ExecutionId> {
        execution_id: ID,
        pub(crate) pending_state: PendingState,
        execution_events: VecDeque<ExecutionEvent<ID>>,
    }

    impl<ID: ExecutionId> ExecutionJournal<ID> {
        pub(crate) fn new(
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<ID>,
            created_at: DateTime<Utc>,
            retry_exp_backoff: Duration,
            max_retries: u32,
        ) -> Self {
            let pending_state = match scheduled_at {
                Some(pending_at) => PendingState::PendingAt(pending_at),
                None => PendingState::PendingNow,
            };
            let event = ExecutionEvent {
                event: ExecutionEventInner::Created {
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                    retry_exp_backoff,
                    max_retries,
                },
                created_at,
            };
            Self {
                execution_id,
                pending_state,
                execution_events: VecDeque::from([event]),
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.execution_events.len()
        }

        pub(crate) fn created_at(&self) -> DateTime<Utc> {
            self.execution_events
                .iter()
                .rev()
                .next()
                .unwrap_or_log()
                .created_at
        }

        pub(crate) fn ffqn(&self) -> &FunctionFqn {
            // TODO: extract to a struct field
            match self.execution_events.get(0) {
                Some(ExecutionEvent {
                    event: ExecutionEventInner::Created { ffqn, .. },
                    ..
                }) => ffqn,
                _ => panic!("first event must be `Created`"),
            }
        }

        pub(crate) fn version(&self) -> Version {
            self.execution_events.len()
        }

        pub(crate) fn execution_id(&self) -> &ID {
            &self.execution_id
        }

        pub(crate) fn append(
            &mut self,
            created_at: DateTime<Utc>,
            event: ExecutionEventInner<ID>,
        ) -> Result<(), RowSpecificError> {
            if self.pending_state == PendingState::Finished {
                return Err(RowSpecificError::ValidationFailed(Cow::Borrowed(
                    "already finished",
                )));
            }

            if let ExecutionEventInner::Locked {
                executor_name,
                lock_expires_at,
            } = &event
            {
                if *lock_expires_at <= created_at {
                    return Err(RowSpecificError::ValidationFailed(Cow::Borrowed(
                        "invalid expiry date",
                    )));
                }
                match &self.pending_state {
                    PendingState::PendingNow => {} // ok to lock
                    PendingState::PendingAt(pending_start) => {
                        if *pending_start <= created_at {
                            // pending now, ok to lock
                        } else {
                            return Err(RowSpecificError::ValidationFailed(Cow::Owned(format!(
                                "cannot append {event} event, not yet pending"
                            ))));
                        }
                    }
                    PendingState::Locked {
                        executor_name: locked_by,
                        lock_expires_at,
                    } => {
                        if executor_name == locked_by {
                            // we allow extending the lock
                        } else if *lock_expires_at <= created_at {
                            // we allow locking after the old lock expired
                        } else {
                            return Err(RowSpecificError::ValidationFailed(Cow::Owned(format!(
                                "cannot append {event}, already in {}",
                                self.pending_state
                            ))));
                        }
                    }
                    PendingState::BlockedByJoinSet => {
                        return Err(RowSpecificError::ValidationFailed(Cow::Borrowed(
                            "cannot append Locked event when in BlockedByJoinSet state",
                        )));
                    }
                    PendingState::Finished => {
                        unreachable!() // handled at the beginning of the function
                    }
                }
            }
            let locked_now = matches!(self.pending_state, PendingState::Locked { lock_expires_at,.. } if lock_expires_at > created_at);
            if locked_now && !event.appendable_only_in_lock() {
                return Err(RowSpecificError::ValidationFailed(Cow::Owned(format!(
                    "cannot append {event} event in {}",
                    self.pending_state
                ))));
            }
            self.execution_events
                .push_back(ExecutionEvent { event, created_at });
            // update the state
            self.pending_state = self
                .execution_events
                .iter()
                .enumerate()
                .rev()
                .find_map(|(idx, event)| match (idx, &event.event) {
                    (
                        _,
                        ExecutionEventInner::Created {
                            scheduled_at: None, ..
                        },
                    ) => Some(PendingState::PendingNow),

                    (
                        _,
                        ExecutionEventInner::Created {
                            scheduled_at: Some(scheduled_at),
                            ..
                        },
                    ) => Some(PendingState::PendingAt(scheduled_at.clone())),

                    (_, ExecutionEventInner::Finished { .. }) => Some(PendingState::Finished),

                    (
                        _,
                        ExecutionEventInner::Locked {
                            executor_name,
                            lock_expires_at,
                        },
                    ) => Some(PendingState::Locked {
                        executor_name: executor_name.clone(),
                        lock_expires_at: *lock_expires_at,
                    }),

                    (_, ExecutionEventInner::IntermittentFailure { expires_at, .. }) => {
                        Some(PendingState::PendingAt(*expires_at))
                    }

                    (_, ExecutionEventInner::IntermittentTimeout { expires_at, .. }) => {
                        Some(PendingState::PendingAt(*expires_at))
                    }

                    (
                        _,
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Yield { .. },
                            ..
                        },
                    ) => Some(PendingState::PendingNow),
                    (
                        idx,
                        ExecutionEventInner::HistoryEvent {
                            event:
                                HistoryEvent::JoinNextBlocking {
                                    joinset_id: expected_join_set_id,
                                    ..
                                },
                            ..
                        },
                    ) => {
                        // pending if this event is followed by an async response
                        if self
                            .execution_events
                            .iter()
                            .skip(idx + 1)
                            .find(|event| {
                                matches!(event, ExecutionEvent {
                                event:
                                    ExecutionEventInner::HistoryEvent{event:
                                        HistoryEvent::AsyncResponse { joinset_id, .. },
                                    .. },
                            .. }
                            if expected_join_set_id == joinset_id)
                            })
                            .is_some()
                        {
                            Some(PendingState::PendingNow)
                        } else {
                            Some(PendingState::BlockedByJoinSet)
                        }
                    }
                    _ => None,
                })
                .expect_or_log("journal must begin with Created event");
            Ok(())
        }

        pub(crate) fn event_history(&self) -> Vec<HistoryEvent<ID>> {
            self.execution_events
                .iter()
                .filter_map(|event| {
                    if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                        Some(eh.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }

        pub(crate) fn retry_exp_backoff(&self) -> Duration {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { retry_exp_backoff, .. },
                ..
            }) => *retry_exp_backoff)
        }

        pub(crate) fn max_retries(&self) -> u32 {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { max_retries, .. },
                ..
            }) => *max_retries)
        }

        pub(crate) fn params(&self) -> Params {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
        }

        pub fn as_execution_history(&self) -> ExecutionHistory<ID> {
            ExecutionHistory {
                execution_events: self.execution_events.iter().cloned().collect(),
                version: self.version(),
                pending_state: self.pending_state.clone(),
            }
        }

        pub(crate) fn can_be_retried_after(&self) -> Option<Duration> {
            crate::can_be_retried_after(
                self.execution_events.iter(),
                self.max_retries(),
                self.retry_exp_backoff(),
            )
        }
    }

    #[derive(Debug, Clone, derive_more::Display, PartialEq, Eq)]
    pub enum PendingState {
        PendingNow,
        #[display(fmt = "Locked(`{lock_expires_at}`,`{executor_name}`)")]
        Locked {
            executor_name: ExecutorName,
            lock_expires_at: DateTime<Utc>,
        },
        #[display(fmt = "PendingAt(`{_0}`)")]
        PendingAt(DateTime<Utc>), // e.g. created with a schedule, intermittent timeout/failure
        BlockedByJoinSet,
        Finished,
    }
}
