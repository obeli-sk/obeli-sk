use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::storage::{CreateRequest, ExecutionEvent, ExecutionEventInner, HistoryEvent};
use concepts::storage::{ExecutionLog, PendingState, SpecificError, Version};
use concepts::ExecutionId;
use concepts::{FunctionFqn, Params, StrVariant};
use std::cmp::max;
use std::{collections::VecDeque, sync::Arc, time::Duration};

#[derive(Debug)]
pub struct ExecutionJournal {
    pub execution_id: ExecutionId,
    pub pending_state: PendingState,
    pub execution_events: VecDeque<ExecutionEvent>,
}

impl ExecutionJournal {
    #[must_use]
    pub fn new(req: CreateRequest) -> Self {
        let pending_state = match req.scheduled_at {
            Some(scheduled_at) => PendingState::PendingAt { scheduled_at },
            None => PendingState::PendingNow,
        };
        let event = ExecutionEvent {
            event: ExecutionEventInner::Created {
                ffqn: req.ffqn,
                params: req.params,
                scheduled_at: req.scheduled_at,
                parent: req.parent,
                retry_exp_backoff: req.retry_exp_backoff,
                max_retries: req.max_retries,
            },
            created_at: req.created_at,
        };
        Self {
            execution_id: req.execution_id,
            pending_state,
            execution_events: VecDeque::from([event]),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.execution_events.len()
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.execution_events.is_empty()
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        match self.execution_events.front().unwrap() {
            ExecutionEvent {
                event: ExecutionEventInner::Created { ffqn, .. },
                ..
            } => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    #[must_use]
    pub fn version(&self) -> Version {
        Version(self.execution_events.len())
    }

    #[must_use]
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn append(
        &mut self,
        created_at: DateTime<Utc>,
        event: ExecutionEventInner,
    ) -> Result<Version, SpecificError> {
        if self.pending_state == PendingState::Finished {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "already finished",
            )));
        }

        if let ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        } = &event
        {
            self.pending_state.can_append_lock(
                created_at,
                *executor_id,
                *run_id,
                *lock_expires_at,
            )?;
        }
        let locked_now = matches!(self.pending_state, PendingState::Locked { lock_expires_at,.. } if lock_expires_at > created_at);

        let appendable_only_in_lock = match &event {
            ExecutionEventInner::Locked { .. } // checked above, only if the lock is being extended by the same executor
            | ExecutionEventInner::IntermittentFailure { .. }
            | ExecutionEventInner::IntermittentTimeout { .. }
            | ExecutionEventInner::Finished { .. } => true,
            ExecutionEventInner::HistoryEvent { event } => event.appendable_only_in_lock(),
            _ => false,
        };

        if locked_now && !appendable_only_in_lock {
            return Err(SpecificError::ValidationFailed(StrVariant::Arc(Arc::from(
                format!("cannot append {event} event in {}", self.pending_state),
            ))));
        }
        self.execution_events
            .push_back(ExecutionEvent { created_at, event });
        // update the state
        self.pending_state = self.calculate_pending_state();
        Ok(self.version())
    }

    fn calculate_pending_state(&self) -> PendingState {
        self.execution_events
            .iter()
            .enumerate()
            .rev()
            .find_map(|(idx, event)| match (idx, &event.event) {
                (
                    _,
                    ExecutionEventInner::Created {
                        scheduled_at: None, ..
                    }
                    | ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::Yield { .. },
                        ..
                    },
                ) => Some(PendingState::PendingNow),

                (
                    _,
                    ExecutionEventInner::Created {
                        scheduled_at: Some(scheduled_at),
                        ..
                    },
                ) => Some(PendingState::PendingAt {
                    scheduled_at: *scheduled_at,
                }),

                (_, ExecutionEventInner::Finished { .. }) => Some(PendingState::Finished),

                (
                    _,
                    ExecutionEventInner::Locked {
                        executor_id,
                        lock_expires_at,
                        run_id,
                    },
                ) => Some(PendingState::Locked {
                    executor_id: *executor_id,
                    lock_expires_at: *lock_expires_at,
                    run_id: *run_id,
                }),

                (
                    _,
                    ExecutionEventInner::IntermittentFailure { expires_at, .. }
                    | ExecutionEventInner::IntermittentTimeout { expires_at, .. },
                ) => Some(PendingState::PendingAt {
                    scheduled_at: *expires_at,
                }),

                (
                    idx,
                    ExecutionEventInner::HistoryEvent {
                        event:
                            HistoryEvent::JoinNext {
                                join_set_id: expected_join_set_id,
                                lock_expires_at,
                            },
                        ..
                    },
                ) => {
                    // Did the async response arrive?
                    let resp = self.execution_events.iter().skip(idx + 1).find(|event| {
                        matches!(event, ExecutionEvent {
                                event:
                                    ExecutionEventInner::HistoryEvent { event:
                                        HistoryEvent::JoinSetResponse { join_set_id, .. },
                                    .. },
                                .. }
                                if expected_join_set_id == join_set_id)
                    });
                    if let Some(resp) = resp {
                        // Original executor has a chance to continue, but after expiry any executor can pick up the execution.
                        let scheduled_at = max(*lock_expires_at, resp.created_at);
                        Some(PendingState::PendingAt { scheduled_at })
                    } else {
                        Some(PendingState::BlockedByJoinSet {
                            join_set_id: *expected_join_set_id,
                            lock_expires_at: *lock_expires_at,
                        })
                    }
                }

                _ => None, // previous event
            })
            .expect("journal must begin with Created event")
    }

    pub fn event_history(&self) -> impl Iterator<Item = HistoryEvent> + '_ {
        self.execution_events.iter().filter_map(|event| {
            if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                Some(eh.clone())
            } else {
                None
            }
        })
    }

    #[must_use]
    pub fn retry_exp_backoff(&self) -> Duration {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { retry_exp_backoff, .. },
                ..
            }) => *retry_exp_backoff)
    }

    #[must_use]
    pub fn max_retries(&self) -> u32 {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { max_retries, .. },
                ..
            }) => *max_retries)
    }

    #[must_use]
    pub fn already_retried_count(&self) -> u32 {
        u32::try_from(
            self.execution_events
                .iter()
                .filter(|event| event.event.is_retry())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn params(&self) -> Params {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
    }

    #[must_use]
    pub fn as_execution_log(&self) -> ExecutionLog {
        ExecutionLog {
            execution_id: self.execution_id,
            events: self.execution_events.iter().cloned().collect(),
            version: self.version(),
            pending_state: self.pending_state,
        }
    }

    pub fn truncate(&mut self, len: usize) {
        self.execution_events.truncate(len);
        self.pending_state = self.calculate_pending_state();
    }
}