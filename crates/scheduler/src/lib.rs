use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use concepts::{Params, SupportedFunctionResult};
use std::time::Duration;

mod memory;
mod storage;

mod worker {
    use super::*;
    /// Worker commands sent to the worker executor.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum WorkerCommand<E: ExecutionId> {
        Yield,
        DelayFor(Duration),
        ExecuteBlocking {
            ffqn: FunctionFqn,
            params: Params,
            child_execution_id: E,
        },
        PublishResult(SupportedFunctionResult),
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    pub enum WorkerError {
        #[error("worker timed out")]
        Timeout,
        #[error("non-determinism detected, reason: `{0}`")]
        NonDeterminismDetected(String),
        #[error("worker failed")]
        Uncategorized, // Panic, cancellation
    }

    #[async_trait]
    pub trait Worker<S: WorkerStore<E>, E: ExecutionId> {
        async fn run(
            &self,
            workflow_id: E,
            params: Params,
            store: tokio::sync::OwnedMutexGuard<S>,
        ) -> Result<WorkerCommand<E>, WorkerError>;
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    #[error("non-determinism detected, reason: `{0}`")]
    pub(crate) struct NonDeterminismError(pub(crate) String);

    impl From<NonDeterminismError> for WorkerError {
        fn from(value: NonDeterminismError) -> Self {
            Self::NonDeterminismDetected(value.0)
        }
    }

    #[derive(Debug)]
    pub(crate) enum MaybeReplayResponse<E: ExecutionId> {
        ReplayResponse(ReplayResponse<E>),
        MissingResponse,
    }

    #[derive(Debug)]
    pub(crate) enum ReplayResponse<E: ExecutionId> {
        Completed,
        CompletedWithResult {
            child_execution_id: E,
            result: FinishedExecutionResult<E>,
        },
    }

    pub(crate) trait WorkerStore<E: ExecutionId> {
        fn next_id(&mut self) -> Result<E, NonDeterminismError>;

        fn next_event(
            &mut self,
            command: &WorkerCommand<E>,
        ) -> Result<MaybeReplayResponse<E>, NonDeterminismError>;
    }

    pub(crate) trait WriteableWorkerStore<E: ExecutionId>:
        WorkerStore<E> + Default + Send + 'static
    {
        fn restart(&mut self);

        fn persist_child_result(
            &mut self,
            child_execution_id: E,
            result: FinishedExecutionResult<E>,
        );

        fn persist_delay_passed(&mut self, duration: Duration);
    }
}

type FinishedExecutionResult<ID> = Result<SupportedFunctionResult, FinishedExecutionError<ID>>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError<ID: ExecutionId> {
    #[error("permanent timeout")]
    PermanentTimeout,
    // TODO PermanentFailure when error retries are implemented
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(String),
    #[error("uncategorized error")]
    UncategorizedError, //TODO: add reason?
    #[error("cancelled, reason: `{0}`")]
    Cancelled(String),
    #[error("continuing as {execution_id}")]
    ContinueAsNew {
        // TODO: Move to the OK part of the result
        execution_id: ID,
    },
    #[error("cancelled and starting {execution_id}")]
    CancelledWithNew { execution_id: ID },
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ExecutionStatusInfo<ID: ExecutionId> {
    Pending,
    Enqueued,
    DelayedUntil(DateTime<Utc>),
    Blocked,
    IntermittentTimeout(DateTime<Utc>),
    Finished(FinishedExecutionResult<ID>),
}
impl<ID: ExecutionId> ExecutionStatusInfo<ID> {
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished(_))
    }
}
