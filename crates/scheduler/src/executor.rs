use crate::{
    storage::inmemory_dao::{api::Version, ExecutionEvent, ExecutionEventInner, ExecutorName},
    time::now,
    worker::{DbConnection, DbConnectionError, DbError, Worker},
};
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
use std::{marker::PhantomData, time::Duration};
use tracing::{debug, instrument};

struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID>> {
    db_connection: DB,
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    max_tick_sleep: Duration,
    worker: W,
    _phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest {
    executed_at: DateTime<Utc>,
    batch_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct ExecutionProgress<ID: ExecutionId> {
    execution_id: ID,
    result: Result<ExecutionEvent<ID>, DbError>,
}

impl<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID>> ExecTask<ID, DB, W> {
    async fn tick(
        &mut self,
        request: ExecTickRequest,
    ) -> Result<Vec<ExecutionProgress<ID>>, DbConnectionError> {
        let pending = self
            .db_connection
            .fetch_pending(request.batch_size, request.executed_at, self.ffqns.clone())
            .await?;

        let mut executions = Vec::new();
        for (execution, version, params, _) in pending {
            executions.push(
                self.lock_execute(execution, version, params, request.executed_at)
                    .await,
            );
        }
        Ok(executions)
    }

    #[instrument(skip_all, fields(%execution_id))]
    async fn lock_execute(
        &self,
        execution_id: ID,
        version: Version,
        params: Params,
        started_at: DateTime<Utc>,
    ) -> ExecutionProgress<ID> {
        match self
            .db_connection
            .lock(
                started_at,
                execution_id.clone(),
                version,
                self.executor_name.clone(),
                started_at + self.lock_expiry,
            )
            .await
        {
            Ok((event_history, version)) => {
                let result = self
                    .worker
                    .run(execution_id.clone(), params, event_history, version)
                    .await;
                ExecutionProgress {
                    execution_id,
                    result,
                }
            }
            Err(err) => {
                debug!("Database error: {err:?}");
                ExecutionProgress {
                    execution_id,
                    result: Err(err),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        storage::inmemory_dao::{
            tests::TickBasedDbConnection, DbTask, EventHistory, ExecutionEventInner,
        },
        time::now,
        worker::DbConnection,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params};
    use std::sync::Arc;
    use tracing::info;
    use tracing_unwrap::{OptionExt, ResultExt};

    use super::*;

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    struct SimpleWorker<DB: DbConnection<WorkflowId>> {
        db_connection: DB,
    }
    #[async_trait]
    impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for SimpleWorker<DB> {
        async fn run(
            &self,
            execution_id: WorkflowId,
            params: Params,
            events: Vec<EventHistory<WorkflowId>>,
            version: Version,
        ) -> Result<ExecutionEvent<WorkflowId>, DbError> {
            debug!("run");
            tokio::time::sleep(Duration::from_millis(10)).await;
            // persist
            let finished_event = ExecutionEvent {
                created_at: now(),
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
            };
            self.db_connection
                .insert(execution_id, version, finished_event.clone())
                .await?;

            Ok(finished_event)
        }
    }

    #[tokio::test]
    async fn tick_based() {
        set_up();
        let db_task = Arc::new(std::sync::Mutex::new(DbTask::new()));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        test(db_connection).await;
    }

    async fn test(db_connection: impl DbConnection<WorkflowId> + Clone + Sync) {
        set_up();

        info!("Now: {}", now());
        let max_tick_sleep = Duration::from_millis(500);
        let mut executor = ExecTask {
            db_connection: db_connection.clone(),
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep,
            worker: SimpleWorker {
                db_connection: db_connection.clone(),
            },
            _phantom_data: Default::default(),
        };
        let actual = executor
            .tick(ExecTickRequest {
                batch_size: 5,
                executed_at: now(),
            })
            .await;
        let executions = actual.unwrap_or_log();
        assert!(executions.is_empty(),);
        // Create an execution
        let execution_id = WorkflowId::generate();
        db_connection
            .insert(
                execution_id.clone(),
                0,
                ExecutionEvent {
                    created_at: now(),
                    event: ExecutionEventInner::Created {
                        ffqn: SOME_FFQN.to_owned(),
                        params: Params::default(),
                        parent: None,
                        scheduled_at: None,
                    },
                },
            )
            .await
            .unwrap_or_log();

        // execute!
        let requested_execution_at = now();
        let actual = executor
            .tick(ExecTickRequest {
                batch_size: 5,
                executed_at: requested_execution_at,
            })
            .await;
        let executions = actual.unwrap_or_log();

        assert_eq!(1, executions.len());
        let finished_event = assert_matches!(
            executions.get(0),
            Some(ExecutionProgress {
                execution_id: returned_id,
                result: Ok(event),
            }) if *returned_id == execution_id => event
        );
        assert_matches!(finished_event, ExecutionEvent {
            created_at: executed_at,
            event: ExecutionEventInner::Finished {
                result: Ok(SupportedFunctionResult::None),
            },
        } if *executed_at >= requested_execution_at);

        // check that DB contains the result.
        let (history, _) = db_connection.get(execution_id).await.unwrap_or_log();
        assert_eq!(3, history.len());
        assert_matches!(
            history[0],
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                ..
            }
        );
        assert_matches!(
            history[1],
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                ..
            }
        );
        assert_eq!(*finished_event, history[2]);
    }
}
