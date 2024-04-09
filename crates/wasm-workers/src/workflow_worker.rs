use crate::workflow_ctx::{FunctionError, WorkflowCtx};
use crate::{EngineConfig, WasmComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ConfigId;
use concepts::storage::{DbConnection, HistoryEvent, Version};
use concepts::{ExecutionId, FunctionFqn, StrVariant};
use concepts::{Params, SupportedFunctionResult};
use executor::worker::FatalError;
use executor::worker::{Worker, WorkerError};
use std::collections::HashMap;
use std::error::Error;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, trace};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

#[must_use]
pub fn workflow_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace(true);
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap())
}

/// Defines behavior of the wasm runtime when `HistoryEvent::JoinNextBlocking` is requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinNextBlockingStrategy {
    /// Shut down the current runtime. When the `JoinSetResponse` is appended, workflow is reexecuted with a new `RunId`.
    Interrupt,
    /// Keep the execution hot. Worker will poll the database until the execution lock expires.
    Await,
}

impl Default for JoinNextBlockingStrategy {
    fn default() -> Self {
        Self::Interrupt
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowConfig<C: ClockFn, DB: DbConnection> {
    pub config_id: ConfigId,
    pub clock_fn: C,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub db_connection: DB,
    pub child_retry_exp_backoff: Duration,
    pub child_max_retries: u32,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn, DB: DbConnection> {
    config: WorkflowConfig<C, DB>,
    engine: Arc<Engine>,
    exported_ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<WorkflowCtx<C, DB>>,
    component: wasmtime::component::Component,
}

impl<C: ClockFn, DB: DbConnection> WorkflowWorker<C, DB> {
    #[tracing::instrument(skip_all, fields(wasm_path = %wasm_component.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        wasm_component: WasmComponent,
        config: WorkflowConfig<C, DB>,
        engine: Arc<Engine>,
    ) -> Result<Self, WasmFileError> {
        const HOST_ACTIVITY_IFC_STRING: &str = "my-org:workflow-engine/host-activities";
        let mut linker = wasmtime::component::Linker::new(&engine);
        // Compile the wasm component
        let component = wasmtime::component::Component::from_binary(&engine, &wasm_component.wasm)
            .map_err(|err| {
                WasmFileError::CompilationError(wasm_component.wasm_path.clone(), err.into())
            })?;
        // Mock imported functions

        let imported_interfaces = {
            let imp_fns_to_metadata = wasm_tools::functions_to_metadata(
                wasm_component.imported_ifc_fns,
            )
            .map_err(|err| {
                WasmFileError::FunctionMetadataError(wasm_component.wasm_path.clone(), err)
            })?;
            wasm_tools::group_by_ifc_to_fn_names(imp_fns_to_metadata.keys())
        };

        for (ifc_fqn, functions) in &imported_interfaces {
            if ifc_fqn.deref() == HOST_ACTIVITY_IFC_STRING {
                // Skip host-implemented functions
                continue;
            }
            trace!("Adding imported interface {ifc_fqn} to the linker");
            if let Ok(mut linker_instance) = linker.instance(ifc_fqn) {
                for function_name in functions {
                    let ffqn = FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: function_name.clone(),
                    };
                    trace!("Adding mock for imported function {ffqn} to the linker");
                    let res = linker_instance.func_new_async(&component, function_name, {
                        let ffqn = ffqn.clone();
                        move |mut store_ctx: wasmtime::StoreContextMut<'_, WorkflowCtx<C, DB>>,
                              params: &[Val],
                              results: &mut [Val]| {
                            let ffqn = ffqn.clone();
                            Box::new(async move {
                                Ok(store_ctx
                                    .data_mut()
                                    .call_imported_func(ffqn, params, results)
                                    .await?)
                            })
                        }
                    });
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            debug!("Skipping mocking of {ffqn}");
                        } else {
                            return Err(WasmFileError::LinkingError {
                                file: wasm_component.wasm_path.clone(),
                                reason: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            });
                        }
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}");
            }
        }
        WorkflowCtx::add_to_linker(&mut linker).map_err(|err| WasmFileError::LinkingError {
            file: wasm_component.wasm_path.clone(),
            reason: StrVariant::Arc(Arc::from("cannot add host activities".to_string())),
            err: err.into(),
        })?;
        Ok(Self {
            config,
            engine,
            exported_ffqns_to_results_len: wasm_component.exported_ffqns_to_results_len,
            linker,
            component,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static, DB: DbConnection> Worker for WorkflowWorker<C, DB> {
    async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), WorkerError> {
        match self
            .run(
                execution_id,
                ffqn,
                params,
                events,
                version,
                execution_deadline,
            )
            .await
        {
            Ok((supported_result, version)) => Ok((supported_result, version)),
            Err(RunError::FunctionCall(err, version)) => {
                match err
                    .source()
                    .and_then(|source| source.downcast_ref::<FunctionError>())
                {
                    Some(err) => Err(err.clone().into_worker_error(version)),
                    None => Err(WorkerError::IntermittentError {
                        err,
                        reason: StrVariant::Static("uncategorized error"),
                        version,
                    }),
                }
            }
            Err(RunError::WorkerError(err)) => Err(err),
        }
    }

    fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.exported_ffqns_to_results_len.keys()
    }
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    WorkerError(WorkerError),
    #[error("wasm function call error")]
    FunctionCall(Box<dyn Error + Send + Sync>, Version),
}

impl<C: ClockFn, DB: DbConnection> WorkflowWorker<C, DB> {
    #[tracing::instrument(skip_all)]
    async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), RunError> {
        let results_len = *self
            .exported_ffqns_to_results_len
            .get(&ffqn)
            .expect("executor must only run existing functions");
        trace!("Params: {params:?}, results_len:{results_len}",);
        let (instance, mut store) = {
            let seed = execution_id.random_part();
            let ctx = WorkflowCtx::new(
                execution_id,
                events,
                seed,
                self.config.clock_fn.clone(),
                self.config.join_next_blocking_strategy,
                self.config.db_connection.clone(),
                version,
                execution_deadline,
                self.config.child_retry_exp_backoff,
                self.config.child_max_retries,
            );
            let mut store = Store::new(&self.engine, ctx);
            let instance = match self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
            {
                Ok(instance) => instance,
                Err(err) => {
                    return Err(RunError::WorkerError(WorkerError::IntermittentError {
                        reason: StrVariant::Static("cannot instantiate"),
                        err: err.into(),
                        version: store.into_data().version,
                    }));
                }
            };
            (instance, store)
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        let call_function = async move {
            let func = {
                let mut exports = instance.exports(&mut store);
                let mut exports_instance = exports.root();
                let mut exports_instance = exports_instance
                    .instance(&ffqn.ifc_fqn)
                    .expect("interface must be found");
                exports_instance
                    .func(&ffqn.function_name)
                    .expect("function must be found")
            };
            let param_types = func.params(&store);
            let params = match params.as_vals(&param_types) {
                Ok(params) => params,
                Err(err) => {
                    return Err(RunError::WorkerError(WorkerError::FatalError(
                        FatalError::ParamsParsingError(err),
                        store.into_data().version,
                    )));
                }
            };
            let mut results = vec![Val::Bool(false); results_len];
            if let Err(err) = func.call_async(&mut store, &params, &mut results).await {
                return Err(RunError::FunctionCall(
                    err.into(),
                    store.into_data().version,
                ));
            } // guest panic exits here
            let result = match SupportedFunctionResult::new(results) {
                Ok(result) => result,
                Err(err) => {
                    return Err(RunError::WorkerError(WorkerError::FatalError(
                        FatalError::ResultParsingError(err),
                        store.into_data().version,
                    )));
                }
            };
            if let Err(err) = func.post_return_async(&mut store).await {
                return Err(RunError::WorkerError(WorkerError::IntermittentError {
                    reason: StrVariant::Static("wasm post function call error"),
                    err: err.into(),
                    version: store.into_data().version,
                }));
            }

            Ok((result, store.into_data().version))
        };
        let started_at = (self.config.clock_fn)();
        let deadline_duration = (execution_deadline - started_at)
            .to_std()
            .unwrap_or_default();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! {
            res = call_function =>{
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished");
                res
            },
            () = tokio::time::sleep(deadline_duration) => {
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %(self.config.clock_fn)(), "Timed out");
                Err(RunError::WorkerError(WorkerError::IntermittentTimeout))
            }
        }
    }
}

mod valuable {
    use super::WorkflowWorker;
    use concepts::storage::DbConnection;
    use utils::time::ClockFn;

    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("config_id")];
    impl<C: ClockFn, DB: DbConnection> ::valuable::Structable for WorkflowWorker<C, DB> {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("WorkflowWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<C: ClockFn, DB: DbConnection> ::valuable::Valuable for WorkflowWorker<C, DB> {
        fn as_value(&self) -> ::valuable::Value<'_> {
            ::valuable::Value::Structable(self)
        }
        fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
            visitor.visit_named_fields(&::valuable::NamedValues::new(
                FIELDS,
                &[::valuable::Value::String(
                    &self.config.config_id.to_string(),
                )],
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: test epoch and select-based timeouts
    use super::*;
    use crate::{
        activity_worker::tests::{spawn_activity_fibo, FIBO_10_INPUT, FIBO_10_OUTPUT},
        EngineConfig,
    };
    use assert_matches::assert_matches;
    use concepts::storage::{journal::PendingState, CreateRequest, DbConnection};
    use concepts::{prefixed_ulid::ConfigId, ExecutionId, Params};
    use db::inmemory_dao::DbTask;
    use executor::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        expired_timers_watcher,
    };
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use utils::time::now;
    use val_json::wast_val::WastVal;
    use wasmtime::component::Val;

    pub const FIBO_WORKFLOW_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_fibo_workflow_builder::FIBOA); // fiboa: func(n: u8, iterations: u32) -> u64;
    const SLEEP_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::SLEEP_HOST_ACTIVITY); // sleep-host-activity: func(millis: u64);

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    pub(crate) fn spawn_workflow_fibo<DB: DbConnection>(db_connection: DB) -> ExecutorTaskHandle {
        let fibo_worker = Arc::new(
            WorkflowWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(
                    test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                ))
                .unwrap(),
                WorkflowConfig {
                    config_id: ConfigId::generate(),
                    clock_fn: now,
                    join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
                    db_connection: db_connection.clone(),
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                },
                workflow_engine(EngineConfig::default()),
            )
            .unwrap(),
        );

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_WORKFLOW_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn: now,
        };
        ExecTask::spawn_new(db_connection, fibo_worker, exec_config, None)
    }

    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity() {
        const INPUT_ITERATIONS: u32 = 1;

        let _guard = test_utils::set_up();
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let workflow_exec_task = spawn_workflow_fibo(db_connection.clone());
        // Create an execution.
        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = now();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_WORKFLOW_FFQN,
                params: Params::from([Val::U8(FIBO_10_INPUT), Val::U32(INPUT_ITERATIONS)]),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();
        // Should end as BlockedByJoinSet
        db_connection
            .wait_for_pending_state_fn(
                execution_id,
                |exe_history| {
                    matches!(
                        exe_history.pending_state,
                        PendingState::BlockedByJoinSet { .. }
                    )
                    .then_some(())
                },
                None,
            )
            .await
            .unwrap();

        // Execution should call the activity and finish
        let activity_exec_task = spawn_activity_fibo(db_connection.clone());

        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionResult::Infallible(val) => val);
        assert_eq!(
            FIBO_10_OUTPUT,
            assert_matches!(res, WastVal::U64(actual) => actual),
        );

        drop(db_connection);
        workflow_exec_task.close().await;
        activity_exec_task.close().await;
        db_task.close().await;
    }

    pub(crate) fn spawn_workflow_sleep<DB: DbConnection>(
        db_connection: DB,
        clock_fn: impl ClockFn + 'static,
    ) -> ExecutorTaskHandle {
        let worker = Arc::new(
            WorkflowWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                ))
                .unwrap(),
                WorkflowConfig {
                    config_id: ConfigId::generate(),
                    clock_fn: clock_fn.clone(),
                    join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
                    db_connection: db_connection.clone(),
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                },
                workflow_engine(EngineConfig::default()),
            )
            .unwrap(),
        );

        let exec_config = ExecConfig {
            ffqns: vec![SLEEP_HOST_ACTIVITY_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn,
        };
        ExecTask::spawn_new(db_connection, worker, exec_config, None)
    }

    #[tokio::test]
    async fn sleep_should_be_persisted_after_executor_restart() {
        const SLEEP_MILLIS: u32 = 100;
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::new(now());
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let workflow_exec_task = spawn_workflow_sleep(db_connection.clone(), sim_clock.clock_fn());
        let timers_watcher_task = expired_timers_watcher::Task::spawn_new(
            db_connection.clone(),
            expired_timers_watcher::Config {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        );
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: SLEEP_HOST_ACTIVITY_FFQN,
                params: Params::from([Val::U32(SLEEP_MILLIS)]),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        db_connection
            .wait_for_pending_state_fn(
                execution_id,
                |exe_history| {
                    matches!(
                        exe_history.pending_state,
                        PendingState::BlockedByJoinSet { .. }
                    )
                    .then_some(())
                },
                None,
            )
            .await
            .unwrap();

        workflow_exec_task.close().await;
        sim_clock.sleep(Duration::from_millis(u64::from(SLEEP_MILLIS)));
        // Restart worker
        let workflow_exec_task = spawn_workflow_sleep(db_connection.clone(), sim_clock.clock_fn());
        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(SupportedFunctionResult::None, res);
        drop(db_connection);
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
    }
}
