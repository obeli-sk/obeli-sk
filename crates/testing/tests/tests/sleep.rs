use assert_matches::assert_matches;
use concepts::{workflow_id::WorkflowId, FunctionFqn};
use rstest::*;
use runtime::{
    activity::ActivityConfig,
    database::Database,
    error::ExecutionError,
    event_history::EventHistory,
    runtime::{EngineConfig, RuntimeBuilder, RuntimeConfig},
    workflow::{AsyncActivityBehavior, WorkflowConfig},
};
use std::sync::{Arc, Once};
use std::{str::FromStr, time::Instant};
use tokio::sync::Mutex;
use tracing::info;
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static mut CHRMOE_TRACE_FILE_GUARD: Option<FlushGuard> = None;
static INIT: Once = Once::new();

fn set_up() -> Option<FlushGuard> {
    INIT.call_once(|| {
        let builder = tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env());
        let enable_chrome_layer = std::env::var("CHRMOE_TRACE")
            .ok()
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or_default();

        if enable_chrome_layer {
            let (chrome_layer, guard) = ChromeLayerBuilder::new()
                .trace_style(tracing_chrome::TraceStyle::Async)
                .build();
            unsafe {
                CHRMOE_TRACE_FILE_GUARD = Some(guard);
            }

            builder.with(chrome_layer).init();
        } else {
            builder.init();
        }
    });
    unsafe { CHRMOE_TRACE_FILE_GUARD.take() }
}

#[rstest]
#[tokio::test]
async fn test_async_activity(
    #[values("sleep-host-activity", "sleep-activity")] function: &str,
    #[values("Restart", "KeepWaiting")] activity_behavior: &str,
) -> Result<(), anyhow::Error> {
    let _guard = set_up();

    const SLEEP_MILLIS: u64 = 1;

    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    let runtime = runtime
        .build(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig {
                async_activity_behavior: AsyncActivityBehavior::from_str(activity_behavior)
                    .unwrap(),
            },
        )
        .await?;
    let _abort_handle = runtime.spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let params = Arc::new(vec![wasmtime::component::Val::U64(SLEEP_MILLIS)]);
    let stopwatch = Instant::now();
    database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::new(
                COUNTER
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    .to_string(),
            ),
            event_history,
            FunctionFqn::new(
                "testing:sleep-workflow/workflow".to_string(),
                function.to_string(),
            ),
            params,
        )
        .await
        .unwrap();
    let stopwatch = stopwatch.elapsed();

    info!("`{function}` finished in {} µs", stopwatch.as_micros());

    Ok(())
}

#[tokio::test]
async fn test_call_activity_with_version() -> Result<(), anyhow::Error> {
    set_up();

    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    let runtime = runtime
        .build(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let _abort_handle = runtime.spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history,
            FunctionFqn::new("testing:sleep-workflow/workflow", "run"),
            Arc::new(Vec::new()),
        )
        .await;
    res.unwrap();

    Ok(())
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
const ITERATIONS: u32 = 2;
const SLEEP_MILLIS: u64 = 1000;
const LIMIT: u32 = ITERATIONS - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitEngineKind {
    Workflow,
    Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display)]
enum LimitKind {
    #[display(fmt = "core")]
    Core,
    #[display(fmt = "component")]
    Component,
}
impl LimitKind {
    fn config(&self) -> EngineConfig {
        match self {
            Self::Core => {
                let mut config = wasmtime::PoolingAllocationConfig::default();
                config.total_core_instances(LIMIT);

                EngineConfig {
                    allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(config),
                }
            }
            Self::Component => {
                let mut config = wasmtime::PoolingAllocationConfig::default();
                config.total_component_instances(LIMIT);
                EngineConfig {
                    allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(config),
                }
            }
        }
    }
}

#[rstest]
#[tokio::test]
async fn test_limits(
    #[values(LimitEngineKind::Workflow, LimitEngineKind::Activity)]
    limit_engine_kind: LimitEngineKind,
    #[values(LimitKind::Core, LimitKind::Component)] limit_kind: LimitKind,
) -> Result<(), anyhow::Error> {
    set_up();
    let database = Database::new(100, 100);
    let engine_config = limit_kind.config();
    let mut runtime =
        RuntimeBuilder::new_with_config(if limit_engine_kind == LimitEngineKind::Activity {
            RuntimeConfig {
                activity_engine_config: engine_config,
                workflow_engine_config: EngineConfig::default(),
            }
        } else {
            RuntimeConfig {
                activity_engine_config: EngineConfig::default(),
                workflow_engine_config: engine_config,
            }
        });
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    let runtime = runtime
        .build(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let mut futures = Vec::new();
    let _abort_handle = runtime.spawn(&database);
    // Prepare futures, last one processed should fail.
    for _ in 0..ITERATIONS {
        let workflow_scheduler = database.workflow_scheduler();
        let join_handle = async move {
            let event_history = Arc::new(Mutex::new(EventHistory::default()));
            let params = Arc::new(vec![wasmtime::component::Val::U64(SLEEP_MILLIS)]);
            workflow_scheduler
                .schedule_workflow(
                    WorkflowId::new(
                        COUNTER
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                            .to_string(),
                    ),
                    event_history,
                    FunctionFqn::new("testing:sleep-workflow/workflow", "sleep-activity"),
                    params,
                )
                .await
        };
        futures.push(join_handle);
    }
    let err = futures_util::future::join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    if limit_engine_kind == LimitEngineKind::Activity {
        assert_matches!(err,
            ExecutionError::ActivityLimitReached { reason, .. }
            if reason == format!("maximum concurrent {limit_kind} instance limit of {LIMIT} reached"),
            "{:?}", match &err { ExecutionError::UnknownError{source,..} => source.to_string(), _ => "".to_string()}
        );
    } else {
        assert_matches!(err,
            ExecutionError::LimitReached { reason, .. }
            if reason == format!("maximum concurrent {limit_kind} instance limit of {LIMIT} reached"),
            "{:?}", match &err { ExecutionError::UnknownError{source,..} => source.to_string(), _ => "".to_string()}
        );
    }

    Ok(())
}
