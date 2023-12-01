use std::sync::Arc;

use runtime::{activity::Activities, event_history::EventHistory, workflow::Workflow};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let activities = Arc::new(
        Activities::new(test_programs_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string()).await?,
    );
    let workflow = Workflow::new(
        test_programs_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
        activities.clone(),
    )
    .await?;

    let iterations = 10;
    for workflow_function in ["fibow", "fiboa"] {
        let mut event_history = EventHistory::new();
        let params = vec![Val::U8(10), Val::U8(iterations)];
        let res = workflow
            .execute_all(
                &mut event_history,
                Some("testing:fibo-workflow/workflow"),
                workflow_function,
                &params,
            )
            .await;
        assert_eq!(res.unwrap(), Some(Val::U64(89)));
        assert_eq!(
            event_history.len(),
            if workflow_function.ends_with("a") {
                iterations as usize
            } else {
                0
            }
        );
    }
    Ok(())
}