use runtime::activity::Activities;
use runtime::event_history::EventHistory;
use runtime::workflow::Workflow;
use std::{sync::Arc, time::Instant};

const IFC_FQN_FUNCTION_NAME_SEPARATOR: &str = ".";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let timer = Instant::now();
    let mut args: std::iter::Skip<std::env::Args> = std::env::args().skip(1);

    let activity_wasm_path = args.next().expect("activity wasm missing");
    let activities = Arc::new(Activities::new(activity_wasm_path).await?);

    let workflow_wasm_path = args.next().expect("workflow wasm missing");
    let workflow_function = args.next().expect("workflow function missing");
    let (ifc_fqn, workflow_function) =
        match workflow_function.split_once(IFC_FQN_FUNCTION_NAME_SEPARATOR) {
            None => (None, workflow_function.as_str()),
            Some((ifc_fqn, workflow_function)) => (Some(ifc_fqn), workflow_function),
        };

    let workflow = Workflow::new(workflow_wasm_path, activities.clone()).await?;
    println!("Initialized in {duration:?}", duration = timer.elapsed());
    println!();

    let mut event_history = EventHistory::new();
    {
        println!(
            "Starting workflow {function}",
            function = if let Some(ifc_fqn) = &ifc_fqn {
                format!("{ifc_fqn}{IFC_FQN_FUNCTION_NAME_SEPARATOR}{workflow_function}")
            } else {
                workflow_function.to_string()
            }
        );
        let timer = Instant::now();
        let res = workflow
            .execute_all(&mut event_history, ifc_fqn, workflow_function)
            .await;
        println!(
            "Finished: in {duration:?} {res:?}, event history size: {len}",
            duration = timer.elapsed(),
            len = event_history.len()
        );
    }
    println!();
    {
        println!("Replaying");
        let timer = Instant::now();
        let res = workflow
            .execute_all(&mut event_history, ifc_fqn, workflow_function)
            .await;
        println!(
            "Finished: in {duration:?} {res:?}",
            duration = timer.elapsed()
        );
    }
    Ok(())
}
