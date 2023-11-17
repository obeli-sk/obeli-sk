use std::{fmt::Debug, time::Duration};

use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Config, Engine, Store,
};

// generate my_org::my_workflow::host_activities::Host trait
wasmtime::component::bindgen!({
    world: "keep-wasmtime-bindgen-happy",
    path: "../wit/host-world.wit",
    async: true,
});

pub async fn execute<S, T>(
    mut store: S,
    instance_pre: &wasmtime::component::InstancePre<T>,
    name: &str,
) -> wasmtime::Result<String>
where
    S: wasmtime::AsContextMut<Data = T>,
    T: Send,
{
    let instance = instance_pre.instantiate_async(&mut store).await?;
    // new
    let execute = {
        let mut store = store.as_context_mut();
        let mut exports = instance.exports(&mut store);
        let mut __exports = exports.root();
        *__exports.typed_func::<(), (String,)>(name)?.func()
    };
    // call_execute
    let callee = unsafe { wasmtime::component::TypedFunc::<(), (String,)>::new_unchecked(execute) };
    let (ret0,) = callee.call_async(&mut store, ()).await?;
    callee.post_return_async(&mut store).await?;
    Ok(ret0)
}

struct HostImports<'a, E: AsRef<Event>> {
    event_history: &'a [E],
    idx: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum Event {
    Sleep(Duration),
}

#[derive(thiserror::Error, Debug)]
enum HostFunctionError {
    #[error("Non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("Handle {0:?}")]
    Handle(Event),
}

#[async_trait::async_trait]
impl my_org::my_workflow::host_activities::Host for HostImports<'_, EventWrapper> {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event::Sleep(Duration::from_millis(millis));
        match self.event_history.get(self.idx).map(AsRef::as_ref) {
            Some(current) if *current == event => {
                println!("Skipping {current:?}");
                self.idx += 1;
                Ok(())
            }
            Some(other) => {
                anyhow::bail!(HostFunctionError::NonDeterminismDetected(format!(
                    "Expected {event:?}, got {other:?}"
                )))
            }
            None => {
                // new event needs to be handled by the runtime
                anyhow::bail!(HostFunctionError::Handle(event))
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ExecutionError {
    #[error("Non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("Handle {0:?}")]
    Handle(EventWrapper),
    #[error("Unknown error: {0:?}")]
    UnknownError(anyhow::Error),
}

// struct that holds the wasmtime error in order to avoid cloning the event
struct EventWrapper(anyhow::Error);
impl AsRef<Event> for EventWrapper {
    fn as_ref(&self) -> &Event {
        match self
            .0
            .source()
            .expect("source must be present")
            .downcast_ref::<HostFunctionError>()
            .expect("source must be HostFunctionError")
        {
            HostFunctionError::Handle(event) => event,
            other => panic!("HostFunctionError::Handle expected, got {other:?}"),
        }
    }
}
impl Debug for EventWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

async fn execute_next_step(
    execution_config: &mut ExecutionConfig<'_, EventWrapper>,
    engine: &Engine,
    instance_pre: &InstancePre<HostImports<'_, EventWrapper>>,
) -> Result<String, ExecutionError> {
    // Instantiate the component
    let mut store = Store::new(
        &engine,
        HostImports {
            event_history: execution_config.event_history,
            idx: 0,
        },
    );
    execute(&mut store, &instance_pre, execution_config.function_name)
        .await
        .map_err(|err| {
            match err
                .source()
                .and_then(|source| source.downcast_ref::<HostFunctionError>())
            {
                Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                    ExecutionError::NonDeterminismDetected(reason.clone())
                }
                Some(HostFunctionError::Handle(_)) => ExecutionError::Handle(EventWrapper(err)),
                None => ExecutionError::UnknownError(err),
            }
        })
}

async fn execute_all(
    execution_config: &mut ExecutionConfig<'_, EventWrapper>,
    engine: &Engine,
    component: &Component,
    linker: &Linker<HostImports<'_, EventWrapper>>,
) -> wasmtime::Result<String> {
    let instance_pre = linker.instantiate_pre(component)?;
    loop {
        let res = execute_next_step(execution_config, engine, &instance_pre).await;
        match res {
            Ok(output) => return Ok(output),
            Err(ExecutionError::Handle(event)) => {
                println!("Handling {event:?}");
                match event.as_ref() {
                    Event::Sleep(duration) => tokio::time::sleep(*duration).await,
                }
                execution_config.event_history.push(event);
            }

            Err(ExecutionError::NonDeterminismDetected(reason)) => {
                panic!("Non determinism detected: {reason}")
            }
            Err(ExecutionError::UnknownError(err)) => panic!("Unknown error {err:?}"),
        }
    }
}

#[derive(Debug)]
struct ExecutionConfig<'a, E: AsRef<Event>> {
    event_history: &'a mut Vec<E>,
    function_name: &'a str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let wasm = args.next().expect("Parameters: file.wasm function_name");
    let function_name = args.next().expect("Parameters: file.wasm function_name");
    // Enable component model and async support
    let mut config = Config::new();
    config.async_support(true).wasm_component_model(true);
    // Create a wasmtime execution context
    let engine = Engine::new(&config)?;
    let mut linker = Linker::new(&engine);
    // add host functions
    my_org::my_workflow::host_activities::add_to_linker(
        &mut linker,
        |state: &mut HostImports<_>| state,
    )?;
    // Read and compile the wasm component
    let component = Component::from_file(&engine, wasm)?;
    // Prepare ExecutionConfig
    let mut event_history = Vec::new();
    let mut execution_config = ExecutionConfig {
        event_history: &mut event_history,
        function_name: &function_name,
    };
    // Execute once recording the events
    let output = execute_all(&mut execution_config, &engine, &component, &linker).await?;
    println!("Finished: {output}, {execution_config:?}");
    println!();
    // Execute by replaying the event history
    let output = execute_all(&mut execution_config, &engine, &component, &linker).await?;
    println!("Finished: {output}, {execution_config:?}");
    Ok(())
}
