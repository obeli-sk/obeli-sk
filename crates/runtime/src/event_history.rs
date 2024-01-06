use crate::{
    activity::ActivityRequest, queue::activity_queue::ActivityQueueSender,
    workflow::AsyncActivityBehavior, workflow_id::WorkflowId, ActivityFailed, ActivityResponse,
    FunctionFqn,
};
use assert_matches::assert_matches;
use std::{fmt::Debug, sync::Arc, time::Duration};
use tracing::{debug, error, trace};
use wasmtime::component::{Linker, Val};

// generate Host trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

#[derive(Clone, Debug, PartialEq)]
pub enum SupportedFunctionResult {
    None,
    Single(Val),
}

impl SupportedFunctionResult {
    pub fn new(mut vec: Vec<Val>) -> Self {
        if vec.is_empty() {
            Self::None
        } else if vec.len() == 1 {
            Self::Single(vec.pop().unwrap())
        } else {
            unimplemented!("multi-value return types are not supported")
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Single(_) => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl IntoIterator for SupportedFunctionResult {
    type Item = Val;
    type IntoIter = std::option::IntoIter<Val>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::None => None.into_iter(),
            Self::Single(item) => Some(item).into_iter(),
        }
    }
}

pub(crate) struct HostImports {
    pub(crate) current_event_history: CurrentEventHistory,
}

impl HostImports {
    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), anyhow::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(
            linker,
            |state: &mut HostImports| state,
        )
    }
}

pub(crate) const HOST_ACTIVITY_SLEEP_FQN: FunctionFqn<'static> =
    FunctionFqn::new("my-org:workflow-engine/host-activities", "sleep");
// When calling host functions, create events and continue or interrupt the execution.
#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for HostImports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event {
            request: ActivityRequest {
                fqn: Arc::new(HOST_ACTIVITY_SLEEP_FQN),
                params: Arc::new(vec![Val::U64(millis)]),
            },
            kind: EventKind::ActivityAsync,
        };
        let replay_result = self
            .current_event_history
            .replay_handle_interrupt(event)
            .await?;
        assert!(replay_result.is_empty());
        Ok(())
    }

    async fn noop(&mut self) -> wasmtime::Result<()> {
        const FQN: FunctionFqn<'static> =
            FunctionFqn::new("my-org:workflow-engine/host-activities", "noop");
        let event = Event {
            request: ActivityRequest {
                fqn: Arc::new(FQN),
                params: Arc::new(vec![]),
            },
            kind: EventKind::HostActivitySync(HostActivitySync::Noop),
        };
        let replay_result = self
            .current_event_history
            .replay_handle_interrupt(event)
            .await?;
        assert!(replay_result.is_empty());
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]

pub(crate) struct Event {
    pub(crate) request: ActivityRequest,
    pub(crate) kind: EventKind,
}

impl Event {
    pub fn new_from_wasm_activity(fqn: Arc<FunctionFqn<'static>>, params: Arc<Vec<Val>>) -> Self {
        Self {
            request: ActivityRequest { fqn, params },
            kind: EventKind::ActivityAsync,
        }
    }

    pub(crate) async fn handle_activity_async(
        request: ActivityRequest,
        activity_queue_sender: &ActivityQueueSender,
    ) -> Result<SupportedFunctionResult, ActivityFailed> {
        if *request.fqn == HOST_ACTIVITY_SLEEP_FQN {
            assert_eq!(1, request.params.len(), "sleep expects a single argument");
            let millis =
                assert_matches!(request.params.first().unwrap(), Val::U64(millis) => *millis);
            tokio::time::sleep(Duration::from_millis(millis)).await;
            Ok(SupportedFunctionResult::None)
        } else {
            activity_queue_sender
                .push(request)
                .await
                .await
                .expect("sender should not be dropped")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventKind {
    HostActivitySync(HostActivitySync),
    ActivityAsync,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HostActivitySync {
    Noop,
}
impl HostActivitySync {
    fn handle(&self) -> Result<SupportedFunctionResult, ActivityFailed> {
        match self {
            Self::Noop => Ok(SupportedFunctionResult::None),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum HostFunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error("interrupt: `{fqn}`", fqn = request.fqn)]
    Interrupt { request: ActivityRequest },
    #[error(transparent)]
    ActivityFailed(#[from] ActivityFailed),
}

pub(crate) struct CurrentEventHistory {
    workflow_id: WorkflowId,
    run_id: u64,
    activity_queue_writer: ActivityQueueSender,
    pub(crate) event_history: EventHistory,
    async_activity_behavior: AsyncActivityBehavior,
    replay_idx: usize,
    replay_len: usize,
}

impl CurrentEventHistory {
    pub(crate) fn new(
        workflow_id: WorkflowId,
        run_id: u64,
        event_history: EventHistory,
        activity_queue_writer: ActivityQueueSender,
        async_activity_behavior: AsyncActivityBehavior,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            activity_queue_writer,
            replay_len: event_history.len(),
            event_history,
            async_activity_behavior,
            replay_idx: 0,
        }
    }

    pub(crate) fn replay_is_drained(&self) -> bool {
        self.replay_idx == self.replay_len
    }

    fn assert_replay_is_drained(&self) {
        assert_eq!(
            self.replay_idx, self.replay_len,
            "replay log has not been drained"
        );
    }

    pub(crate) async fn persist_start(&mut self, request: &ActivityRequest) {
        self.assert_replay_is_drained();
        self.event_history.persist_start(request).await
    }

    pub(crate) async fn persist_end(&mut self, request: ActivityRequest, val: ActivityResponse) {
        self.assert_replay_is_drained();
        self.event_history.persist_end(request, val).await;
    }

    #[allow(clippy::type_complexity)]
    fn next(
        &mut self,
    ) -> Option<(
        &Arc<FunctionFqn<'static>>,
        &Arc<Vec<Val>>,
        &Result<SupportedFunctionResult, ActivityFailed>,
    )> {
        if self.replay_idx < self.replay_len {
            let (fqn, params, res) = self
                .event_history
                .vec
                .get(self.replay_idx)
                .expect("must contain value");
            self.replay_idx += 1;
            Some((fqn, params, res))
        } else {
            None
        }
    }

    pub(crate) async fn replay_handle_interrupt(
        &mut self,
        event: Event,
    ) -> Result<SupportedFunctionResult, HostFunctionError> {
        let workflow_id = self.workflow_id.clone();
        let run_id = self.run_id;
        let found = self.next();
        let found_matches = matches!(found,  Some((found_fqn, found_params, _replay_result))
            if event.request.fqn == *found_fqn && event.request.params == *found_params);
        trace!(
            "[{workflow_id},{run_id}] replay_handle_interrupt {fqn}, found: {found_matches}",
            fqn = event.request.fqn,
        );
        match (event, found_matches, found) {
            // Continue running on HostActivitySync
            (
                Event {
                    request,
                    kind: EventKind::HostActivitySync(host_activity_sync),
                },
                _,
                None,
            ) => {
                debug!("[{workflow_id},{run_id}] Running {host_activity_sync:?}");
                self.persist_start(&request).await;
                let res = host_activity_sync.handle();
                self.persist_end(request, res.clone()).await;
                Ok(res?)
            }
            // Replay if found
            (event, true, Some((_, _, replay_result))) => {
                debug!(
                    "[{workflow_id},{run_id}] Replaying {fqn}",
                    fqn = event.request.fqn
                );
                Ok(replay_result.clone()?)
            }
            // New event needs to be handled by the runtime, interrupt or execute it.
            (
                Event {
                    request,
                    kind: EventKind::ActivityAsync,
                },
                _,
                None,
            ) => match self.async_activity_behavior {
                AsyncActivityBehavior::Restart => {
                    debug!(
                        "[{workflow_id},{run_id}] Interrupting {fqn}",
                        fqn = request.fqn
                    );
                    Err(HostFunctionError::Interrupt { request })
                }
                AsyncActivityBehavior::KeepWaiting => {
                    debug!(
                        "[{workflow_id},{run_id}] Executing {fqn}",
                        fqn = request.fqn
                    );
                    self.persist_start(&request).await;
                    let res =
                        Event::handle_activity_async(request.clone(), &self.activity_queue_writer)
                            .await;
                    self.persist_end(request, res.clone()).await;
                    Ok(res?)
                }
            },
            // Non determinism
            (event, false, Some(found)) => Err(HostFunctionError::NonDeterminismDetected(format!(
                "[{workflow_id},{run_id}] Expected {found:?}, got {event:?}"
            ))),
        }
    }
}

pub type EventHistoryTriple = (Arc<FunctionFqn<'static>>, Arc<Vec<Val>>, ActivityResponse);

#[derive(Debug, Default, derive_more::From, derive_more::Into)]
pub struct EventHistory {
    vec: Vec<EventHistoryTriple>,
}
impl EventHistory {
    pub(crate) async fn persist_start(&mut self, _request: &ActivityRequest) {
        // TODO
    }

    pub(crate) async fn persist_end(&mut self, request: ActivityRequest, val: ActivityResponse) {
        self.vec.push((request.fqn, request.params, val));
    }

    pub fn successful_activities(&self) -> usize {
        self.vec.iter().filter(|(_, _, res)| res.is_ok()).count()
    }

    pub(crate) fn len(&self) -> usize {
        self.vec.len()
    }
}

impl AsMut<EventHistory> for EventHistory {
    fn as_mut(&mut self) -> &mut EventHistory {
        self
    }
}
