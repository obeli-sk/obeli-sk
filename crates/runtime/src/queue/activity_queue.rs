use crate::{
    activity::{Activity, ActivityRequest},
    ActivityResponse, FunctionFqn,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

pub type QueueItem = (ActivityRequest, oneshot::Sender<ActivityResponse>);

pub(crate) struct ActivityQueueReceiver {
    pub(crate) receiver: mpsc::Receiver<QueueItem>,
    pub(crate) functions_to_activities: HashMap<Arc<FunctionFqn<'static>>, Arc<Activity>>,
}

impl ActivityQueueReceiver {
    pub(crate) async fn process(&mut self) {
        while let Some((request, resp_tx)) = self.receiver.recv().await {
            let activity = self
                .functions_to_activities
                .get(&request.fqn)
                .unwrap_or_else(|| {
                    panic!(
                        "instance must have checked its imports, yet `{}` is not found",
                        request.fqn
                    )
                });
            let activity_res = activity.run(&request).await;
            if let Err(_err) = resp_tx.send(activity_res) {
                warn!("Not sending back the activity result");
            }
        }
        debug!("ActivityQueueReceiver::process exitting");
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ActivityQueueSender {
    pub(crate) sender: mpsc::Sender<QueueItem>,
}

impl ActivityQueueSender {
    pub async fn push(&self, request: ActivityRequest) -> oneshot::Receiver<ActivityResponse> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        self.sender
            .send((request, resp_sender))
            .await
            .expect("activity queue receiver must be running");
        resp_receiver
    }
}
