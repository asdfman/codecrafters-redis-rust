use tokio::sync::oneshot::Sender;
use uuid::Uuid;

use super::{core::InMemoryStore, stream::get_unix_ms};

pub struct Subscription {
    keys: Vec<String>,
    tx: Sender<String>,
    timestamp: u64,
}

impl InMemoryStore {
    pub async fn subscribe(&self, keys: Vec<String>, tx: Sender<String>) -> Uuid {
        let id = Uuid::new_v4();
        let mut subscribers = self.subscribers.lock().await;
        subscribers.insert(
            id,
            Subscription {
                keys,
                tx,
                timestamp: get_unix_ms(),
            },
        );
        id
    }

    pub async fn unsubscribe(&self, id: Uuid) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.remove(&id);
    }

    pub async fn broadcast(&self, updated_key: &str) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.retain(|_, Subscription { tx, .. }| !tx.is_closed());

        let Some(sub_key) = subscribers
            .iter()
            .filter(|(_, sub)| sub.keys.contains(&updated_key.to_string()))
            .min_by_key(|(_, sub)| sub.timestamp)
            .map(|(k, _)| *k)
        else {
            return;
        };
        if let Some(sub) = subscribers.remove(&sub_key) {
            let _ = sub.tx.send(updated_key.to_string());
        }
    }
}

pub async fn wait_for_new_data(
    keys: &[String],
    timeout_ms: u64,
    store: &InMemoryStore,
) -> Option<String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sub_id = store.subscribe(keys.to_vec(), tx).await;
    let future = async { rx.await.ok() };

    let result = if timeout_ms == 0 {
        future.await
    } else {
        tokio::time::timeout(tokio::time::Duration::from_millis(timeout_ms), future)
            .await
            .ok()
            .flatten()
    };
    store.unsubscribe(sub_id).await;
    result
}
