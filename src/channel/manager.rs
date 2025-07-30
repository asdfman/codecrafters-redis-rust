use anyhow::{bail, Result};
use futures::future::join_all;
use hashbrown::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex};
use uuid::Uuid;

use crate::command::response::encode_array_of_bstrings;

type Subscription = (Sender<String>, HashSet<String>);

#[derive(Clone, Default)]
pub struct ChannelManager {
    subscribers: Arc<Mutex<HashMap<Uuid, Subscription>>>,
}

impl ChannelManager {
    pub async fn init(&self, tx: Sender<String>) -> Uuid {
        let id = Uuid::new_v4();
        self.subscribers
            .lock()
            .await
            .insert(id, (tx, HashSet::new()));
        id
    }

    pub async fn unsubscribe(&self, id: Uuid, channels: Vec<String>) -> Result<usize> {
        let mut subscribers = self.subscribers.lock().await;
        if let Some((_, subscribed_channels)) = subscribers.get_mut(&id) {
            subscribed_channels.retain(|c| !channels.contains(c));
            return Ok(subscribed_channels.len());
        }
        bail!("Subscriber with ID {} not found", id);
    }

    pub async fn subscribe(&self, id: Uuid, channel: String) -> Result<usize> {
        let mut subscribers = self.subscribers.lock().await;
        if let Some((_, channels)) = subscribers.get_mut(&id) {
            channels.insert(channel);
            return Ok(channels.len());
        }
        bail!("Subscriber with ID {} not found", id);
    }

    pub async fn quit(&self, id: Uuid) {
        self.subscribers.lock().await.remove(&id);
    }

    pub async fn publish(&self, channel: String, message: String) -> usize {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.retain(|_, (tx, _)| !tx.is_closed());

        let futures = subscribers
            .iter_mut()
            .filter_map(|(_, (tx, channels))| {
                if channels.contains(&channel) {
                    Some(tx.send(get_message(&channel, &message)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let count = futures.len();
        let _ = join_all(futures).await;
        count
    }
}

fn get_message(channel: &str, message: &str) -> String {
    encode_array_of_bstrings(vec!["MESSAGE".into(), channel.into(), message.into()].as_slice())
}
