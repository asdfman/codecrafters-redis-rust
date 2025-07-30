use anyhow::Result;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    command::response::{encode_array_of_bstrings, encode_resp_array},
    common::{encode_bstring, encode_error, encode_int},
};

use super::{ChannelCommand, ChannelManager};

pub struct SubscriptionContext {
    rx: mpsc::Receiver<String>,
    subscription_id: Uuid,
    manager: ChannelManager,
}

impl SubscriptionContext {
    pub async fn new(manager: ChannelManager) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            subscription_id: manager.init(tx).await,
            manager,
            rx,
        }
    }

    pub async fn receive_publish(&mut self) -> Option<String> {
        self.rx.recv().await
    }

    pub async fn process_command(&self, command: ChannelCommand) -> Result<String> {
        match command {
            ChannelCommand::Subscribe(channel) => self.subscribe(channel).await,
            ChannelCommand::Unsubscribe(channels) => self.unsubscribe(channels).await,
            ChannelCommand::Publish(channel, message) => self.publish(channel, message).await,
            ChannelCommand::Ping => Ok(encode_array_of_bstrings(&["pong".into(), "".to_string()])),
            ChannelCommand::Invalid(command) => {
                Ok(encode_error(format!("Can't execute '{command}'").as_str()))
            }
        }
    }

    async fn unsubscribe(&self, channels: Vec<String>) -> Result<String> {
        let len = self
            .manager
            .unsubscribe(self.subscription_id, channels.clone())
            .await?;

        Ok(encode_resp_array(
            vec![
                encode_bstring("unsubscribe"),
                encode_bstring(channels.first().unwrap()),
                encode_int(len as i64),
            ]
            .as_slice(),
        ))
    }

    async fn publish(&self, channel: String, message: String) -> Result<String> {
        let count = self.manager.publish(channel, message).await;
        Ok(encode_int(count as i64))
    }

    async fn subscribe(&self, channel: String) -> Result<String> {
        let count = self
            .manager
            .subscribe(self.subscription_id, channel.clone())
            .await?;

        Ok(encode_resp_array(
            vec![
                encode_bstring("subscribe"),
                encode_bstring(&channel),
                encode_int(count as i64),
            ]
            .as_slice(),
        ))
    }
}
