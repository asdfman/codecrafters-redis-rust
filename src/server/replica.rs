use anyhow::Result;
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::command::send::SendCommand;

#[derive(Default)]
pub struct ReplicaManager {
    channels: Mutex<Vec<mpsc::Sender<Vec<u8>>>>,
}

impl ReplicaManager {
    pub async fn add_channel(&self, channel: mpsc::Sender<Vec<u8>>) {
        let mut channels = self.channels.lock().await;
        channels.push(channel);
    }

    pub async fn broadcast(&self, data: Vec<u8>) {
        let mut channels = self.channels.lock().await;
        channels.retain(|x| !x.is_closed());
        for channel in channels.iter() {
            let _ = channel.send(data.clone()).await;
        }
    }
}

pub async fn init_replica(replica_config: &str, listen_port: &str) -> Result<()> {
    let (host, port) = replica_config
        .split_once(char::is_whitespace)
        .expect("Invalid master host/port");
    let stream = TcpStream::connect(format!("{host}:{port}")).await?;
    let mut sender = SendCommand::new(stream);
    sender.send("PING").await?;
    sender
        .send(&format!("REPLCONF listening-port {listen_port}"))
        .await?;
    sender.send("REPLCONF capa psync2").await?;
    sender.send("PSYNC ? -1").await?;
    Ok(())
}
