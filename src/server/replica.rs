use anyhow::Result;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};

use crate::command::{
    definition::Command, handlers::replconf_getack, response::CommandResponse, send::SendCommand,
};

use super::stream_reader::StreamReader;

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

pub async fn init_replica(
    replica_config: &str,
    listen_port: &str,
    tx: mpsc::Sender<(Command, Option<oneshot::Sender<CommandResponse>>)>,
) -> Result<()> {
    let (host, port) = replica_config
        .split_once(char::is_whitespace)
        .expect("Invalid master host/port");
    let mut stream = TcpStream::connect(format!("{host}:{port}")).await?;
    let mut sender = SendCommand::new(&mut stream);

    sender.send("PING").await?;
    sender.expect_response("PONG").await?;

    sender
        .send(&format!("REPLCONF listening-port {listen_port}"))
        .await?;
    sender.expect_response("OK").await?;

    sender.send("REPLCONF capa psync2").await?;
    sender.expect_response("OK").await?;

    sender.send("PSYNC ? -1").await?;
    let _response = sender.receive_sstring().await;

    let mut reader = StreamReader::new(stream, true);
    reader.receive_bytes().await?;

    tokio::spawn(async move {
        loop {
            let Ok(data) = reader.read_command().await else {
                break;
            };
            let command = Command::from(data);
            if let Command::ReplconfGetAck(_) = &command {
                let (result_tx, result_rx) = oneshot::channel();
                if tx.send((command, Some(result_tx))).await.is_err() {
                    eprintln!("Failed to send request to event loop.");
                }
                let response = result_rx.await;
                if let Ok(CommandResponse::ReplconfAck) = response {
                    if reader
                        .write_stream(replconf_getack(reader.get_processed_bytes()).as_bytes())
                        .await
                        .is_err()
                    {
                        eprintln!("Failed to send REPLCONF ACK response.");
                    }
                }
            } else if tx.send((command, None)).await.is_err() {
                eprintln!("Failed to send request to event loop.");
            }
        }
    });

    Ok(())
}
