use anyhow::Result;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};

use crate::command::{definition::Command, response::CommandResponse};

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

    pub async fn count(&self) -> i64 {
        self.channels.lock().await.len() as i64
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
    let stream = TcpStream::connect(format!("{host}:{port}")).await?;
    let mut reader = StreamReader::new(stream, true);

    reader.send("PING").await?;
    reader.expect_response("PONG").await?;

    reader
        .send(&format!("REPLCONF listening-port {listen_port}"))
        .await?;
    reader.expect_response("OK").await?;

    reader.send("REPLCONF capa psync2").await?;
    reader.expect_response("OK").await?;

    reader.send("PSYNC ? -1").await?;
    let _response = reader.receive_sstring().await;

    reader.expect_bytes().await?;

    tokio::spawn(async move {
        reader.reset_processed_bytes();
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
                        .write_stream(
                            crate::command::handlers::replconf_getack(
                                reader.get_processed_bytes()
                                    - reader.get_latest_command_byte_length(),
                            )
                            .as_bytes(),
                        )
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
