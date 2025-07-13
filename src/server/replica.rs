use anyhow::Result;
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};

use crate::command::{response::CommandResponse, send::SendCommand};

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
    tx: mpsc::Sender<(String, Option<oneshot::Sender<CommandResponse>>)>,
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
    sender.receive_bytes().await?;

    tokio::spawn(async move {
        let mut buffer = [0u8; 2048];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => break,
                Ok(len) => {
                    let request = String::from_utf8_lossy(&buffer[..len]).to_string();
                    println!("Replica received: {:?}", &request);
                    if tx.send((request, None)).await.is_err() {
                        eprintln!("Failed to send request to event loop.");
                    }
                }
                Err(err) => eprintln!("Error reading from replica stream: {err}"),
            }
        }
    });

    Ok(())
}
