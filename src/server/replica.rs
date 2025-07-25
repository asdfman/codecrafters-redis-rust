use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures::future::join_all;
use hashbrown::{HashMap, HashSet};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};

use crate::{
    command::{definition::Command, response::CommandResponse},
    protocol::{Data, RedisArray},
};

use super::stream_reader::StreamReader;

#[derive(Debug)]
pub struct ReplicaState {
    channel: mpsc::Sender<Vec<u8>>,
    expected_offset: usize,
    latest_offset: usize,
}

impl ReplicaState {
    pub fn new(channel: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            channel,
            expected_offset: 0,
            latest_offset: 0,
        }
    }

    pub fn update_latest_offset(&mut self, offset: usize) {
        self.latest_offset = offset;
    }
}

#[derive(Default)]
pub struct ReplicaManager(HashMap<String, Arc<Mutex<ReplicaState>>>);

impl ReplicaManager {
    pub fn add_channel(&mut self, id: String, state: Arc<Mutex<ReplicaState>>) {
        self.0.insert(id, state);
    }

    async fn clean_closed(&mut self) {
        let mut remove = vec![];
        for (k, v) in self.0.iter() {
            let replica = v.lock().await;
            if replica.channel.is_closed() {
                remove.push(k.clone());
            }
        }
        for k in remove.iter() {
            self.0.remove(k);
        }
    }

    pub async fn broadcast(&mut self, data: Vec<u8>) {
        self.clean_closed().await;
        for (_, replica) in self.0.iter_mut() {
            let mut lock = replica.lock().await;
            let _ = lock.channel.send(data.clone()).await;
            lock.expected_offset += data.len();
        }
    }

    pub fn count(&self) -> i64 {
        self.0.len() as i64
    }

    pub async fn wait(
        &mut self,
        min_num_acks: i64,
        timeout_ms: std::time::Duration,
    ) -> Result<i64> {
        let mut channels = Vec::new();
        for (_, replica) in self.0.iter() {
            channels.push(replica.lock().await.channel.clone());
        }

        let futures = {
            channels
                .into_iter()
                .map(|tx| {
                    tokio::spawn(async move {
                        if let Err(e) = send_get_ack(tx).await {
                            eprintln!("Error in replica acks: {e}");
                        }
                    })
                })
                .collect::<Vec<_>>()
        };

        let ack_task = tokio::spawn(async move {
            join_all(futures).await;
        });

        let mut ack_count = 0;
        let start = tokio::time::Instant::now();
        let mut ids = self
            .0
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<HashSet<_>>();

        while ack_count < min_num_acks {
            for id in ids.clone().iter() {
                let Some(replica) = self.0.get(*id) else {
                    bail!("Replica with id {id} not found");
                };
                let replica = replica.lock().await;
                if replica.latest_offset >= replica.expected_offset {
                    ack_count += 1;
                    ids.remove(id);
                }
            }
            if start.elapsed() >= timeout_ms {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        ack_task.abort();

        Ok(ack_count)
    }
}

async fn send_get_ack(tx: mpsc::Sender<Vec<u8>>) -> Result<()> {
    let request: String = RedisArray(vec![
        Data::BStr("REPLCONF".into()),
        Data::BStr("GETACK".into()),
        Data::BStr("*".into()),
    ])
    .into();
    tx.send(request.into_bytes())
        .await
        .context("Failed to send REPLCONF GETACK request")
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
            let Ok(data) = reader.read_redis_data().await else {
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
