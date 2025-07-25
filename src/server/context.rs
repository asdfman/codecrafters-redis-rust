use std::sync::Arc;

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::{
    command::{
        definition::Command,
        handlers::{self, encode_bstring, encode_int, encode_sstring},
        response::CommandResponse,
    },
    protocol::{Data, RedisArray},
    server::{config, state::ServerState},
    store::InMemoryStore,
};

use super::{
    replica::{ReplicaManager, ReplicaState},
    stream_reader::StreamReader,
};

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}

#[derive(Default)]
pub struct ServerContext {
    store: InMemoryStore,
    state: ServerState,
    replicas: ReplicaManager,
}

impl ServerContext {
    pub fn new(store: InMemoryStore, state: ServerState) -> Self {
        Self {
            store,
            state,
            replicas: ReplicaManager::default(),
        }
    }

    pub async fn execute_command(&mut self, request: Command) -> CommandResponse {
        match request {
            Command::Ping => sstring_response("PONG"),
            Command::Echo(val) => bstring_response(&val),
            Command::Get(key) => CommandResponse::Single(handlers::get(&key, &self.store).await),
            Command::Set {
                key,
                value,
                expiry,
                raw_command,
            } => {
                self.store.set(key.to_string(), value.clone(), expiry).await;
                self.replicas.broadcast(raw_command.into_bytes()).await;
                sstring_response("OK")
            }
            Command::ConfigGet(key) => config::get_config_value(&key)
                .map(|x| {
                    CommandResponse::Single(RedisArray(vec![Data::BStr(key), Data::BStr(x)]).into())
                })
                .unwrap_or(null_response()),
            Command::Keys(pattern) => {
                CommandResponse::Single(handlers::keys(&pattern, &self.store).await)
            }
            Command::Info => CommandResponse::Single(handlers::info(&self.state)),
            Command::Psync(..) => CommandResponse::Stream,
            Command::Replconf => sstring_response("OK"),
            Command::ReplconfGetAck(_) => CommandResponse::ReplconfAck,
            Command::Wait {
                num_replicas,
                timeout,
            } => int_response(handlers::wait(&mut self.replicas, num_replicas, timeout).await),
            Command::Type(key) => sstring_response(
                handlers::type_handler(key.as_str(), &self.store)
                    .await
                    .as_ref(),
            ),
            Command::Invalid | Command::ReplconfAck(_) => null_response(),
        }
    }

    pub async fn add_replica(&mut self, reader: StreamReader<TcpStream>) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(100);
        let replica_id = uuid::Uuid::new_v4().to_string();
        let replica_state = Arc::new(Mutex::new(ReplicaState::new(tx.clone())));
        let state_clone = replica_state.clone();
        if handlers::psync(&tx, &self.state).await.is_err() {
            eprintln!("Replica sync failed");
        }
        self.replicas.add_channel(replica_id, replica_state);

        tokio::spawn(async move {
            replica_stream_handler(reader, rx, state_clone).await;
        });
    }
}

async fn replica_stream_handler(
    mut reader: StreamReader<TcpStream>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    state: Arc<Mutex<ReplicaState>>,
) {
    loop {
        tokio::select! {
            Some(data) = rx.recv() => {
                if let Err(e) = reader.write_stream(&data).await {
                    eprintln!("Failed to write response to replica: {e}");
                    break;
                }
            }
            result = reader.read_redis_data() => {
                match result {
                    Ok(data) => process_response(Ok(data), state.clone()).await,
                    Err(e) => {
                        eprintln!("Failed to read response from replica: {e}");
                        break;
                    }
                }
            }
        }
    }
}

async fn process_response(result: anyhow::Result<Data>, state: Arc<Mutex<ReplicaState>>) {
    match result {
        Ok(Data::Array(response)) => {
            if let Command::ReplconfAck(offset) = Command::from(response.0.as_slice()) {
                state.lock().await.update_latest_offset(offset);
            }
        }
        Ok(data) => eprintln!("Unexpected response received from replica: {data:?}"),
        Err(e) => {
            eprintln!("Failed to read response from replica: {e}");
        }
    }
}

fn bstring_response(val: &str) -> CommandResponse {
    CommandResponse::Single(encode_bstring(val))
}

fn sstring_response(val: &str) -> CommandResponse {
    CommandResponse::Single(encode_sstring(val))
}

fn null_response() -> CommandResponse {
    CommandResponse::Single(null())
}

fn int_response(val: i64) -> CommandResponse {
    CommandResponse::Single(encode_int(val))
}
