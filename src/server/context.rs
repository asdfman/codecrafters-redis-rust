use std::sync::Arc;

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::{
    command::{
        core::Command,
        handlers::{self, encode_bstring, encode_int, encode_sstring},
        response::CommandResponse,
        stream_handlers,
    },
    protocol::{Data, RedisArray},
    server::{config, state::ServerState},
    store::core::InMemoryStore,
};

use super::{
    replica::{ReplicaManager, ReplicaState},
    stream_reader::StreamReader,
};

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}

#[derive(Clone, Default)]
pub struct ServerContext {
    pub store: InMemoryStore,
    pub state: Arc<Mutex<ServerState>>,
    pub replicas: Arc<Mutex<ReplicaManager>>,
}

impl ServerContext {
    pub async fn execute_command(&self, request: Command) -> CommandResponse {
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
                self.replicas
                    .lock()
                    .await
                    .broadcast(raw_command.into_bytes())
                    .await;
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
            Command::Info => CommandResponse::Single(handlers::info(self).await),
            Command::Psync(..) => CommandResponse::Stream,
            Command::Replconf => sstring_response("OK"),
            Command::ReplconfGetAck(_) => CommandResponse::ReplconfAck,
            Command::Wait {
                num_replicas,
                timeout,
            } => int_response(handlers::wait(self, num_replicas, timeout).await),
            Command::Type(key) => sstring_response(
                handlers::type_handler(key.as_str(), &self.store)
                    .await
                    .as_ref(),
            ),
            Command::XAdd { key, id, entry } => match self.store.add_stream(key, id, entry).await {
                Ok(res) => bstring_response(&res),
                Err(e) => error_response(&e.to_string()),
            },
            Command::XRange { key, start, end } => {
                stream_handlers::xrange(key, start, end, &self.store)
                    .await
                    .unwrap_or(null_response())
            }
            Command::XRead { streams, block } => {
                stream_handlers::xread(streams, block, &self.store)
                    .await
                    .unwrap_or(null_response())
            }
            Command::Incr(key) => {
                let value = self.store.incr(key.clone()).await;
                match value {
                    0 => error_response("value is not an integer or out of range"),
                    _ => {
                        self.replicas
                            .lock()
                            .await
                            .broadcast(raw_incr_command(key, value as u64).into_bytes())
                            .await;
                        int_response(value)
                    }
                }
            }
            Command::Multi => sstring_response("OK"),
            Command::Exec => error_response("EXEC without multi"),
            Command::Invalid | Command::ReplconfAck(_) | Command::Transaction(_) => null_response(),
        }
    }

    pub async fn process_transaction(&self, commands: Vec<Command>) -> CommandResponse {
        let mut responses = Vec::new();
        for command in commands {
            if let CommandResponse::Single(res) = self.execute_command(command).await {
                responses.push(res);
            }
        }

        CommandResponse::Multiple(responses)
    }

    pub async fn add_replica(&self, reader: StreamReader<TcpStream>) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(100);
        let replica_id = uuid::Uuid::new_v4().to_string();
        let replica_state = Arc::new(Mutex::new(ReplicaState::new(tx.clone())));
        let state_clone = replica_state.clone();
        if handlers::psync(&tx, self).await.is_err() {
            eprintln!("Replica sync failed");
        }
        self.replicas
            .lock()
            .await
            .add_channel(replica_id, replica_state);

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
            if let Command::ReplconfAck(offset) = Command::from(response.as_slice()) {
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

fn error_response(err: &str) -> CommandResponse {
    CommandResponse::Single(String::from(&Data::SimpleError(err.to_string())))
}

fn raw_incr_command(key: String, arg: u64) -> String {
    RedisArray(vec![
        Data::BStr("INCR".into()),
        Data::BStr(key),
        Data::BStr(arg.to_string()),
    ])
    .into()
}
