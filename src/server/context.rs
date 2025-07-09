use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::mpsc,
};

use crate::{
    command::{
        definition::Command,
        handlers::{self, encode_bstring, encode_sstring},
        response::CommandResponse,
    },
    protocol::{Data, RedisArray},
    server::{config, state::ServerState},
    store::InMemoryStore,
};

use super::replica::ReplicaManager;

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

    pub async fn execute_command(&self, request: String) -> CommandResponse {
        let command = Command::from(RedisArray::from(request.as_str()).0.as_slice());
        match &command {
            Command::Ping => bstring_response("PONG"),
            Command::Echo(val) => bstring_response(val),
            Command::Get(key) => CommandResponse::Single(handlers::get(key, &self.store).await),
            Command::Set { key, value, expiry } => {
                self.store
                    .set(key.to_string(), value.clone(), *expiry)
                    .await;
                self.replicas.broadcast(request.into()).await;
                sstring_response("OK")
            }
            Command::ConfigGet(key) => config::get_config_value(key)
                .map(|x| {
                    CommandResponse::Single(
                        RedisArray(vec![Data::BStr(key.into()), Data::BStr(x)]).into(),
                    )
                })
                .unwrap_or(null_response()),
            Command::Keys(pattern) => {
                CommandResponse::Single(handlers::keys(pattern, &self.store).await)
            }
            Command::Info => CommandResponse::Single(handlers::info(&self.state)),
            Command::Psync(..) => CommandResponse::Stream,
            Command::Replconf => sstring_response("OK"),
            Command::Invalid => null_response(),
        }
    }

    pub async fn add_replica(&self, mut reader: BufReader<TcpStream>) {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                if let Err(e) = reader.write_all(&data).await {
                    eprintln!("Failed to write response to replica: {e}");
                }
            }
        });
        if handlers::psync(&tx, &self.state).await.is_err() {
            eprintln!("Replica sync failed");
        }
        self.replicas.add_channel(tx).await;
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
