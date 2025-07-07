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

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}

#[derive(Default)]
pub struct ServerContext {
    store: InMemoryStore,
    state: ServerState,
}

impl ServerContext {
    pub fn new(store: InMemoryStore, state: ServerState) -> Self {
        Self { store, state }
    }

    pub async fn execute_command(&self, command: &Command) -> CommandResponse {
        match command {
            Command::Ping => bstring_response("PONG"),
            Command::Echo(val) => bstring_response(val),
            Command::Get(key) => CommandResponse::Single(handlers::get(key, &self.store).await),
            Command::Set { key, value, expiry } => {
                self.store
                    .set(key.to_string(), value.clone(), *expiry)
                    .await;
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
            Command::Psync(..) => handlers::psync(&self.state),
            Command::Replconf => sstring_response("OK"),
            Command::Invalid => null_response(),
        }
    }

    pub async fn process_request(&self, request: String) -> CommandResponse {
        let request = RedisArray::from(request.as_str());
        let command = Command::from(request.0.as_slice());
        self.execute_command(&command).await
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
