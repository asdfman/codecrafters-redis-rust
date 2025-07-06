use super::handlers::{self, get_timestamp};
use crate::{
    protocol::{Data, RedisArray},
    server::{config, state::ServerState},
    store::{InMemoryStore, Value},
};

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}

pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: Value,
        expiry: Option<u64>,
    },
    ConfigGet(String),
    Keys(String),
    Info,
    Invalid,
}

impl From<&[Data]> for Command {
    fn from(val: &[Data]) -> Self {
        let Some(Data::BStr(command)) = val.first() else {
            return Command::Invalid;
        };
        match (command.to_uppercase().as_str(), &val[1..]) {
            ("PING", []) => Command::Ping,
            ("ECHO", [Data::BStr(value)]) => Command::Echo(value.into()),
            ("GET", [Data::BStr(key)]) => Command::Get(key.into()),
            (
                "SET",
                [Data::BStr(key), Data::BStr(value), Data::BStr(param), Data::BStr(expiry_ms)],
            ) if param.eq_ignore_ascii_case("PX") => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: get_timestamp(expiry_ms),
            },
            ("SET", [Data::BStr(key), Data::BStr(value)]) => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: None,
            },
            ("CONFIG", [Data::BStr(arg), Data::BStr(key)]) if arg.eq_ignore_ascii_case("GET") => {
                Command::ConfigGet(key.into())
            }
            ("KEYS", [Data::BStr(pattern)]) => Command::Keys(pattern.into()),
            ("INFO", [Data::BStr(section)]) if section.eq_ignore_ascii_case("REPLICATION") => {
                Command::Info
            }
            _ => Command::Invalid,
        }
    }
}

impl Command {
    pub async fn execute(&self, store: &InMemoryStore, state: &ServerState) -> String {
        match self {
            Command::Ping => encode("PONG"),
            Command::Echo(val) => encode(val),
            Command::Get(key) => handlers::get(key, store).await,
            Command::Set { key, value, expiry } => {
                store.set(key.to_string(), value.clone(), *expiry).await;
                encode("OK")
            }
            Command::ConfigGet(key) => config::get_config_value(key)
                .map(|x| String::from(RedisArray(vec![Data::BStr(key.into()), Data::BStr(x)])))
                .unwrap_or(null()),
            Command::Keys(pattern) => handlers::keys(pattern, store).await,
            Command::Info => handlers::info(state),
            Command::Invalid => null(),
        }
    }
}

pub fn encode(val: &str) -> String {
    String::from(&Data::BStr(val.to_string()))
}
