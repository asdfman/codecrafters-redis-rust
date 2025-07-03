use crate::{
    protocol::RedisData,
    store::{InMemoryStore, Value},
};
use std::time::Duration;
use tokio::time::Instant;

pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: Value,
        expiry: Option<Instant>,
    },
    ConfigGet(String),
    Invalid,
}

impl From<&[RedisData]> for Command {
    fn from(val: &[RedisData]) -> Self {
        let Some(RedisData::BulkString(command)) = val.first() else {
            return Command::Invalid;
        };
        match (command.to_uppercase().as_str(), &val[1..]) {
            ("PING", []) => Command::Ping,
            ("ECHO", [RedisData::BulkString(value)]) => Command::Echo(value.into()),
            ("GET", [RedisData::BulkString(key)]) => Command::Get(key.into()),
            (
                "SET",
                [RedisData::BulkString(key), RedisData::BulkString(value), RedisData::BulkString(param), RedisData::BulkString(expiry_ms)],
            ) if param.eq_ignore_ascii_case("PX") => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: get_instant(expiry_ms),
            },
            ("SET", [RedisData::BulkString(key), RedisData::BulkString(value), ..]) => {
                Command::Set {
                    key: key.into(),
                    value: value.to_string().into(),
                    expiry: None,
                }
            }
            ("CONFIG", [RedisData::BulkString(arg), RedisData::BulkString(key)])
                if arg.eq_ignore_ascii_case("GET") =>
            {
                Command::ConfigGet(key.into())
            }
            _ => Command::Invalid,
        }
    }
}

fn get_instant(duration_ms: &str) -> Option<Instant> {
    duration_ms
        .parse::<u64>()
        .ok()
        .map(|x| Instant::now() + Duration::from_millis(x))
}

impl Command {
    pub async fn execute(&self, store: &InMemoryStore) -> String {
        match self {
            Command::Ping => encode("PONG"),
            Command::Echo(val) => encode(val),
            Command::Get(key) => get(key, store).await,
            Command::Set { key, value, expiry } => {
                store.set(key.to_string(), value.clone(), *expiry).await;
                encode("OK")
            }
            Command::ConfigGet(key) => crate::config::get_config_value(key),
            Command::Invalid => null(),
        }
    }
}

async fn get(key: &str, store: &InMemoryStore) -> String {
    match store.get(key).await {
        Some(value) => match value {
            Value::String(s) => encode(&s),
            _ => panic!("Unexpected value type"),
        },
        None => null(),
    }
}

fn encode(val: &str) -> String {
    String::from(&RedisData::BulkString(val.to_string()))
}

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}
