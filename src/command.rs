use crate::{
    protocol::RedisDataType,
    store::{InMemoryStore, Value},
};

pub enum Command {
    Ping,
    Echo,
    Get,
    Set,
    Invalid,
}

impl From<&str> for Command {
    fn from(val: &str) -> Self {
        match val {
            "PING" => Command::Ping,
            "ECHO" => Command::Echo,
            "GET" => Command::Get,
            "SET" => Command::Set,
            _ => Command::Invalid,
        }
    }
}

impl Command {
    pub async fn execute(&self, args: &[RedisDataType], store: &InMemoryStore) -> String {
        match self {
            Command::Ping => encode("PONG"),
            Command::Echo => String::from(&args[0]),
            Command::Get => get(&args[0], store).await,
            Command::Set => set(args, store).await,
            Command::Invalid => null(),
        }
    }
}

async fn get(arg: &RedisDataType, store: &InMemoryStore) -> String {
    let RedisDataType::BulkString(key) = arg else {
        return null();
    };
    match store.get(key).await {
        Some(value) => match value {
            Value::String(s) => encode(&s),
            _ => panic!("Unexpected value type"),
        },
        None => null(),
    }
}

async fn set(args: &[RedisDataType], store: &InMemoryStore) -> String {
    let (RedisDataType::BulkString(key), RedisDataType::BulkString(value)) = (&args[0], &args[1])
    else {
        return null();
    };
    store.set(key.clone(), Value::String(value.clone())).await;
    encode("OK")
}

fn encode(val: &str) -> String {
    String::from(&RedisDataType::BulkString(val.to_string()))
}

const NULL: &str = "$-1\r\n";
fn null() -> String {
    NULL.to_string()
}
