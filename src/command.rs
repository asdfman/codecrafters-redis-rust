use crate::protocol::RedisDataType;

pub enum Command {
    Ping,
    Echo,
    Invalid,
}

impl From<&str> for Command {
    fn from(val: &str) -> Self {
        match val {
            "PING" => Command::Ping,
            "ECHO" => Command::Echo, // ECHO will be handled separately
            _ => Command::Invalid,
        }
    }
}

impl Command {
    pub fn execute(&self, args: &[RedisDataType]) -> String {
        match self {
            Command::Ping => encode("PONG"),
            Command::Echo => String::from(&args[0]),
            Command::Invalid => "*1\r\n$-1\r\n".to_string(),
        }
    }
}

fn encode(val: &str) -> String {
    String::from(&RedisDataType::BulkString(val.to_string()))
}
