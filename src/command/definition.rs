use super::handlers::get_timestamp;
use crate::{
    protocol::{Data, RedisArray},
    store::Value,
};

#[derive(Clone, Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: Value,
        expiry: Option<u64>,
        raw_command: String,
    },
    ConfigGet(String),
    Keys(String),
    Info,
    Psync(String, String),
    Replconf,
    ReplconfGetAck(String),
    ReplconfAck(usize),
    Wait {
        num_replicas: i64,
        timeout: u64,
    },
    Type(String),
    Invalid,
}

impl From<Data> for Command {
    fn from(val: Data) -> Self {
        match val {
            Data::Array(arr) => Command::from(arr.0.as_slice()),
            _ => Command::Invalid,
        }
    }
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
                raw_command: get_raw_array_command(val),
            },
            ("SET", [Data::BStr(key), Data::BStr(value)]) => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: None,
                raw_command: get_raw_array_command(val),
            },
            ("CONFIG", [Data::BStr(arg), Data::BStr(key)]) if arg.eq_ignore_ascii_case("GET") => {
                Command::ConfigGet(key.into())
            }
            ("KEYS", [Data::BStr(pattern)]) => Command::Keys(pattern.into()),
            ("INFO", [Data::BStr(section)]) if section.eq_ignore_ascii_case("REPLICATION") => {
                Command::Info
            }
            ("PSYNC", [Data::BStr(replica_id), Data::BStr(offset)]) => {
                Command::Psync(replica_id.into(), offset.into())
            }
            ("REPLCONF", [Data::BStr(subcmd), Data::BStr(arg), ..])
                if subcmd.eq_ignore_ascii_case("GETACK") && arg == "*" =>
            {
                Command::ReplconfGetAck(arg.into())
            }
            ("REPLCONF", [Data::BStr(subcmd), Data::BStr(offset), ..])
                if subcmd.eq_ignore_ascii_case("ACK") =>
            {
                Command::ReplconfAck(offset.parse::<usize>().unwrap())
            }
            ("REPLCONF", [..]) => Command::Replconf,
            ("WAIT", [Data::BStr(num), Data::BStr(timeout)]) => Command::Wait {
                num_replicas: num.parse::<i64>().unwrap(),
                timeout: timeout.parse::<u64>().unwrap(),
            },
            ("TYPE", [Data::BStr(key)]) => Command::Type(key.into()),
            _ => Command::Invalid,
        }
    }
}

fn get_raw_array_command(val: &[Data]) -> String {
    RedisArray(val.to_vec()).into()
}
