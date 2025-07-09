use super::handlers::get_timestamp;
use crate::{protocol::Data, store::Value};

#[derive(Clone)]
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
    Psync(String, String),
    Replconf,
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
            ("PSYNC", [Data::BStr(replica_id), Data::BStr(offset)]) => {
                Command::Psync(replica_id.into(), offset.into())
            }
            ("REPLCONF", [..]) => Command::Replconf,
            _ => Command::Invalid,
        }
    }
}
