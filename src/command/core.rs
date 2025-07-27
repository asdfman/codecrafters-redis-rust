use super::handlers::get_timestamp;
use crate::{
    protocol::{Data, RedisArray},
    store::value::Value,
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
    XAdd {
        key: String,
        id: String,
        entry: (String, String),
    },
    XRange {
        key: String,
        start: String,
        end: String,
    },
    XRead {
        streams: Vec<(String, String)>,
        block: Option<u64>,
    },
    Incr(String),
    Multi,
    Exec,
    Invalid,
    Discard,
    Transaction(Vec<Command>),
}

impl From<Data> for Command {
    fn from(val: Data) -> Self {
        match val {
            Data::Array(arr) => Command::from(arr.as_slice()),
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
                if subcmd.eq_ignore_ascii_case("ACK") && is_number(offset) =>
            {
                Command::ReplconfAck(offset.parse::<usize>().unwrap())
            }
            ("REPLCONF", [..]) => Command::Replconf,
            ("WAIT", [Data::BStr(num), Data::BStr(timeout)])
                if is_number(num) && is_number(timeout) =>
            {
                Command::Wait {
                    num_replicas: num.parse::<i64>().unwrap(),
                    timeout: timeout.parse::<u64>().unwrap(),
                }
            }
            ("TYPE", [Data::BStr(key)]) => Command::Type(key.into()),
            (
                "XADD",
                [Data::BStr(key), Data::BStr(id), Data::BStr(entry_key), Data::BStr(entry_value)],
            ) => Command::XAdd {
                key: key.into(),
                id: id.into(),
                entry: (entry_key.into(), entry_value.into()),
            },
            ("XRANGE", [Data::BStr(key), Data::BStr(start), Data::BStr(end)]) => Command::XRange {
                key: key.into(),
                start: start.into(),
                end: end.into(),
            },
            ("XREAD", ..) => parse_xread(val),
            ("INCR", [Data::BStr(key)]) => Command::Incr(key.into()),
            ("MULTI", ..) => Command::Multi,
            ("EXEC", ..) => Command::Exec,
            ("DISCARD", ..) => Command::Discard,
            _ => Command::Invalid,
        }
    }
}

impl Command {
    pub fn is_blocking(&self) -> bool {
        matches!(self, Command::XRead { block: Some(_), .. })
    }
}

fn is_number(val: &str) -> bool {
    val.chars().all(|c| c.is_ascii_digit())
}

fn parse_xread(val: &[Data]) -> Command {
    let mut stream_start = 2usize;
    let block = match &val[1..=2] {
        [Data::BStr(arg), Data::BStr(ms)] if arg.eq_ignore_ascii_case("BLOCK") && is_number(ms) => {
            stream_start = 4;
            Some(ms.parse::<u64>().unwrap())
        }
        _ => None,
    };
    let mid = val[stream_start..].len() / 2;
    let (left, right) = val[stream_start..].split_at(mid);
    let streams = left
        .iter()
        .zip(right.iter())
        .filter_map(|(l, r)| match (l, r) {
            (Data::BStr(stream), Data::BStr(id)) => Some((stream.into(), id.into())),
            _ => None,
        })
        .collect();
    Command::XRead { streams, block }
}

fn get_raw_array_command(val: &[Data]) -> String {
    RedisArray(val.to_vec()).into()
}
