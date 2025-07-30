use crate::{common::parse_string_args, protocol::Data};

pub enum ChannelCommand {
    Subscribe(String),
    Unsubscribe(Vec<String>),
    Ping,
    Invalid(String),
}

impl From<Data> for ChannelCommand {
    fn from(val: Data) -> Self {
        match val {
            Data::Array(arr) => arr.as_slice().into(),
            _ => Self::Invalid("Invalid command format".to_string()),
        }
    }
}

impl From<&[Data]> for ChannelCommand {
    fn from(val: &[Data]) -> Self {
        let Some(Data::BStr(command)) = val.first() else {
            return Self::Invalid("No command found".to_string());
        };
        match (command.to_uppercase().as_str(), &val[1..]) {
            ("SUBSCRIBE", [Data::BStr(channel)]) => Self::Subscribe(channel.into()),
            ("UNSUBSCRIBE", ..) => Self::Unsubscribe(parse_string_args(&val[1..])),
            ("PING", []) => Self::Ping,
            (val, ..) => Self::Invalid(val.into()),
        }
    }
}
