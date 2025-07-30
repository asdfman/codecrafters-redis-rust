use crate::{common::parse_string_args, protocol::Data};

pub enum ChannelCommand {
    Subscribe(String),
    Unsubscribe(Vec<String>),
    Ping,
    Publish(String, String),
    Invalid,
}

impl From<Data> for ChannelCommand {
    fn from(val: Data) -> Self {
        match val {
            Data::Array(arr) => arr.as_slice().into(),
            _ => Self::Invalid,
        }
    }
}

impl From<&[Data]> for ChannelCommand {
    fn from(val: &[Data]) -> Self {
        let Some(Data::BStr(command)) = val.first() else {
            return Self::Invalid;
        };
        match (command.to_uppercase().as_str(), &val[1..]) {
            ("SUBSCRIBE", [Data::BStr(channel)]) => Self::Subscribe(channel.into()),
            ("UNSUBSCRIBE", ..) => Self::Unsubscribe(parse_string_args(val)),
            ("PING", []) => Self::Ping,
            ("PUBLISH", [Data::BStr(channel), Data::BStr(message)]) => {
                Self::Publish(channel.to_string(), message.to_string())
            }
            _ => Self::Invalid,
        }
    }
}
