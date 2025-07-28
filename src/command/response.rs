use crate::{
    common::{encode_bstring, encode_error, encode_int, encode_sstring, null},
    rdb::util::get_empty_rdb_file_bytes,
};

pub enum CommandResponse {
    Single(String),
    Multiple(Vec<String>),
    Stream,
    ReplconfAck,
}

impl From<CommandResponse> for String {
    fn from(response: CommandResponse) -> Self {
        match response {
            CommandResponse::Single(data) => data,
            CommandResponse::Multiple(data) => encode_resp_array(&data),
            _ => null(),
        }
    }
}

pub fn encode_resp_array(items: &[String]) -> String {
    let mut result = String::new();
    result.push_str(&format!("*{}\r\n", items.len()));
    for item in items {
        result.push_str(item);
    }
    result
}

pub fn replconf_getack(bytes: usize) -> String {
    encode_resp_array(&[
        encode_bstring("REPLCONF"),
        encode_bstring("ACK"),
        encode_bstring(&bytes.to_string()),
    ])
}

pub fn psync_response() -> (String, Vec<u8>) {
    let bytes = get_empty_rdb_file_bytes();
    (format!("${}\r\n", bytes.len()), bytes)
}

pub fn bstring_response(val: &str) -> CommandResponse {
    CommandResponse::Single(encode_bstring(val))
}

pub fn sstring_response(val: &str) -> CommandResponse {
    CommandResponse::Single(encode_sstring(val))
}

pub fn null_response() -> CommandResponse {
    CommandResponse::Single(null())
}

pub fn int_response(val: i64) -> CommandResponse {
    CommandResponse::Single(encode_int(val))
}

pub fn error_response(err: &str) -> CommandResponse {
    CommandResponse::Single(encode_error(err))
}

pub fn array_response(items: Vec<String>) -> CommandResponse {
    let bstring_items: Vec<String> = items.into_iter().map(|s| encode_bstring(&s)).collect();
    CommandResponse::Single(encode_resp_array(bstring_items.as_slice()))
}
