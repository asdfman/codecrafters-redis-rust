use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    protocol::{Data, RedisArray},
    rdb::util::get_empty_rdb_file_bytes,
    server::{context::null, state::ServerState},
    store::{InMemoryStore, Value},
};

use super::response::{CommandResponse, ResponseData};

pub async fn keys(pattern: &str, store: &InMemoryStore) -> String {
    let keys = store
        .get_keys(pattern)
        .await
        .into_iter()
        .map(Data::BStr)
        .collect::<Vec<Data>>();
    RedisArray(keys).into()
}

pub fn get_timestamp(duration_ms: &str) -> Option<u64> {
    duration_ms.parse::<u64>().ok().map(|x| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .unwrap()
            .as_millis() as u64
            + x
    })
}

pub async fn get(key: &str, store: &InMemoryStore) -> String {
    match store.get(key).await {
        Some(value) => match value {
            Value::String(s) => encode_bstring(&s),
            _ => panic!("Unexpected value type"),
        },
        None => null(),
    }
}

pub fn psync(state: &ServerState) -> CommandResponse {
    let master_replid = state
        .get_key("replication", "master_replid")
        .expect("Failed to get master replication id");
    let master_repl_offset = state
        .get_key("replication", "master_repl_offset")
        .expect("Failed to get master replication offset");
    let (header, bytes) = psync_response();
    CommandResponse::Multi(vec![
        ResponseData::String(encode_sstring(&format!(
            "FULLRESYNC {master_replid} {master_repl_offset}"
        ))),
        header,
        bytes,
    ])
}

pub fn info(state: &ServerState) -> String {
    state
        .get_section("replication")
        .map(|x| {
            x.iter()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect::<Vec<String>>()
                .join("\r\n")
        })
        .map(|s| encode_bstring(&s))
        .unwrap_or_default()
}

pub fn encode_bstring(val: &str) -> String {
    String::from(&Data::BStr(val.to_string()))
}

pub fn encode_sstring(val: &str) -> String {
    format!("+{val}\r\n")
}

fn psync_response() -> (ResponseData, ResponseData) {
    let bytes = get_empty_rdb_file_bytes();
    (
        ResponseData::String(format!("${}\r\n", bytes.len())),
        ResponseData::Bytes(bytes),
    )
}
