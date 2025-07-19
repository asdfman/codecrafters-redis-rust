use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

use crate::{
    protocol::{Data, RedisArray},
    rdb::util::get_empty_rdb_file_bytes,
    server::{context::null, state::ServerState},
    store::{InMemoryStore, Value},
};

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

pub async fn psync(tx: &Sender<Vec<u8>>, state: &ServerState) -> Result<()> {
    let master_replid = state
        .get_key("replication", "master_replid")
        .expect("Failed to get master replication id");
    let master_repl_offset = state
        .get_key("replication", "master_repl_offset")
        .expect("Failed to get master replication offset");
    let (header, bytes) = psync_response();
    let response = encode_sstring(&format!("FULLRESYNC {master_replid} {master_repl_offset}"));
    tx.send(response.into()).await?;
    tx.send(header.into()).await?;
    tx.send(bytes).await?;
    Ok(())
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

pub fn decode_sstring(val: &str) -> String {
    val.trim_start_matches('+')
        .trim_end_matches("\r\n")
        .to_string()
}

pub fn replconf_getack(bytes: usize) -> String {
    RedisArray(vec![
        Data::BStr("REPLCONF".into()),
        Data::BStr("ACK".into()),
        Data::BStr(format!("{bytes}")),
    ])
    .into()
}

fn psync_response() -> (String, Vec<u8>) {
    let bytes = get_empty_rdb_file_bytes();
    (format!("${}\r\n", bytes.len()), bytes)
}
