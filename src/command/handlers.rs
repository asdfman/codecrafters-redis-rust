use crate::{
    protocol::{Data, RedisArray},
    rdb::util::get_empty_rdb_file_bytes,
    server::{context::null, replica::ReplicaManager, state::ServerState},
    store::{store::InMemoryStore, value::Value},
};
use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;

use super::response::CommandResponse;

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

pub async fn wait(replicas: &mut ReplicaManager, min_num_acks: i64, timeout_ms: u64) -> i64 {
    replicas
        .wait(min_num_acks, std::time::Duration::from_millis(timeout_ms))
        .await
        .expect("Failed to wait for replicas")
}

pub async fn type_handler(key: &str, store: &InMemoryStore) -> String {
    match store.get(key).await {
        Some(value) => match value {
            Value::String(_) => "string".to_string(),
            Value::List(_) => "list".to_string(),
            Value::Stream { .. } => "stream".to_string(),
            _ => panic!("Unexpected value type"),
        },
        None => "none".to_string(),
    }
}

pub async fn xadd(
    key: String,
    id: String,
    entry: (String, String),
    store: &mut InMemoryStore,
) -> Result<String> {
    store.add_stream(key, id.clone(), entry).await
}

pub async fn xrange(
    key: String,
    start: String,
    end: String,
    store: &InMemoryStore,
) -> Result<CommandResponse> {
    let stream = store.get_stream(&key).await?;
    let arrays = &Data::Array(
        stream
            .range(start..=end)
            .map(|(id, entries)| {
                vec![
                    Data::BStr(id.clone()),
                    Data::Array(
                        entries
                            .iter()
                            .flat_map(|(k, v)| vec![Data::BStr(k.clone()), Data::BStr(v.clone())])
                            .collect::<Vec<_>>(),
                    ),
                ]
            })
            .map(Data::Array)
            .collect::<Vec<_>>(),
    );
    Ok(CommandResponse::Single(arrays.into()))
}

pub fn encode_bstring(val: &str) -> String {
    String::from(&Data::BStr(val.to_string()))
}

pub fn encode_sstring(val: &str) -> String {
    String::from(&Data::SStr(val.to_string()))
}

pub fn encode_int(val: i64) -> String {
    String::from(&Data::Int(val))
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
        Data::BStr(bytes.to_string()),
    ])
    .into()
}

fn psync_response() -> (String, Vec<u8>) {
    let bytes = get_empty_rdb_file_bytes();
    (format!("${}\r\n", bytes.len()), bytes)
}
