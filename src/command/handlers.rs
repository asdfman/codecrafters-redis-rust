use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    protocol::{Data, RedisArray},
    server::state::ServerState,
    store::{InMemoryStore, Value},
};

use super::{encode, null};

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
            Value::String(s) => encode(&s),
            _ => panic!("Unexpected value type"),
        },
        None => null(),
    }
}

pub fn info(state: &ServerState) -> String {
    state
        .get_section("replication")
        .map(|x| x.iter().map(|(k, v)| encode(&format!("{k}:{v}"))))
        .unwrap()
        .collect()
}
