use std::time::Duration;

use tokio::time::Instant;

use crate::{
    protocol::{Data, RedisArray},
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

pub fn get_instant(duration_ms: &str) -> Option<Instant> {
    duration_ms
        .parse::<u64>()
        .ok()
        .map(|x| Instant::now() + Duration::from_millis(x))
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
