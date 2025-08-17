use crate::common::convert_range_indices;

use super::{
    core::InMemoryStore,
    subscribe::wait_for_new_data,
    value::{Value, ValueWrapper},
};
use anyhow::{bail, Result};

impl InMemoryStore {
    pub async fn list_push(
        &self,
        key: String,
        values: Vec<String>,
        is_left: bool,
    ) -> Result<usize> {
        let mut data = self.data.lock().await;
        let entry = data.entry(key.clone()).or_insert_with(|| ValueWrapper {
            value: Value::List(vec![]),
            expiry: None,
        });
        if let Value::List(ref mut list) = &mut entry.value {
            match is_left {
                true => values.into_iter().for_each(|v| list.insert(0, v)),
                false => values.into_iter().for_each(|v| list.push(v)),
            }
            self.broadcast(&key).await;
            Ok(list.len())
        } else {
            bail!("Key is not a list");
        }
    }

    pub async fn list_range(&self, key: String, start: isize, end: isize) -> Vec<String> {
        if let Some(ValueWrapper {
            value: Value::List(list),
            ..
        }) = self.data.lock().await.get(&key)
        {
            let len = list.len() as isize;
            if let Some((start, end)) = convert_range_indices(start, end, len) {
                return list[start..=end].to_vec();
            }
        }
        vec![]
    }

    pub async fn list_len(&self, key: String) -> usize {
        if let Some(ValueWrapper {
            value: Value::List(list),
            ..
        }) = self.data.lock().await.get(&key)
        {
            list.len()
        } else {
            0
        }
    }

    pub async fn list_pop(&self, key: String, count: usize) -> Option<Vec<String>> {
        let mut data = self.data.lock().await;
        if let Some(ValueWrapper {
            value: Value::List(list),
            ..
        }) = data.get_mut(&key)
        {
            let popped = list.drain(..count.min(list.len())).collect::<Vec<_>>();
            if list.is_empty() {
                data.remove(&key);
            }
            Some(popped)
        } else {
            None
        }
    }
    pub async fn blpop(&self, keys: &[String]) -> Option<Vec<String>> {
        let mut data = self.data.lock().await;
        for key in keys {
            if let Some(ValueWrapper {
                value: Value::List(list),
                ..
            }) = data.get_mut(key)
            {
                let value = list.remove(0);
                if list.is_empty() {
                    data.remove(key);
                }
                return Some(vec![key.clone(), value]);
            }
        }
        None
    }
}

pub async fn blpop_handler(
    store: &InMemoryStore,
    keys: Vec<String>,
    block_ms: u64,
) -> Option<Vec<String>> {
    match store.blpop(&keys).await {
        val if val.is_some() => val,
        _ => {
            let updated_key = wait_for_new_data(&keys, block_ms, store).await?;
            store.blpop(&[updated_key]).await
        }
    }
}
