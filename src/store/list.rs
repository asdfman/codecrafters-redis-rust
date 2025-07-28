use super::{
    core::InMemoryStore,
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
            Ok(list.len())
        } else {
            bail!("Key is not a list");
        }
    }

    pub async fn list_range(&self, key: String, mut start: isize, mut end: isize) -> Vec<String> {
        if let Some(ValueWrapper {
            value: Value::List(list),
            ..
        }) = self.data.lock().await.get(&key)
        {
            let len = list.len() as isize;
            if start < 0 {
                start += len;
            }
            if end < 0 {
                end += len;
            }
            start = start.clamp(0, len - 1);
            end = end.clamp(0, len - 1);
            if start > end {
                return vec![];
            }
            list[start as usize..=end as usize].to_vec()
        } else {
            vec![]
        }
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

    pub async fn lpop(&self, key: String, count: usize) -> Option<Vec<String>> {
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
}
