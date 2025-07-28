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
}
