use hashbrown::HashMap;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    Integer(i64),
    List(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct ValueWrapper {
    value: Value,
    expiry: Option<Instant>,
}

#[derive(Clone)]
pub struct InMemoryStore {
    data: Arc<Mutex<HashMap<String, ValueWrapper>>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl InMemoryStore {
    pub async fn get(&self, key: &str) -> Option<Value> {
        match self.data.lock().await.get(key) {
            Some(wrapper) => match wrapper.expiry {
                Some(expiry) if Instant::now() < expiry => Some(wrapper.value.clone()),
                Some(_) => {
                    self.data.lock().await.remove(key);
                    None
                }
                None => Some(wrapper.value.clone()),
            },
            None => None,
        }
    }
    pub async fn set(&self, key: String, value: Value) {
        self.data.lock().await.insert(
            key,
            ValueWrapper {
                value,
                expiry: None,
            },
        );
    }
    pub async fn set_ex(&self, key: String, value: Value, duration: Duration) {
        let value = ValueWrapper {
            value,
            expiry: Some(Instant::now() + duration),
        };
        self.data.lock().await.insert(key, value);
    }
}
