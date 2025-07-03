use hashbrown::HashMap;
use std::sync::Arc;
use tokio::{sync::Mutex, time::Instant};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    List(Vec<String>),
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

#[derive(Clone, Debug)]
pub struct ValueWrapper {
    value: Value,
    expiry: Option<Instant>,
}

#[derive(Clone, Debug)]
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
        let mut data = self.data.lock().await;
        match data.get(key) {
            Some(wrapper) => match wrapper.expiry {
                Some(expiry) if Instant::now() < expiry => Some(wrapper.value.clone()),
                Some(_) => {
                    data.remove(key);
                    None
                }
                None => Some(wrapper.value.clone()),
            },
            None => None,
        }
    }

    pub async fn set(&self, key: String, value: Value, expiry: Option<Instant>) {
        let value = ValueWrapper { value, expiry };
        self.data.lock().await.insert(key, value);
    }
}
