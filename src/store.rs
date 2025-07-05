use bytes::Bytes;
use hashbrown::HashMap;
use std::{path::PathBuf, sync::Arc};
use tokio::{sync::Mutex, time::Instant};

use crate::{config::get_config_value, rdb::rdb_file::RdbFile};

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
    pub async fn init_from_file() -> Option<Self> {
        let (dir, file_name) = (get_config_value("dir")?, get_config_value("dbfilename")?);
        let mut bytes = read_file(PathBuf::from(dir).join(file_name)).await?;
        let data = RdbFile::try_from(&mut bytes).ok()?;
        Some(Self::from_rdb_file(data))
    }

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

    pub async fn get_keys(&self, pattern: &str) -> Vec<String> {
        let data = self.data.lock().await;
        let pattern = pattern.trim_matches('*');
        data.keys()
            .filter(|key| pattern.is_empty() || key.starts_with(pattern))
            .map(String::from)
            .collect()
    }
    fn from_rdb_file(data: RdbFile) -> Self {
        let map: HashMap<String, ValueWrapper> = data
            .sections
            .into_iter()
            .flat_map(|x| {
                x.data.into_iter().map(|(k, v)| {
                    (
                        k,
                        ValueWrapper {
                            value: Value::from(v),
                            expiry: None,
                        },
                    )
                })
            })
            .collect();
        Self {
            data: Arc::new(Mutex::new(map)),
        }
    }
}

async fn read_file(path: PathBuf) -> Option<Bytes> {
    tokio::fs::read(path).await.ok().map(Bytes::from)
}
