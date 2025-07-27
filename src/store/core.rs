use super::value::{Value, ValueWrapper};
use crate::{rdb::rdb_file::RdbFile, server::config::get_config_value};
use bytes::Bytes;
use hashbrown::HashMap;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc::Sender, Mutex};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct InMemoryStore {
    pub data: Arc<Mutex<HashMap<String, ValueWrapper>>>,
    pub subscribers: Arc<Mutex<HashMap<Uuid, Sender<String>>>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        InMemoryStore::init_from_file().unwrap_or(Self::new(HashMap::new()))
    }
}

impl InMemoryStore {
    fn new(data: HashMap<String, ValueWrapper>) -> Self {
        Self {
            data: Arc::new(Mutex::new(data)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn init_from_file() -> Option<Self> {
        let (dir, file_name) = (get_config_value("dir")?, get_config_value("dbfilename")?);
        let mut bytes = read_file(PathBuf::from(dir).join(file_name))?;
        let data = RdbFile::try_from(&mut bytes).ok()?;
        Some(Self::from_rdb_file(data))
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let mut data = self.data.lock().await;
        match data.get(key) {
            Some(wrapper) => match wrapper.expiry {
                Some(timestamp) if !is_expired(timestamp) => Some(wrapper.value.clone()),
                Some(_) => {
                    data.remove(key);
                    None
                }
                None => Some(wrapper.value.clone()),
            },
            None => None,
        }
    }

    pub async fn set(&self, key: String, value: Value, expiry: Option<u64>) {
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

    pub async fn incr(&self, key: String) -> i64 {
        let mut data = self.data.lock().await;
        let value = data.entry(key).or_insert_with(|| ValueWrapper {
            value: Value::String("0".to_string()),
            expiry: None,
        });

        if let Value::String(ref mut current) = &mut value.value {
            if let Ok(current_value) = current.parse::<i64>() {
                let new_value = current_value + 1;
                *current = new_value.to_string();
                return new_value;
            }
        }
        0
    }

    fn from_rdb_file(data: RdbFile) -> Self {
        let map: HashMap<String, ValueWrapper> = data
            .sections
            .into_iter()
            .flat_map(|x| {
                x.data.into_iter().map(|(k, (v, exp))| {
                    (
                        k,
                        ValueWrapper {
                            value: Value::from(v),
                            expiry: exp,
                        },
                    )
                })
            })
            .collect();
        InMemoryStore::new(map)
    }
}

fn read_file(path: PathBuf) -> Option<Bytes> {
    std::fs::read(path).ok().map(Bytes::from)
}

fn is_expired(expiry_timestamp: u64) -> bool {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        >= expiry_timestamp
}
