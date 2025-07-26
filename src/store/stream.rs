use super::{
    store::InMemoryStore,
    value::{Value, ValueWrapper},
};
use anyhow::{bail, Result};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

enum StreamId {
    Explicit(u64, u64),
    TimeOnly(u64),
    Generate,
}
impl From<&str> for StreamId {
    fn from(s: &str) -> Self {
        let parts: Vec<_> = s.split('-').collect();
        match parts.as_slice() {
            ["*"] => Self::Generate,
            [val, "*"] => Self::TimeOnly(val.parse().expect("Invalid timestamp")),
            [ms, seq] => Self::Explicit(
                ms.parse().expect("Invalid millisecond value"),
                seq.parse().expect("Invalid sequence value"),
            ),
            _ => panic!("Invalid StreamId format"),
        }
    }
}

impl InMemoryStore {
    pub async fn add_stream(
        &mut self,
        key: String,
        stream_id: String,
        stream_entry: (String, String),
    ) -> Result<String> {
        let mut data = self.data.lock().await;
        let entry = data.entry(key.clone()).or_insert(ValueWrapper {
            value: Value::Stream(BTreeMap::new()),
            expiry: None,
        });
        let Value::Stream(stream) = &mut entry.value else {
            panic!("Expected a stream value");
        };
        let stream_id = get_stream_id(
            StreamId::from(stream_id.as_str()),
            stream.keys().next_back().map(String::as_str),
        )?;
        stream
            .entry(stream_id.clone())
            .or_insert(vec![])
            .push(stream_entry);
        self.notifier.send(key).unwrap_or(0);
        Ok(stream_id)
    }

    pub async fn get_stream(&self, key: &str) -> Result<BTreeMap<String, Vec<(String, String)>>> {
        if let Some(Value::Stream(stream)) = self.data.lock().await.get(key).map(|v| &v.value) {
            Ok(stream.clone())
        } else {
            bail!("Key not found or not a stream");
        }
    }
}

fn get_stream_id(incoming: StreamId, last: Option<&str>) -> Result<String> {
    let (last_ms, last_seq) =
        if let Some(StreamId::Explicit(last_ms, last_seq)) = last.map(StreamId::from) {
            (last_ms, last_seq)
        } else {
            (0, 0)
        };
    match incoming {
        StreamId::Explicit(ms, seq) if ms == 0 && seq == 0 => {
            bail!(ERR_INVALID)
        }
        StreamId::Explicit(ms, seq) => {
            if ms < last_ms || (ms == last_ms && seq <= last_seq) {
                bail!(ERR_SMALL)
            }
            Ok(format!("{ms}-{seq}"))
        }
        StreamId::TimeOnly(ms) => {
            if ms < last_ms {
                bail!(ERR_SMALL)
            }
            if ms == last_ms {
                return Ok(format!("{ms}-{}", last_seq + 1));
            }
            Ok(format!("{ms}-0"))
        }
        StreamId::Generate => {
            let ms = get_unix_ms();
            Ok(format!("{ms}-0"))
        }
    }
}

fn get_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

const ERR_SMALL: &str =
    "The ID specified in XADD is equal or smaller than the target stream top item";
const ERR_INVALID: &str = "The ID specified in XADD must be greater than 0-0";
