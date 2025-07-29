use super::{
    core::InMemoryStore,
    value::{Value, ValueWrapper},
};
use crate::command::stream_handlers::{StreamData, StreamFilter};
use anyhow::{bail, Result};
use std::{
    collections::BTreeMap,
    ops::Bound,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct StreamQueryResult {
    pub data: Option<Vec<StreamData>>,
    pub max_ids: Vec<(String, Option<String>)>,
}

enum StreamId {
    Explicit(u64, u64),
    TimeOnly(u64),
    Generate,
}
impl TryFrom<&str> for StreamId {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<Self> {
        let parts: Vec<_> = s.split('-').collect();
        match parts.as_slice() {
            ["*"] => Ok(Self::Generate),
            [val, "*"] => Ok(Self::TimeOnly(val.parse()?)),
            [ms, seq] => Ok(Self::Explicit(ms.parse()?, seq.parse()?)),
            _ => bail!("Invalid StreamId format"),
        }
    }
}

impl InMemoryStore {
    pub async fn add_stream(
        &self,
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
            bail!("Key is not a stream");
        };
        let stream_id = get_stream_id(
            StreamId::try_from(stream_id.as_str())?,
            stream.keys().next_back().map(String::as_str),
        )?;
        stream
            .entry(stream_id.clone())
            .or_insert(vec![])
            .push(stream_entry);
        self.broadcast(&key).await;

        Ok(stream_id)
    }

    pub async fn get_stream(&self, key: &str) -> Result<BTreeMap<String, Vec<(String, String)>>> {
        if let Some(Value::Stream(stream)) = self.data.lock().await.get(key).map(|v| &v.value) {
            Ok(stream.clone())
        } else {
            bail!("Key not found or not a stream");
        }
    }

    pub async fn get_filtered_streams(&self, filters: Vec<StreamFilter>) -> StreamQueryResult {
        let guard = self.data.lock().await;
        let mut streams = vec![];
        let mut max_ids = vec![];
        for StreamFilter { key, range } in filters {
            if let Some(Value::Stream(stream)) = guard.get(&key).map(|v| &v.value) {
                if let (Bound::Excluded(id), _) = &range {
                    if id == "$" {
                        max_ids.push((key, stream.keys().next_back().cloned()));
                        continue;
                    }
                }
                let entries = stream
                    .range(range)
                    .map(|(id, entries)| {
                        (
                            id.clone(),
                            entries
                                .iter()
                                .map(|(field, value)| (field.clone(), value.clone()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>();
                if entries.is_empty() {
                    continue;
                }
                streams.push(StreamData { key, entries });
            }
        }
        StreamQueryResult {
            data: Some(streams).filter(|v| !v.is_empty()),
            max_ids,
        }
    }
}

fn get_stream_id(incoming: StreamId, last: Option<&str>) -> Result<String> {
    let (last_ms, last_seq) = match last.map(StreamId::try_from).transpose()? {
        Some(StreamId::Explicit(ms, seq)) => (ms, seq),
        _ => (0, 0),
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

pub fn get_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

const ERR_SMALL: &str =
    "The ID specified in XADD is equal or smaller than the target stream top item";
const ERR_INVALID: &str = "The ID specified in XADD must be greater than 0-0";
