use super::response::CommandResponse;
use crate::{
    protocol::Data,
    store::{core::InMemoryStore, stream::StreamQueryResult},
};
use std::ops::Bound::{self, *};

#[derive(Debug)]
pub struct StreamData {
    pub key: String,
    pub entries: Vec<(String, Vec<(String, String)>)>,
}

pub struct StreamFilter {
    pub key: String,
    pub range: (Bound<String>, Bound<String>),
}

pub async fn xread(
    streams: Vec<(String, String)>,
    block: Option<u64>,
    store: &InMemoryStore,
) -> Option<CommandResponse> {
    let key_ranges = streams
        .iter()
        .map(|(key, id)| StreamFilter {
            key: key.clone(),
            range: (Excluded(id.clone()), Unbounded),
        })
        .collect();
    let filtered_streams = match (store.get_filtered_streams(key_ranges).await, block) {
        (
            StreamQueryResult {
                data: Some(data), ..
            },
            None,
            ..,
        ) => Some(data),
        (
            StreamQueryResult {
                data: None,
                max_ids,
            },
            Some(timeout),
        ) => {
            let updated_key = wait_for_new_data(
                streams.iter().map(|f| f.0.clone()).collect(),
                timeout,
                store,
            )
            .await?;
            let mut id = streams
                .iter()
                .find_map(|(key, id)| if key == &updated_key { Some(id) } else { None })
                .cloned()?;
            if id == "$" {
                id = max_ids
                    .into_iter()
                    .find_map(|(key, id)| if key == updated_key { id } else { None })
                    .unwrap_or("0-0".to_string());
            }
            store
                .get_filtered_streams(vec![StreamFilter {
                    key: updated_key,
                    range: (Excluded(id.clone()), Unbounded),
                }])
                .await
                .data
        }
        _ => None,
    }?;
    let arrays = map_xread_response(filtered_streams);
    Some(CommandResponse::Single(String::from(&arrays)))
}

async fn wait_for_new_data(
    keys: Vec<String>,
    timeout_ms: u64,
    store: &InMemoryStore,
) -> Option<String> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let sub_id = store.subscribe(tx).await;
    let future = async {
        while let Some(key) = rx.recv().await {
            if keys.contains(&key) {
                return Some(key);
            }
        }
        None
    };

    let result = if timeout_ms == 0 {
        future.await
    } else {
        tokio::time::timeout(tokio::time::Duration::from_millis(timeout_ms), future)
            .await
            .ok()
            .flatten()
    };
    store.unsubscribe(sub_id).await;
    result
}

pub async fn xrange(
    key: String,
    start: String,
    end: String,
    store: &InMemoryStore,
) -> Option<CommandResponse> {
    let range = match (start.as_str(), end.as_str()) {
        ("-", "+") => (Unbounded, Unbounded),
        ("-", _) => (Unbounded, Included(end)),
        (_, "+") => (Included(start), Unbounded),
        _ => (Included(start), Included(end)),
    };
    let key_ranges = vec![StreamFilter { key, range }];
    let filtered_stream = store.get_filtered_streams(key_ranges).await;
    let stream = filtered_stream.data?.into_iter().next()?;
    let arrays = map_xrange_response(stream.entries);
    Some(CommandResponse::Single(String::from(&arrays)))
}

fn map_xrange_response(entries: Vec<(String, Vec<(String, String)>)>) -> Data {
    Data::Array(
        entries
            .into_iter()
            .map(|(id, entries)| {
                vec![
                    Data::BStr(id.clone()),
                    Data::Array(
                        entries
                            .into_iter()
                            .flat_map(|(k, v)| vec![Data::BStr(k), Data::BStr(v)])
                            .collect::<Vec<_>>(),
                    ),
                ]
            })
            .map(Data::Array)
            .collect::<Vec<_>>(),
    )
}

fn map_xread_response(streams: Vec<StreamData>) -> Data {
    Data::Array(
        streams
            .into_iter()
            .map(|data| {
                Data::Array(vec![
                    Data::BStr(data.key),
                    map_xrange_response(data.entries),
                ])
            })
            .collect::<Vec<_>>(),
    )
}
