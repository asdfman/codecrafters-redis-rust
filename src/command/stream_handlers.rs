use crate::protocol::Data;
use std::ops::Bound;

pub struct OwnedStreamData {
    pub key: String,
    pub entries: Vec<(String, Vec<(String, String)>)>,
}

pub struct StreamFilter {
    pub key: String,
    pub range: (Bound<String>, Bound<String>),
}

pub fn map_xrange_response(entries: Vec<(String, Vec<(String, String)>)>) -> Data {
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

pub fn map_xread_response(streams: Vec<OwnedStreamData>) -> Data {
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
