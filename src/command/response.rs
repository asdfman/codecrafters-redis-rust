pub enum CommandResponse {
    Single(String),
    Multiple(Vec<String>),
    Stream,
}

#[derive(Clone)]
pub enum ResponseData {
    String(String),
    Bytes(Vec<u8>),
}

impl ResponseData {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            ResponseData::String(data) => data.as_bytes(),
            ResponseData::Bytes(data) => data,
        }
    }
}

impl From<ResponseData> for Vec<u8> {
    fn from(data: ResponseData) -> Self {
        match data {
            ResponseData::String(data) => data.into_bytes(),
            ResponseData::Bytes(data) => data,
        }
    }
}
