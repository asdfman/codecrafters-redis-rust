pub enum CommandResponse {
    Single(String),
    Multi(Vec<ResponseData>),
}

pub enum ResponseData {
    String(String),
    Bytes(Vec<u8>),
}

impl From<ResponseData> for Vec<u8> {
    fn from(data: ResponseData) -> Self {
        match data {
            ResponseData::String(data) => data.into_bytes(),
            ResponseData::Bytes(data) => data,
        }
    }
}
