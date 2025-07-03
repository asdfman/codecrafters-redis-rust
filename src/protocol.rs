const CRLF: &str = "\r\n";
const CRLF_LEN: usize = 2;

#[derive(Debug)]
pub enum RedisData {
    BulkString(String),
    Integer(i64),
}
impl RedisData {
    pub fn deserialize(val: &str) -> (Self, usize) {
        match val {
            _ if val.starts_with("$") => parse_bulk_string(val),
            _ if val.starts_with(":") => parse_integer(val),
            _ => panic!("Unsupported data type"),
        }
    }
}
impl From<&RedisData> for String {
    fn from(data: &RedisData) -> Self {
        match data {
            RedisData::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            _ => panic!("Unsupported data type for conversion to string"),
        }
    }
}

pub struct RedisArray(pub Vec<RedisData>);
impl From<&str> for RedisArray {
    fn from(val: &str) -> Self {
        let mut items = Vec::new();
        let (data_len, data_start) = get_len(val);
        let mut remaining = &val[data_start..];
        for _ in 0..data_len {
            let (item, next) = RedisData::deserialize(remaining);
            items.push(item);
            remaining = &remaining[next..];
        }
        Self(items)
    }
}
impl From<RedisArray> for String {
    fn from(arr: RedisArray) -> Self {
        let mut result = String::new();
        result.push_str(&format!("*{}\r\n", arr.0.len()));
        for item in arr.0 {
            result.push_str(String::from(&item).as_str());
        }
        result
    }
}

fn get_len(val: &str) -> (usize, usize) {
    let len_str = &val[1..val.find(CRLF).unwrap()];
    (len_str.parse().unwrap(), len_str.len() + CRLF_LEN + 1)
}

fn parse_bulk_string(val: &str) -> (RedisData, usize) {
    let (data_len, data_start) = get_len(val);
    let data_end = data_start + data_len;
    (
        RedisData::BulkString(val[data_start..data_end].to_string()),
        data_end + CRLF_LEN,
    )
}

fn parse_integer(val: &str) -> (RedisData, usize) {
    let data_end = val.find(CRLF).unwrap();
    let int_str = &val[1..data_end];
    (
        RedisData::Integer(int_str.parse().unwrap()),
        data_end + CRLF_LEN,
    )
}
