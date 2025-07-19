use crate::store::Value;

pub const CRLF: &str = "\r\n";
pub const CRLF_LEN: usize = 2;

#[derive(Debug)]
pub enum Data {
    BStr(String),
    SStr(String),
    Array(RedisArray),
}
impl Data {
    pub fn deserialize(val: &str) -> (Self, usize) {
        match val {
            _ if val.starts_with("$") => parse_bulk_string(val),
            _ if val.starts_with("+") => parse_simple_string(val),
            _ => panic!("Unsupported data type"),
        }
    }
}
impl From<&Data> for String {
    fn from(data: &Data) -> Self {
        match data {
            Data::BStr(s) => format!("${}\r\n{}\r\n", s.len(), s),
            _ => panic!("Unsupported data type for conversion to string"),
        }
    }
}

impl From<Value> for Data {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => Data::BStr(s),
            _ => panic!("Unsupported conversion"),
        }
    }
}

#[derive(Debug)]
pub struct RedisArray(pub Vec<Data>);
impl From<&str> for RedisArray {
    fn from(val: &str) -> Self {
        let (arr, _) = RedisArray::extract_array_from_str(val);
        arr
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

impl RedisArray {
    pub fn extract_array_from_str(val: &str) -> (RedisArray, &str) {
        let (data_len, data_start) = get_len(val);
        let mut remaining = &val[data_start..];
        let mut items = Vec::new();
        for _ in 0..data_len {
            let (item, next) = Data::deserialize(remaining);
            items.push(item);
            remaining = &remaining[next..];
        }
        (RedisArray(items), remaining)
    }

    pub fn extract_all(val: &str) -> Vec<RedisArray> {
        let mut remaining = val;
        let mut arrays = vec![];
        while !val.is_empty() {
            if val.starts_with("*") {
                return arrays;
            }
            let (arr, next) = Self::extract_array_from_str(remaining);
            arrays.push(arr);
            remaining = next;
        }
        arrays
    }
}

pub fn split_redis_array_string(val: &str) -> Vec<String> {
    val.split("*")
        .filter_map(|s| {
            if s.is_empty() {
                None
            } else {
                Some(format!("*{s}"))
            }
        })
        .collect()
}

pub fn get_len(val: &str) -> (usize, usize) {
    let len_str = &val[1..val.find(CRLF).unwrap()];
    (len_str.parse().unwrap(), len_str.len() + CRLF_LEN + 1)
}

fn parse_bulk_string(val: &str) -> (Data, usize) {
    let (data_len, data_start) = get_len(val);
    let data_end = data_start + data_len;
    (
        Data::BStr(val[data_start..data_end].to_string()),
        data_end + CRLF_LEN,
    )
}

fn parse_simple_string(val: &str) -> (Data, usize) {
    let data_end = val.find(CRLF).unwrap();
    (
        Data::SStr(val[1..data_end].to_string()),
        data_end + CRLF_LEN,
    )
}
