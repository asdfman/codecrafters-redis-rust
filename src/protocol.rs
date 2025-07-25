use crate::store::value::Value;

pub const CRLF: &str = "\r\n";
pub const CRLF_LEN: usize = 2;

#[derive(Debug, Clone)]
pub enum Data {
    BStr(String),
    SStr(String),
    Int(i64),
    Array(RedisArray),
    SimpleError(String),
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
            Data::SStr(s) => format!("+{s}\r\n"),
            Data::Int(i) => format!(":{}{}\r\n", if *i < 0 { "-" } else { "+" }, i),
            Data::SimpleError(e) => format!("-ERR {e}\r\n"),
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

#[derive(Debug, Clone)]
pub struct RedisArray(pub Vec<Data>);

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
