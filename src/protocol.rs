const CRLF: &str = "\r\n";
const CRLF_LEN: usize = 2;

#[derive(Debug)]
pub enum RedisDataType {
    BulkString(String),
    Integer(i64),
}
impl RedisDataType {
    fn deserialize(val: &str) -> (Self, usize) {
        let (data_len, data_start) = get_len(val);
        let data_end = data_start + data_len;
        let ret = match val {
            _ if val.starts_with("$") => Self::BulkString(val[data_start..data_end].to_string()),
            _ => panic!("Unsupported data type"),
        };
        (ret, data_end + CRLF_LEN)
    }
}
impl From<&RedisDataType> for String {
    fn from(data: &RedisDataType) -> Self {
        match data {
            RedisDataType::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            _ => panic!("Unsupported data type for conversion to string"),
        }
    }
}

pub struct RedisArray(pub Vec<RedisDataType>);
impl From<&str> for RedisArray {
    fn from(val: &str) -> Self {
        let mut items = Vec::new();
        let (data_len, data_start) = get_len(val);
        let mut remaining = &val[data_start..];
        for _ in 0..data_len {
            let (item, next) = RedisDataType::deserialize(remaining);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let input = "$10\r\nhellohello\r\n";
        let (data, _) = RedisDataType::deserialize(input);
        let RedisDataType::BulkString(s) = data else {
            panic!("Expected BulkString variant");
        };
        assert_eq!(s, "hellohello");
    }

    #[test]
    fn test_array_deserialize() {
        let input = "*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$10\r\nhelloworld\r\n";
        let data = RedisArray::from(input);
        let arr = data.0;
        assert_eq!(arr.len(), 3);
        assert!(matches!(&arr[0], RedisDataType::BulkString(s) if s == "foo"));
        assert!(matches!(&arr[1], RedisDataType::BulkString(s) if s == "bar"));
        assert!(matches!(&arr[2], RedisDataType::BulkString(s) if s == "helloworld"));
    }
}
