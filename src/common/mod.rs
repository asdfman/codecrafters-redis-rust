use crate::protocol::Data;

const NULL: &str = "$-1\r\n";
pub fn null() -> String {
    NULL.to_string()
}

pub fn encode_bstring(val: &str) -> String {
    format!("${}\r\n{val}\r\n", val.len())
}

pub fn encode_sstring(val: &str) -> String {
    format!("+{val}\r\n")
}

pub fn encode_int(val: i64) -> String {
    format!(":{}{}\r\n", if val < 0 { "-" } else { "+" }, val)
}

pub fn encode_error(val: &str) -> String {
    format!("-ERR {val}\r\n")
}

pub fn parse_string_args(val: &[Data]) -> Vec<String> {
    val.iter()
        .filter_map(|x| {
            if let Data::BStr(s) = x {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect()
}
