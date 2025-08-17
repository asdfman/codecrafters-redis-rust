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

pub fn convert_range_indices(
    mut start: isize,
    mut end: isize,
    len: isize,
) -> Option<(usize, usize)> {
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.clamp(0, len - 1);
    end = end.clamp(0, len - 1);
    if start > end {
        return None;
    }
    Some((start as usize, end as usize))
}
