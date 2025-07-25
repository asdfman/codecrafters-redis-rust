use std::collections::BTreeMap;

use crate::rdb::rdb_file::RdbValue;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    List(Vec<String>),
    Stream(BTreeMap<String, Vec<(String, String)>>),
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

#[derive(Clone, Debug)]
pub struct ValueWrapper {
    pub value: Value,
    pub expiry: Option<u64>,
}

impl From<RdbValue> for Value {
    fn from(value: RdbValue) -> Self {
        match value {
            RdbValue::String(s) => Value::String(s),
        }
    }
}
