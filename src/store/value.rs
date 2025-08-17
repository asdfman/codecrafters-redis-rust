use std::collections::BTreeMap;

use crate::rdb::rdb_file::RdbValue;

use super::sorted_set::SortedSet;

pub enum Value {
    String(String),
    Integer(i64),
    List(Vec<String>),
    Stream(BTreeMap<String, Vec<(String, String)>>),
    SortedSet(SortedSet),
}

impl Clone for Value {
    fn clone(&self) -> Self {
        match self {
            Value::String(s) => Value::String(s.clone()),
            Value::Integer(i) => Value::Integer(*i),
            Value::List(l) => Value::List(l.clone()),
            Value::Stream(s) => Value::Stream(s.clone()),
            _ => panic!("Clone not implemented"),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

#[derive(Clone)]
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
