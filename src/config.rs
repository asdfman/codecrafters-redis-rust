use hashbrown::HashMap;
use once_cell::sync::Lazy;

use crate::protocol::{RedisArray, RedisData};

static CONFIG: Lazy<HashMap<String, String>> = Lazy::new(|| {
    let mut args = HashMap::new();
    let mut iter = std::env::args().skip(1);
    while let Some(key) = iter.next() {
        if let Some(value) = iter.next() {
            args.insert(key.trim_start_matches("--").into(), value);
        }
    }
    args
});

pub fn get_config_value(key: &str) -> String {
    let Some(value) = CONFIG.get(key) else {
        return crate::command::null();
    };
    RedisArray(vec![
        RedisData::BulkString(key.into()),
        RedisData::BulkString(value.clone()),
    ])
    .into()
}
