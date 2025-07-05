use hashbrown::HashMap;
use once_cell::sync::Lazy;

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

pub fn get_config_value(key: &str) -> Option<String> {
    CONFIG.get(key).cloned()
}
