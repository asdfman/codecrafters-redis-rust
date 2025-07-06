use hashbrown::HashMap;
use rand::distr::{Alphanumeric, SampleString};

use super::config::get_config_value;

#[derive(Debug)]
pub struct ServerState {
    sections: HashMap<String, HashMap<String, String>>,
}

impl ServerState {
    pub fn set(&mut self, section: &str, key: &str, value: &str) {
        self.sections
            .entry(section.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
    }

    pub fn get_key(&self, section: &str, key: &str) -> Option<&String> {
        self.sections.get(section).and_then(|s| s.get(key))
    }

    pub fn get_section(&self, section: &str) -> Option<&HashMap<String, String>> {
        self.sections.get(section)
    }

    pub fn get_all_sections(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.sections
    }
}

impl Default for ServerState {
    fn default() -> Self {
        let mut state = Self {
            sections: HashMap::new(),
        };
        match get_config_value("replicaof") {
            Some(_) => state.set("replication", "role", "slave"),
            None => {
                state.set("replication", "role", "master");
                state.set(
                    "replication",
                    "master_replid",
                    gen_replication_id().as_str(),
                );
                state.set("replication", "master_repl_offset", "0");
            }
        }
        state
    }
}

fn gen_replication_id() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 40)
}
