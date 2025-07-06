use hashbrown::HashMap;

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
        Self {
            sections: HashMap::new(),
        }
    }
}
