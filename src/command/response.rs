pub enum CommandResponse {
    Single(String),
    Multiple(Vec<String>),
    Stream,
    ReplconfAck,
}
