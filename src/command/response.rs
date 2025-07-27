pub enum CommandResponse {
    Single(String),
    Multiple(Vec<String>),
    Stream,
    ReplconfAck,
}

impl From<CommandResponse> for String {
    fn from(response: CommandResponse) -> Self {
        match response {
            CommandResponse::Single(data) => data,
            CommandResponse::Multiple(data) => encode_resp_array(&data),
            CommandResponse::Stream => "STREAM".to_string(),
            CommandResponse::ReplconfAck => "REPLCONF ACK".to_string(),
        }
    }
}

fn encode_resp_array(items: &[String]) -> String {
    let mut result = String::new();
    result.push_str(&format!("*{}\r\n", items.len()));
    for item in items {
        result.push_str(item);
    }
    result
}
