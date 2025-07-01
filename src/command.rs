use bytes::Bytes;

enum Command {
    Ping,
}

impl TryFrom<Bytes> for Command {
    type Error = anyhow::Error;
    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let command = String::from_utf8(bytes.to_vec())?;
        match command.lines().next() {
            _ => Ok(Command::Ping),
        }
    }
}
