use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::protocol::{Data, RedisArray};

pub struct SendCommand {
    stream: TcpStream,
    buffer: [u8; 1024],
}

impl SendCommand {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: [0; 1024],
        }
    }

    pub async fn send(&mut self, command: &str) -> Result<String> {
        self.stream
            .write_all(encode_command(command).as_bytes())
            .await?;
        let len = self.stream.read(&mut self.buffer).await?;
        Ok(String::from_utf8_lossy(&self.buffer[..len]).to_string())
    }
}

fn encode_command(cmd: &str) -> String {
    RedisArray(
        cmd.split_whitespace()
            .map(|x| Data::BStr(x.into()))
            .collect::<Vec<Data>>(),
    )
    .into()
}
