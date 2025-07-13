use anyhow::{bail, Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::protocol::{Data, RedisArray};

pub struct SendCommand<'a> {
    stream: &'a mut TcpStream,
    buffer: [u8; 1024],
}

impl<'a> SendCommand<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        Self {
            stream,
            buffer: [0; 1024],
        }
    }

    pub async fn send(&mut self, command: &str) -> Result<()> {
        self.stream
            .write_all(encode_command(command).as_bytes())
            .await
            .context("Failed to send command")
    }

    pub async fn expect_response(&mut self, expected: &str) -> Result<()> {
        let response = self.receive_sstring().await?;
        if !response.eq_ignore_ascii_case(expected) {
            anyhow::bail!("Expected response '{}', but got '{}'", expected, response);
        }
        Ok(())
    }

    pub async fn receive_sstring(&mut self) -> Result<String> {
        loop {
            let len = self.stream.read(&mut self.buffer).await?;
            if len == 0 {
                continue;
            }
            return Ok(decode_sstring(&String::from_utf8_lossy(
                &self.buffer[..len],
            )));
        }
    }

    pub async fn receive_bytes(&mut self) -> Result<Vec<u8>> {
        let mut len_bytes = vec![];
        loop {
            let mut byte = [0u8; 1];
            let len = self.stream.read_exact(&mut byte).await?;
            if len == 0 {
                bail!("Connection closed");
            }
            len_bytes.push(byte[0]);

            if len_bytes.ends_with(b"\r\n") {
                break;
            }
        }
        let len_str = String::from_utf8_lossy(&len_bytes);
        let len: usize = len_str[1..len_str.len() - 2].parse()?;

        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).await?;
        Ok(data)
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

fn decode_sstring(val: &str) -> String {
    val.trim_start_matches('+')
        .trim_end_matches("\r\n")
        .to_string()
}
