use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::protocol::{Data, RedisArray};

pub struct SendCommand<'a> {
    stream: &'a mut BufReader<TcpStream>,
    buffer: [u8; 1024],
}

impl<'a> SendCommand<'a> {
    pub fn new(stream: &'a mut BufReader<TcpStream>) -> Self {
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
        loop {
            let len = self.stream.read(&mut self.buffer).await?;
            if len == 0 {
                continue;
            }
            return Ok(self.buffer[..len].to_vec());
        }
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
