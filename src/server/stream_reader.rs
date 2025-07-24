use anyhow::{bail, Context, Result};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::{Data, RedisArray, CRLF_LEN};
const CR: u8 = b'\r';

pub struct StreamReader<T: AsyncRead + AsyncWrite + Unpin> {
    pub stream: T,
    buffer: BytesMut,
    temp_resp_array: Option<Data>,
    expected_array_items: usize,
    current_command_processed_bytes: usize,
    total_processed_bytes: usize,
    is_replication_stream: bool,
}

impl<T: AsyncRead + AsyncWrite + Unpin> StreamReader<T> {
    pub fn new(stream: T, is_replication_stream: bool) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(2048),
            temp_resp_array: None,
            expected_array_items: 0,
            current_command_processed_bytes: 0,
            total_processed_bytes: 0,
            is_replication_stream,
        }
    }

    pub async fn read_command(&mut self) -> Result<Data> {
        self.current_command_processed_bytes = 0;
        loop {
            loop {
                println!("Buffer before parsing: {:?}", self.buffer);
                match self.try_parse_resp() {
                    Some(Data::Array(data)) => self.temp_resp_array = Some(Data::Array(data)),
                    Some(data) if self.expected_array_items > 0 => {
                        self.add_to_array(data);
                        if self.expected_array_items == 0 {
                            println!("Returning array: {:?}", self.temp_resp_array);
                            self.total_processed_bytes += self.current_command_processed_bytes;
                            return Ok(self.temp_resp_array.take().unwrap());
                        }
                    }
                    Some(data) => {
                        println!("Returning data: {data:?}");
                        self.total_processed_bytes += self.current_command_processed_bytes;
                        return Ok(data);
                    }
                    None => break,
                }
            }
            let len = self.read_stream().await?;
            if len == 0 && self.buffer.is_empty() {
                bail!("EOF received")
            }
        }
    }

    fn add_to_array(&mut self, data: Data) {
        if let Some(Data::Array(arr)) = self.temp_resp_array.as_mut() {
            arr.0.push(data);
            self.expected_array_items -= 1;
        }
    }

    fn try_parse_resp(&mut self) -> Option<Data> {
        if self.buffer.is_empty() {
            return None;
        }
        match self.buffer[0] {
            b'+' => self.parse_simple_string(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => None,
        }
    }

    fn parse_array(&mut self) -> Option<Data> {
        let (data_len, data_start) = get_len(&self.buffer)?;
        self.advance(data_start);
        self.expected_array_items = data_len;
        Some(Data::Array(RedisArray(vec![])))
    }

    fn parse_bulk_string(&mut self) -> Option<Data> {
        let (data_len, data_start) = get_len(&self.buffer)?;
        let total_len = data_start + data_len + CRLF_LEN;
        if self.buffer.len() < total_len {
            return None;
        }
        let data = Some(Data::BStr(
            str::from_utf8(&self.buffer[data_start..data_start + data_len])
                .ok()?
                .to_string(),
        ));
        self.advance(total_len);
        data
    }

    fn parse_simple_string(&mut self) -> Option<Data> {
        let data_end = self.buffer.iter().position(|&b| b == CR)?;
        let data = Some(Data::SStr(
            str::from_utf8(&self.buffer[1..data_end]).ok()?.to_string(),
        ));
        self.advance(data_end + CRLF_LEN);
        data
    }

    fn advance(&mut self, val: usize) {
        self.buffer.advance(val);
        if self.is_replication_stream {
            self.current_command_processed_bytes += val;
        }
    }

    pub fn get_processed_bytes(&self) -> usize {
        self.total_processed_bytes
    }

    async fn read_stream(&mut self) -> Result<usize> {
        println!("Buffer before read: {:?}", self.buffer);
        let res = self
            .stream
            .read_buf(&mut self.buffer)
            .await
            .context("Failed to read from stream");
        println!("Buffer after read: {:?}", self.buffer);
        res
    }

    pub async fn expect_bytes(&mut self) -> Result<Vec<u8>> {
        self.read_stream().await?;
        println!("Buffer after reading bytes: {:?}", self.buffer);
        if self.buffer.is_empty() || self.buffer[0] != b'$' {
            bail!("Expected bytes, but got: {:?}", self.buffer);
        }
        let Some((data_len, data_start)) = get_len(&self.buffer) else {
            bail!("Failed to get length of data")
        };
        let data = self.buffer[data_start..data_start + data_len].to_vec();
        self.buffer.advance(data_start + data_len);
        println!("Buffer after extracting bytes: {:?}", self.buffer);
        Ok(data)
    }

    pub async fn expect_response(&mut self, expected: &str) -> Result<()> {
        let val = self.receive_sstring().await?;
        if val == expected {
            Ok(())
        } else {
            bail!("Expected response '{}', but got '{}'", expected, val);
        }
    }

    pub async fn write_stream(&mut self, data: &[u8]) -> Result<()> {
        self.stream
            .write_all(data)
            .await
            .context("Failed to write to stream")
    }

    pub async fn send(&mut self, command: &str) -> Result<()> {
        self.write_stream(encode_command(command).as_bytes())
            .await
            .context("Failed to send command")
    }

    pub async fn receive_sstring(&mut self) -> Result<String> {
        self.read_stream().await?;
        if self.buffer.is_empty() || self.buffer[0] != b'+' {
            bail!(
                "Expected simple string response, but got: {:?}",
                self.buffer
            );
        }
        let Some(Data::SStr(simple_string)) = self.parse_simple_string() else {
            bail!("Failed to parse simple string from buffer");
        };
        Ok(simple_string)
    }

    pub fn take_stream(self) -> T {
        self.stream
    }

    pub fn reset_processed_bytes(&mut self) {
        self.total_processed_bytes = 0;
    }
    pub fn ignore_latest_processed_bytes(&mut self) {
        self.total_processed_bytes -= self.current_command_processed_bytes;
    }
}

fn get_len(val: &[u8]) -> Option<(usize, usize)> {
    let len_str = std::str::from_utf8(&val[1..val.iter().position(|&b| b == CR)?]).ok()?;
    let len = len_str.parse().ok()?;
    Some((len, len_str.len() + CRLF_LEN + 1))
}

fn encode_command(cmd: &str) -> String {
    RedisArray(
        cmd.split_whitespace()
            .map(|x| Data::BStr(x.into()))
            .collect::<Vec<Data>>(),
    )
    .into()
}
