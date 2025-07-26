use std::{future::poll_fn, pin::Pin, task::Poll};

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

    pub async fn read_redis_data(&mut self) -> Result<Data> {
        self.current_command_processed_bytes = 0;
        loop {
            loop {
                match self.try_parse_resp() {
                    Some(Data::Array(data)) => self.temp_resp_array = Some(Data::Array(data)),
                    Some(data) if self.expected_array_items > 0 => {
                        self.add_to_array(data);
                        if self.expected_array_items == 0 {
                            self.total_processed_bytes += self.current_command_processed_bytes;
                            return Ok(self.temp_resp_array.take().unwrap());
                        }
                    }
                    Some(data) => {
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
            arr.push(data);
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
        Some(Data::Array(vec![]))
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
        self.stream
            .read_buf(&mut self.buffer)
            .await
            .context("Failed to read from stream")
    }

    async fn non_blocking_read(&mut self) -> Result<usize> {
        let mut temp_buffer = vec![0u8; 1024];
        let mut read_buf = tokio::io::ReadBuf::new(&mut temp_buffer);

        let res = poll_fn(
            |cx| match Pin::new(&mut self.stream).poll_read(cx, &mut read_buf) {
                Poll::Pending | Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            },
        )
        .await;

        match res {
            Ok(()) => {
                let filled = read_buf.filled().len();
                if filled > 0 {
                    self.buffer.extend_from_slice(read_buf.filled());
                }
                Ok(filled)
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e).context("Failed to read from stream"),
        }
    }

    pub async fn expect_bytes(&mut self) -> Result<Vec<u8>> {
        if self.buffer.is_empty() {
            self.read_stream().await?;
        } else {
            self.non_blocking_read().await?;
        }
        if self.buffer.is_empty() || self.buffer[0] != b'$' {
            bail!("Expected bytes, but got: {:?}", self.buffer);
        }
        let Some((data_len, data_start)) = get_len(&self.buffer) else {
            bail!("Failed to get length of data")
        };
        let data = self.buffer[data_start..data_start + data_len].to_vec();
        self.buffer.advance(data_start + data_len);
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
        if self.buffer.is_empty() {
            self.read_stream().await?;
        } else {
            self.non_blocking_read().await?;
        }
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

    pub fn reset_processed_bytes(&mut self) {
        self.total_processed_bytes = 0;
    }

    pub fn get_latest_command_byte_length(&self) -> usize {
        self.current_command_processed_bytes
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
