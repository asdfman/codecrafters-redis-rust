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
    processed_bytes: usize,
    is_replication_stream: bool,
}

impl<T: AsyncRead + AsyncWrite + Unpin> StreamReader<T> {
    pub fn new(stream: T, is_replication_stream: bool) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(2048),
            temp_resp_array: None,
            expected_array_items: 0,
            processed_bytes: 0,
            is_replication_stream,
        }
    }

    pub async fn read_command(&mut self) -> Result<Data> {
        loop {
            loop {
                match self.try_parse_resp() {
                    Some(Data::Array(data)) => self.temp_resp_array = Some(Data::Array(data)),
                    Some(data) if self.expected_array_items > 0 => {
                        self.add_to_array(data);
                        if self.expected_array_items == 0 {
                            return Ok(self.temp_resp_array.take().unwrap());
                        }
                    }
                    Some(data) => {
                        return Ok(data);
                    }
                    None => break,
                }
            }
            let len = self.stream.read_buf(&mut self.buffer).await?;
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
            self.processed_bytes += val;
        }
    }

    pub fn get_processed_bytes(&self) -> usize {
        self.processed_bytes
    }

    pub async fn receive_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.stream.read_buf(&mut self.buffer).await?;
        if self.buffer[0] != b'$' {
            return Ok(vec![]);
        }
        let Some((data_len, data_start)) = get_len(&self.buffer) else {
            bail!("Failed to get length of data")
        };
        let data = self.buffer[data_start..data_start + data_len].to_vec();
        self.buffer.advance(data_start + data_len);
        Ok(data)
    }

    pub async fn write_stream(&mut self, data: &[u8]) -> Result<()> {
        self.stream
            .write_all(data)
            .await
            .context("Failed to write to stream")
    }

    pub fn take_stream(self) -> T {
        self.stream
    }
}

fn get_len(val: &[u8]) -> Option<(usize, usize)> {
    let len_str = std::str::from_utf8(&val[1..val.iter().position(|&b| b == CR)?]).ok()?;
    let len = len_str.parse().ok()?;
    Some((len, len_str.len() + CRLF_LEN + 1))
}
