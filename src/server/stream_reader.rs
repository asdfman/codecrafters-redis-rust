use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::protocol::{Data, RedisArray, CRLF_LEN};
const CRLF: u8 = b'\r';

pub struct StreamReader {
    pub stream: TcpStream,
    buffer: BytesMut,
    temp_resp_array: Option<Vec<Data>>,
    expected_array_items: usize,
    processed_bytes: usize,
    is_replication_stream: bool,
}

impl StreamReader {
    pub fn new(stream: TcpStream, is_replication_stream: bool) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(2048),
            temp_resp_array: None,
            expected_array_items: 0,
            processed_bytes: 0,
            is_replication_stream,
        }
    }

    pub async fn read_command(&mut self) -> Result<Option<Data>> {
        loop {
            dbg!(&self.buffer);
            if let Some(data) = self.try_parse_resp() {
                if let Some(arr) = self.temp_resp_array.as_mut() {
                    arr.push(data);
                    self.expected_array_items -= 1;
                    if self.expected_array_items == 0 {
                        return Ok(Some(Data::Array(RedisArray(
                            self.temp_resp_array.take().unwrap(),
                        ))));
                    }
                } else {
                    return Ok(Some(data));
                }
            }
            dbg!(&self.buffer);
            let Ok(len) = self.stream.read_buf(&mut self.buffer).await else {
                bail!("Error reading stream")
            };
            if len == 0 {
                bail!("EOF received")
            }
            if self.buffer.is_empty() {
                return Ok(None);
            }
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
        let (data_len, data_start) = get_len(&self.buffer[1..])?;
        self.advance(data_start + CRLF_LEN);
        self.expected_array_items = data_len;
        self.temp_resp_array = Some(vec![]);
        None
    }

    fn parse_bulk_string(&mut self) -> Option<Data> {
        let (data_len, data_start) = get_len(&self.buffer[1..])?;
        let total_len = data_start + data_len + CRLF_LEN;
        if self.buffer.len() < total_len + 1 {
            return None;
        }
        let data = Some(Data::BStr(
            str::from_utf8(&self.buffer[data_start..data_start + data_len])
                .ok()?
                .to_string(),
        ));
        self.advance(data_start + total_len);
        data
    }

    fn parse_simple_string(&mut self) -> Option<Data> {
        let data_end = self.buffer.iter().position(|&b| b == CRLF)?;
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
}

fn get_len(val: &[u8]) -> Option<(usize, usize)> {
    let len_str = std::str::from_utf8(&val[..val.iter().position(|&b| b == CRLF)?]).ok()?;
    let len = len_str.parse().ok()?;
    Some((len, len_str.len() + CRLF_LEN))
}
