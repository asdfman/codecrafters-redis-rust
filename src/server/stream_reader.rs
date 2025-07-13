use anyhow::{bail, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::TcpStream,
};

use crate::protocol::RedisArray;

struct StreamReader {
    stream: BufReader<TcpStream>,
    buffer: BytesMut,
}

impl StreamReader {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            buffer: BytesMut::with_capacity(2048),
        }
    }

    // async fn try_read(&mut self) -> Option<RedisArray> {
    //     match self.stream.read_buf(&mut self.buffer).await {
    //         Ok(0) => bail!("Connection closed"),
    //         Ok(_) => DecodeUtf16Error,
    //     }
    // }
}
