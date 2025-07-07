use anyhow::Result;
use tokio::net::TcpStream;

use crate::command::send::SendCommand;

pub async fn init_replica(replica_config: &str, listen_port: &str) -> Result<()> {
    let (host, port) = replica_config
        .split_once(char::is_whitespace)
        .expect("Invalid master host/port");
    let stream = TcpStream::connect(format!("{host}:{port}")).await?;
    let mut sender = SendCommand::new(stream);
    sender.send("PING").await?;
    sender
        .send(&format!("REPLCONF listening-port {listen_port}"))
        .await?;
    sender.send("REPLCONF capa psync2").await?;
    sender.send("PSYNC ? -1").await?;
    Ok(())
}
