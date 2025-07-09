use std::sync::Arc;

use anyhow::Result;
use codecrafters_redis::{
    command::response::CommandResponse,
    server::{config::get_config_value, context::ServerContext, replica::init_replica},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listen_port = get_config_value("port").unwrap_or("6379".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{listen_port}")).await?;
    let (tx, mut rx) = mpsc::channel::<(String, oneshot::Sender<CommandResponse>)>(100);
    let context = Arc::new(ServerContext::default());

    if let Some(val) = get_config_value("replicaof") {
        init_replica(&val, &listen_port).await?;
    }

    let context_clone = context.clone();
    let event_loop = tokio::spawn(async move {
        while let Some((task, result_tx)) = rx.recv().await {
            let result = context_clone.execute_command(task).await;
            if result_tx.send(result).is_err() {
                eprintln!("Failed to send response to connection handler.");
            }
        }
    });

    while let Ok((stream, _)) = listener.accept().await {
        let context = context.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(BufReader::new(stream), tx, &context).await {
                eprintln!("Connection ended with error: {e}");
            }
        });
    }

    event_loop.await?;
    Ok(())
}

async fn handle_connection(
    mut reader: BufReader<TcpStream>,
    tx: mpsc::Sender<(String, oneshot::Sender<CommandResponse>)>,
    context: &ServerContext,
) -> Result<()> {
    let mut buffer = [0u8; 1024];
    loop {
        let length = reader.read(&mut buffer).await?;
        if length == 0 {
            return Ok(());
        }

        let request = String::from_utf8_lossy(&buffer[..length]).to_string();
        let (result_tx, result_rx) = oneshot::channel();
        tx.send((request, result_tx)).await?;

        match result_rx.await? {
            CommandResponse::Stream => {
                context.add_replica(reader).await;
                return Ok(());
            }
            CommandResponse::Single(response) => reader.write_all(response.as_bytes()).await?,
        }
    }
}
