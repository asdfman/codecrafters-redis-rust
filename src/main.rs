use std::sync::Arc;

use anyhow::{bail, Result};
use codecrafters_redis::{
    command::{definition::Command, response::CommandResponse},
    server::{
        config::get_config_value, context::ServerContext, replica::init_replica,
        stream_reader::StreamReader,
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listen_port = get_config_value("port").unwrap_or("6379".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{listen_port}")).await?;
    let (tx, mut rx) = mpsc::channel::<(Command, Option<oneshot::Sender<CommandResponse>>)>(100);
    let context = Arc::new(ServerContext::default());

    let context_clone = context.clone();
    let event_loop = tokio::spawn(async move {
        while let Some((task, result_tx)) = rx.recv().await {
            let result = context_clone.execute_command(task).await;
            if result_tx.is_some() && result_tx.unwrap().send(result).is_err() {
                eprintln!("Failed to send response to connection handler.");
            }
        }
    });

    if let Some(val) = get_config_value("replicaof") {
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = init_replica(&val, &listen_port, tx).await {
                eprintln!("Failed to initialize replica: {e}");
            }
        });
    }

    while let Ok((stream, _)) = listener.accept().await {
        let context = context.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx, &context).await {
                eprintln!("Connection ended with error: {e}");
            }
        });
    }

    event_loop.await?;
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    tx: mpsc::Sender<(Command, Option<oneshot::Sender<CommandResponse>>)>,
    context: &ServerContext,
) -> Result<()> {
    let mut reader = StreamReader::new(stream, false);
    loop {
        let data = reader.read_command().await?;

        let (result_tx, result_rx) = oneshot::channel();
        tx.send((data.into(), Some(result_tx))).await?;

        match result_rx.await? {
            CommandResponse::Stream => {
                context.add_replica(reader.take_stream()).await;
                return Ok(());
            }
            CommandResponse::Single(response) => reader.write_stream(response.as_bytes()).await?,
            CommandResponse::Multiple(responses) => {
                for response in responses {
                    reader.write_stream(response.as_bytes()).await?;
                }
            }
            CommandResponse::ReplconfAck => bail!("REPLCONF ACK in non-replication stream"),
        }
    }
}
