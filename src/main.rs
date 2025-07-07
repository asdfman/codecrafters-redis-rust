use anyhow::Result;
use codecrafters_redis::{
    command::response::CommandResponse,
    server::{config::get_config_value, context::ServerContext, replica::init_replica},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listen_port = get_config_value("port").unwrap_or("6379".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{listen_port}")).await?;
    let (tx, mut rx) = mpsc::channel::<(String, oneshot::Sender<CommandResponse>)>(100);

    if let Some(val) = get_config_value("replicaof") {
        init_replica(&val, &listen_port).await?;
    }

    let event_loop = tokio::spawn(async move {
        let server_context = ServerContext::default();
        while let Some((task, result_tx)) = rx.recv().await {
            let result = server_context.process_request(task).await;
            if result_tx.send(result).is_err() {
                eprintln!("Failed to send response to connection handler.");
            }
        }
    });

    while let Ok((stream, _)) = listener.accept().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx).await {
                eprintln!("Connection ended with error: {e}");
            }
        });
    }

    event_loop.await?;
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    tx: mpsc::Sender<(String, oneshot::Sender<CommandResponse>)>,
) -> Result<()> {
    let mut buffer = [0u8; 1024];
    loop {
        let length = stream.read(&mut buffer).await?;
        if length == 0 {
            return Ok(());
        }

        let request = String::from_utf8_lossy(&buffer[..length]).to_string();
        let (result_tx, result_rx) = oneshot::channel();

        if tx.send((request, result_tx)).await.is_err() {
            anyhow::bail!("Event loop receiver dropped");
        }

        match result_rx.await {
            Ok(response) => write_response(&mut stream, response).await?,
            Err(_) => anyhow::bail!("Event loop dropped response sender"),
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: CommandResponse) -> Result<()> {
    match response {
        CommandResponse::Single(data) => stream.write_all(data.as_bytes()).await?,
        CommandResponse::Multi(data) => {
            for res in data {
                stream.write_all(&Vec::<u8>::from(res)).await?;
            }
        }
    }
    Ok(())
}

