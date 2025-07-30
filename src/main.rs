use anyhow::Result;
use codecrafters_redis::{
    command::core::Command,
    server::{
        config::get_config_value,
        connection_handler::{ChannelType, ConnectionHandler},
        context::ServerContext,
        replica::init_replica,
    },
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Receiver},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listen_port = get_config_value("port").unwrap_or("6379".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{listen_port}")).await?;
    let (tx, rx) = mpsc::channel::<ChannelType>(100);
    let context = ServerContext::default();

    let context_clone = context.clone();
    let event_loop_future = tokio::spawn(async move { event_loop(rx, context_clone).await });

    if let Some(val) = get_config_value("replicaof") {
        let tx = tx.clone();
        if let Err(e) = init_replica(&val, &listen_port, tx).await {
            eprintln!("Failed to initialize replica: {e}");
        }
    }

    while let Ok((stream, _)) = listener.accept().await {
        let context = context.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = ConnectionHandler::new(stream, tx, context).handle().await {
                eprintln!("Connection ended with error: {e}");
            }
        });
    }

    event_loop_future.await?;
    Ok(())
}

async fn event_loop(mut rx: Receiver<ChannelType>, context: ServerContext) -> () {
    while let Some((task, result_tx)) = rx.recv().await {
        match task {
            Command::Transaction(commands) => {
                let result = context.process_transaction(commands).await;
                if result_tx.is_some() && result_tx.unwrap().send(result).is_err() {
                    eprintln!("Failed to send response to connection handler.");
                }
            }
            _ if task.is_blocking() => {
                let context = context.clone();
                tokio::spawn(async move {
                    let result = context.execute_command(task).await;
                    if let Some(tx) = result_tx {
                        let _ = tx.send(result);
                    }
                });
            }
            _ => {
                let result = context.execute_command(task).await;
                if result_tx.is_some() && result_tx.unwrap().send(result).is_err() {
                    eprintln!("Failed to send response to connection handler.");
                }
            }
        }
    }
}
