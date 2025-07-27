use anyhow::{bail, Result};
use codecrafters_redis::{
    command::{core::Command, handlers::encode_sstring, response::CommandResponse},
    protocol::Data,
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
    let context = ServerContext::default();

    let context_clone = context.clone();
    let event_loop = tokio::spawn(async move {
        while let Some((task, result_tx)) = rx.recv().await {
            if task.is_blocking() {
                let context = context_clone.clone();
                tokio::spawn(async move {
                    let result = context.execute_command(task).await;
                    if let Some(tx) = result_tx {
                        let _ = tx.send(result);
                    }
                });
            } else {
                let result = context_clone.execute_command(task).await;
                if result_tx.is_some() && result_tx.unwrap().send(result).is_err() {
                    eprintln!("Failed to send response to connection handler.");
                }
            }
        }
    });

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
            if let Err(e) = handle_connection(stream, tx, context).await {
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
    context: ServerContext,
) -> Result<()> {
    let mut reader = StreamReader::new(stream, false);
    let mut transaction_commands = vec![];
    let mut in_transaction = false;
    loop {
        let data = reader.read_redis_data().await?;
        let command: Command = data.into();
        match &command {
            Command::Multi => {
                in_transaction = true;
                reader.write_stream(encode_sstring("OK").as_bytes()).await?;
                continue;
            }
            Command::Exec if !in_transaction => {
                let response = String::from(&Data::SimpleError("EXEC without MULTI".to_string()));
                reader.write_stream(response.as_bytes()).await?;
                continue;
            }
            Command::Exec => {
                let response = context
                    .process_transaction(transaction_commands.clone())
                    .await;
                reader
                    .write_stream(String::from(response).as_bytes())
                    .await?;
                transaction_commands.clear();
                in_transaction = false;
                continue;
            }
            _ if in_transaction => {
                transaction_commands.push(command.clone());
                reader
                    .write_stream(encode_sstring("QUEUED").as_bytes())
                    .await?;
                continue;
            }
            _ => (),
        }

        let (result_tx, result_rx) = oneshot::channel();
        tx.send((command, Some(result_tx))).await?;

        match result_rx.await? {
            CommandResponse::Stream => {
                context.add_replica(reader).await;
                return Ok(());
            }
            CommandResponse::Single(response) => reader.write_stream(response.as_bytes()).await?,
            CommandResponse::Multiple(responses) => {
                for response in responses {
                    reader.write_stream(response.as_bytes()).await?;
                }
            }
            CommandResponse::ReplconfAck => bail!("REPLCONF ACK in client stream"),
        }
    }
}
