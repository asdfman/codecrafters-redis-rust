use super::{context::ServerContext, stream_reader::StreamReader};
use crate::{
    command::{core::Command, response::CommandResponse},
    common::{encode_error, encode_sstring},
};
use anyhow::{bail, Result};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};

pub async fn handle_connection(
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
                let response = encode_error("EXEC without MULTI");
                reader.write_stream(response.as_bytes()).await?;
                continue;
            }
            Command::Exec => {
                let (result_tx, result_rx) = oneshot::channel();
                let tr_command = Command::Transaction(transaction_commands.clone());
                tx.send((tr_command, Some(result_tx))).await?;
                let response = result_rx.await?;
                reader
                    .write_stream(String::from(response).as_bytes())
                    .await?;
                transaction_commands.clear();
                in_transaction = false;
                continue;
            }
            Command::Discard if in_transaction => {
                in_transaction = false;
                transaction_commands.clear();
                reader.write_stream(encode_sstring("OK").as_bytes()).await?;
                continue;
            }
            Command::Discard if !in_transaction => {
                let response = encode_error("DISCARD without MULTI");
                reader.write_stream(response.as_bytes()).await?;
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
