use super::{context::ServerContext, stream_reader::StreamReader};
use crate::{
    channel::{context::SubscriptionContext, ChannelCommand},
    command::{core::Command, response::CommandResponse},
    common::{encode_error, encode_sstring},
    protocol::Data,
};
use anyhow::{bail, Result};
use tokio::{
    net::TcpStream,
    sync::{mpsc::Sender, oneshot},
};

pub type ChannelType = (Command, Option<oneshot::Sender<CommandResponse>>);

pub struct ConnectionHandler {
    reader: Option<StreamReader<TcpStream>>,
    in_transaction: bool,
    transaction_commands: Vec<Command>,
    tx: Sender<ChannelType>,
    context: ServerContext,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, tx: Sender<ChannelType>, context: ServerContext) -> Self {
        Self {
            reader: Some(StreamReader::new(stream, false)),
            in_transaction: false,
            transaction_commands: vec![],
            tx,
            context,
        }
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(reader) = &mut self.reader {
            reader.write_stream(bytes).await?;
        }
        Ok(())
    }

    async fn read(&mut self) -> Result<Data> {
        if let Some(reader) = &mut self.reader {
            return reader.read_redis_data().await;
        }
        bail!("No reader");
    }

    async fn handle_transaction(&mut self, command: &Command) -> Result<bool> {
        match &command {
            Command::Multi => {
                self.in_transaction = true;
                self.write(encode_sstring("OK").as_bytes()).await?;
            }
            Command::Exec if !self.in_transaction => {
                let response = encode_error("EXEC without MULTI");
                self.write(response.as_bytes()).await?;
            }
            Command::Exec => {
                let (result_tx, result_rx) = oneshot::channel();
                let tr_command = Command::Transaction(self.transaction_commands.clone());
                self.tx.send((tr_command, Some(result_tx))).await?;
                let response = result_rx.await?;
                self.write(String::from(response).as_bytes()).await?;
                self.transaction_commands.clear();
                self.in_transaction = false;
            }
            Command::Discard if self.in_transaction => {
                self.in_transaction = false;
                self.transaction_commands.clear();
                self.write(encode_sstring("OK").as_bytes()).await?;
            }
            Command::Discard if !self.in_transaction => {
                let response = encode_error("DISCARD without MULTI");
                self.write(response.as_bytes()).await?;
            }
            _ if self.in_transaction => {
                self.transaction_commands.push(command.clone());
                self.write(encode_sstring("QUEUED").as_bytes()).await?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    async fn subscribe_mode(&mut self, channel: String) -> Result<()> {
        let mut sub_context = SubscriptionContext::new(self.context.channels.clone()).await;
        let response = sub_context
            .process_command(ChannelCommand::Subscribe(channel))
            .await?;
        self.write(response.as_bytes()).await?;

        loop {
            tokio::select! {
                Ok(data) = self.read() => {
                    if let Ok(response) = sub_context.process_command(data.into()).await {
                        self.write(response.as_bytes()).await?
                    } else {
                        break;
                    }
                }
                Some(publish) = sub_context.receive_publish() => {
                    self.write(publish.as_bytes()).await?
                }
            }
        }
        Ok(())
    }

    pub async fn handle(&mut self) -> Result<()> {
        loop {
            let data = self.read().await?;
            let command: Command = data.into();

            match command {
                Command::Psync(..) => {
                    self.context.add_replica(self.reader.take().unwrap()).await;
                    return Ok(());
                }
                Command::Subscribe(channel) => {
                    self.subscribe_mode(channel).await?;
                    continue;
                }
                _ => (),
            }

            if self.handle_transaction(&command).await? {
                continue;
            }

            let (result_tx, result_rx) = oneshot::channel();
            self.tx.send((command, Some(result_tx))).await?;

            match result_rx.await? {
                CommandResponse::Single(response) => self.write(response.as_bytes()).await?,
                CommandResponse::Multiple(responses) => {
                    for response in responses {
                        self.write(response.as_bytes()).await?;
                    }
                }
                _ => (),
            }
        }
    }
}
