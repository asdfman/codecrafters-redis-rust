use anyhow::Result;
use codecrafters_redis::{
    command::Command,
    protocol::{RedisArray, RedisDataType},
    store::InMemoryStore,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let (tx, mut rx) = mpsc::channel::<(String, oneshot::Sender<String>)>(100);
    let store = InMemoryStore::default();

    let store = store.clone();
    let event_loop = tokio::spawn(async move {
        while let Some((task, result_tx)) = rx.recv().await {
            let result = process_request(task, &store).await;
            let _ = result_tx.send(result);
        }
    });

    while let Ok((mut stream, _)) = listener.accept().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];
            while let Ok(length) = stream.read(&mut buffer).await {
                if length == 0 {
                    break;
                }
                let request = String::from_utf8_lossy(&buffer[..length]).to_string();
                let (result_tx, result_rx) = oneshot::channel();
                let _ = tx.send((request, result_tx)).await;
                if let Ok(response) = result_rx.await {
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        });
    }

    event_loop.await?;
    Ok(())
}

async fn process_request(request: String, store: &InMemoryStore) -> String {
    let request = RedisArray::from(request.as_str());
    let Some(RedisDataType::BulkString(command)) = request.0.first() else {
        panic!("Invalid command");
    };
    Command::from(command.as_str())
        .execute(&request.0[1..], store)
        .await
}
