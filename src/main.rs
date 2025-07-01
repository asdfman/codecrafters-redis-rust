use anyhow::Result;
use codecrafters_redis::{
    command::Command,
    protocol::{RedisArray, RedisDataType},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let (tx, mut rx) = mpsc::channel::<(
        Box<dyn FnOnce() -> String + Send + 'static>,
        oneshot::Sender<String>,
    )>(100);

    let event_loop = tokio::spawn(async move {
        while let Some((task, result_tx)) = rx.recv().await {
            let result = task();
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
                let task = Box::new(move || process_request(request));
                let _ = tx.send((task, result_tx)).await;
                if let Ok(response) = result_rx.await {
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        });
    }

    event_loop.await?;
    Ok(())
}

fn process_request(request: String) -> String {
    let request = RedisArray::from(request.as_str());
    let Some(RedisDataType::BulkString(command)) = request.0.first() else {
        panic!("Invalid command");
    };
    Command::from(command.as_str()).execute(&request.0[1..])
}
