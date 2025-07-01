use anyhow::Result;
use bytes::Bytes;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                thread::spawn(|| {
                    let _ = handle_connection(_stream);
                });
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let mut buffer = [0u8; 1024];
    while let Ok(length) = stream.read(&mut buffer) {
        let request_bytes = Bytes::copy_from_slice(&buffer[..length]);
        if request_bytes.is_empty() {
            break;
        }
        let response = process_request(request_bytes)?;
        stream.write_all(&response)?;
        stream.flush()?;
        thread::sleep(Duration::from_millis(1000));
    }
    Ok(())
}

fn process_request(request: Bytes) -> Result<Vec<u8>> {
    let response = "+PONG\r\n";
    Ok(response.as_bytes().to_vec())
}
