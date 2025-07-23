#[cfg(test)]
mod tests {
    use anyhow::Result;
    use codecrafters_redis::protocol::Data;
    use codecrafters_redis::protocol::RedisArray;
    use codecrafters_redis::server::stream_reader::StreamReader;
    use tokio::io::duplex;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_parse_ping_command() -> Result<()> {
        // Create a duplex stream to simulate TcpStream
        let (client, server) = duplex(1024);

        // Write the PING command to the server side of the stream
        let ping_command = b"*1\r\n$4\r\nPING\r\n";
        let mut client = client;
        client.write_all(ping_command).await?;
        client.flush().await?;

        let mut reader = StreamReader::new(server, false);

        let result = reader.read_command().await?;

        let Data::Array(RedisArray(array)) = result else {
            panic!("Expected a RedisArray");
        };
        let Data::BStr(command) = array.first().unwrap() else {
            panic!("Expected a BStr command");
        };

        assert_eq!(command, "PING");

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_multiple_ping_commands() -> Result<()> {
        // Create a duplex stream to simulate TcpStream
        let (mut client, server) = duplex(1024);

        // Write two PING commands to the server side of the stream
        let ping_commands = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        assert_eq!(
            ping_commands.len(),
            28,
            "Two PING commands should be 28 bytes"
        );
        client.write_all(ping_commands).await?;
        client.flush().await?; // Ensure data is sent

        let mut reader = StreamReader::new(server, false);

        let result1 = reader.read_command().await?;
        let Data::Array(RedisArray(array1)) = result1 else {
            panic!("Expected a RedisArray for first command");
        };
        let Data::BStr(command1) = array1.first().unwrap() else {
            panic!("Expected a BStr command for first command");
        };
        assert_eq!(command1, "PING");

        let result2 = reader.read_command().await?;
        let Data::Array(RedisArray(array2)) = result2 else {
            panic!("Expected a RedisArray for second command");
        };
        let Data::BStr(command2) = array2.first().unwrap() else {
            panic!("Expected a BStr command for second command");
        };
        assert_eq!(command2, "PING");

        assert_eq!(reader.get_processed_bytes(), 0);

        Ok(())
    }
}
