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
        let (client, server) = duplex(1024);

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
    async fn test_process_replica_init_worst_case() -> Result<()> {
        let commands = b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\0\xfa\x08aof-base\xc0\0\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";

        let (mut client, server) = duplex(1024);
        let mut reader = StreamReader::new(server, true);
        client.write_all(commands).await?;
        client.flush().await?;

        reader.receive_sstring().await?; // read the FULLRESYNC command
        reader.expect_bytes().await?;

        // read the SET commands
        let result1 = reader.read_command().await?;
        let Data::Array(RedisArray(array1)) = result1 else {
            panic!("Expected a RedisArray");
        };
        let result2 = reader.read_command().await?;
        let Data::Array(RedisArray(array2)) = result2 else {
            panic!("Expected a RedisArray");
        };
        let result3 = reader.read_command().await?;
        let Data::Array(RedisArray(array3)) = result3 else {
            panic!("Expected a RedisArray");
        };

        let Data::BStr(command1) = array1.first().unwrap() else {
            panic!("Expected a BStr command");
        };
        let Data::BStr(command2) = array2.first().unwrap() else {
            panic!("Expected a BStr command");
        };
        let Data::BStr(command3) = array3.first().unwrap() else {
            panic!("Expected a BStr command");
        };

        assert_eq!(command1, "SET");
        assert_eq!(command2, "SET");
        assert_eq!(command3, "SET");

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_multiple_set_commands() -> Result<()> {
        let (mut client, server) = tokio::io::duplex(1024);
        let set_commands = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        client.write_all(set_commands).await?;
        client.flush().await?; // Ensure data is sent

        let mut reader = StreamReader::new(server, false);

        let result1 = reader.read_command().await?;
        let Data::Array(RedisArray(array1)) = result1 else {
            panic!("Expected a RedisArray for first command");
        };
        assert_eq!(
            array1.len(),
            3,
            "Expected three elements in first SET command"
        );
        let Data::BStr(command1) = array1.first().unwrap() else {
            panic!("Expected a BStr for first element of first command");
        };
        let Data::BStr(key1) = array1.get(1).unwrap() else {
            panic!("Expected a BStr for second element of first command");
        };
        let Data::BStr(value1) = array1.get(2).unwrap() else {
            panic!("Expected a BStr for third element of first command");
        };
        assert_eq!(command1, "SET");
        assert_eq!(key1, "foo");
        assert_eq!(value1, "123");

        let result2 = reader.read_command().await?;
        let Data::Array(RedisArray(array2)) = result2 else {
            panic!("Expected a RedisArray for second command");
        };
        assert_eq!(
            array2.len(),
            3,
            "Expected three elements in second SET command"
        );
        let Data::BStr(command2) = array2.first().unwrap() else {
            panic!("Expected a BStr for first element of second command");
        };
        let Data::BStr(key2) = array2.get(1).unwrap() else {
            panic!("Expected a BStr for second element of second command");
        };
        let Data::BStr(value2) = array2.get(2).unwrap() else {
            panic!("Expected a BStr for third element of second command");
        };
        assert_eq!(command2, "SET");
        assert_eq!(key2, "bar");
        assert_eq!(value2, "456");

        let result3 = reader.read_command().await?;
        let Data::Array(RedisArray(array3)) = result3 else {
            panic!("Expected a RedisArray for third command");
        };
        assert_eq!(
            array3.len(),
            3,
            "Expected three elements in third SET command"
        );
        let Data::BStr(command3) = array3.first().unwrap() else {
            panic!("Expected a BStr for first element of third command");
        };
        let Data::BStr(key3) = array3.get(1).unwrap() else {
            panic!("Expected a BStr for second element of third command");
        };
        let Data::BStr(value3) = array3.get(2).unwrap() else {
            panic!("Expected a BStr for third element of third command");
        };
        assert_eq!(command3, "SET");
        assert_eq!(key3, "baz");
        assert_eq!(value3, "789");

        Ok(())
    }
}
