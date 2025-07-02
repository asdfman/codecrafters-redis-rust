#[cfg(test)]
mod tests {
    use codecrafters_redis::protocol::RedisArray;
    use codecrafters_redis::protocol::RedisDataType;

    #[test]
    fn test_deserialize() {
        let input = "$10\r\nhellohello\r\n";
        let (data, _) = RedisDataType::deserialize(input);
        let RedisDataType::BulkString(s) = data else {
            panic!("Expected BulkString variant");
        };
        assert_eq!(s, "hellohello");
    }

    #[test]
    fn test_array_deserialize() {
        let input = "*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$10\r\nhelloworld\r\n";
        let data = RedisArray::from(input);
        let arr = data.0;
        assert_eq!(arr.len(), 3);
        assert!(matches!(&arr[0], RedisDataType::BulkString(s) if s == "foo"));
        assert!(matches!(&arr[1], RedisDataType::BulkString(s) if s == "bar"));
        assert!(matches!(&arr[2], RedisDataType::BulkString(s) if s == "helloworld"));
    }
}
