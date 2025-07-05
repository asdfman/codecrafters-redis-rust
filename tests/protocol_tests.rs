#[cfg(test)]
mod tests {
    use codecrafters_redis::protocol::Data;
    use codecrafters_redis::protocol::RedisArray;

    #[test]
    fn test_deserialize() {
        let input = "$10\r\nhellohello\r\n";
        let (data, _) = Data::deserialize(input);
        let Data::BStr(s) = data else {
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
        assert!(matches!(&arr[0], Data::BStr(s) if s == "foo"));
        assert!(matches!(&arr[1], Data::BStr(s) if s == "bar"));
        assert!(matches!(&arr[2], Data::BStr(s) if s == "helloworld"));
    }

    #[test]
    fn test_deserialize_set_with_px() {
        // Example: SET foo bar PX 100
        let input = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n";
        let array = RedisArray::from(input)
            .0
            .iter()
            .map(|x| match x {
                Data::BStr(s) => s.clone(),
                Data::Integer(i) => i.to_string(),
            })
            .collect::<Vec<String>>();
        assert_eq!(array, vec!["SET", "foo", "bar", "PX", "100"]);
    }
}
