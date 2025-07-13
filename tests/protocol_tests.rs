#[cfg(test)]
mod tests {
    use codecrafters_redis::protocol::split_redis_array_string;
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
        let input = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n";
        let array = RedisArray::from(input)
            .0
            .iter()
            .map(|x| match x {
                Data::BStr(s) => s.clone(),
                Data::SStr(s) => s.clone(),
            })
            .collect::<Vec<String>>();
        assert_eq!(array, vec!["SET", "foo", "bar", "PX", "100"]);
    }

    #[test]
    fn test_split_single_array() {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let expected = vec![String::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")];
        assert_eq!(split_redis_array_string(input), expected);
    }

    #[test]
    fn test_split_multiple_arrays() {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$3\r\nbaz\r\n";
        let expected = vec![
            String::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"),
            String::from("*1\r\n$3\r\nbaz\r\n"),
        ];
        assert_eq!(split_redis_array_string(input), expected);
    }

    #[test]
    fn test_single_element_array() {
        let input = "*1\r\n$3\r\nfoo\r\n";
        let expected = vec![String::from("*1\r\n$3\r\nfoo\r\n")];
        assert_eq!(split_redis_array_string(input), expected);
    }
    #[test]
    fn test_two_element_array() {
        let input = "*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        let expected = vec![
            String::from("*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n"),
            String::from("*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"),
        ];
        assert_eq!(split_redis_array_string(input), expected);
    }
}
