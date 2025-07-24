#[cfg(test)]
mod tests {
    use codecrafters_redis::protocol::Data;

    #[test]
    fn test_deserialize() {
        let input = "$10\r\nhellohello\r\n";
        let (data, _) = Data::deserialize(input);
        let Data::BStr(s) = data else {
            panic!("Expected BulkString variant");
        };
        assert_eq!(s, "hellohello");
    }
}
