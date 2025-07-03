#[cfg(test)]
mod tests {
    use codecrafters_redis::{command::Command, protocol::RedisData, store::InMemoryStore};
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_set_with_expiry() {
        let store = InMemoryStore::default();
        let args = [
            RedisData::BulkString("SET".to_string()),
            RedisData::BulkString("apple".to_string()),
            RedisData::BulkString("strawberry".to_string()),
            RedisData::BulkString("px".to_string()),
            RedisData::BulkString("100".to_string()),
        ];
        let result = Command::from(args.as_slice()).execute(&store).await;
        assert_eq!(result, "$2\r\nOK\r\n");
        assert_eq!(
            store.get("apple").await.unwrap(),
            codecrafters_redis::store::Value::String("strawberry".to_string())
        );
        sleep(Duration::from_millis(101)).await;
        assert!(store.get("mykey").await.is_none());
    }
}
