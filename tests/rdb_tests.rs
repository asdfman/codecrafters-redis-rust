mod utils;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use codecrafters_redis::rdb::{length_encoded_value::LengthEncodedValue, rdb_file::RdbFile};

    use crate::utils;

    #[test]
    fn test_integer_6bit() {
        // 0b11xxxx00: 6 bit integer type, next byte is 8 bit integer
        let mut bytes = Bytes::from_static(&[0b11000000, 0b10101010]);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::Integer(i) => assert_eq!(i, 0b10101010),
            _ => panic!("Expected Integer"),
        }

        // 0b11000001: 6 bit integer type, next 2 bytes is 16 bit integer (little endian)
        let mut bytes = Bytes::from_static(&[0b11000001, 0x34, 0x12]);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::Integer(i) => assert_eq!(i, 0x1234),
            _ => panic!("Expected Integer"),
        }

        // 0b11000010: 6 bit integer type, next 4 bytes is 32 bit integer (little endian)
        let mut bytes = Bytes::from_static(&[0b11000010, 0x78, 0x56, 0x34, 0x12]);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::Integer(i) => assert_eq!(i, 0x12345678),
            _ => panic!("Expected Integer"),
        }
    }

    #[test]
    fn test_string_14bit() {
        // 0b01xxxxxx yyyyyyyy: 14 bit string length
        let mut bytes = Bytes::from_static(&[0b01000000, 0x03, b'a', b'b', b'c']);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::String(s) => assert_eq!(s, "abc"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_string_6bit() {
        // 0b00xxxxxx: 6 bit string length
        let mut bytes = Bytes::from_static(&[0b00000011, b'x', b'y', b'z']);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::String(s) => assert_eq!(s, "xyz"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_string_32bit() {
        // 0b10xxxxxx: next 4 bytes are length
        let mut bytes = Bytes::from_static(&[0b10000000, 0x00, 0x00, 0x00, 0x03, b'1', b'2', b'3']);
        let value = LengthEncodedValue::from(&mut bytes);
        match value {
            LengthEncodedValue::String(s) => assert_eq!(s, "123"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_deserialize_file() {
        let data = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fe00fb010000047065617209626c75656265727279ffb64ad09079e1b2ce";
        let mut bytes: Bytes = utils::hex_to_bytes(data).into();
        let rdb = RdbFile::try_from(&mut bytes).unwrap();
    }
}
