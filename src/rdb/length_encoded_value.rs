use bytes::{Buf, Bytes};

pub enum LengthEncodedValue {
    String(String),
    Integer(u64),
}

impl From<&mut Bytes> for LengthEncodedValue {
    fn from(bytes: &mut Bytes) -> Self {
        let len = bytes.get_u8();
        match len >> 6 {
            0b11 => LengthEncodedValue::Integer(get_integer(bytes, len)),
            0b01 => {
                let second_byte = bytes.get_u8();
                LengthEncodedValue::String(get_string(bytes, len_14_bits(len, second_byte)))
            }
            0b00 => LengthEncodedValue::String(get_string(bytes, (len & 0b0011_1111) as usize)),
            0b10 => {
                let length = bytes.get_u32() as usize;
                LengthEncodedValue::String(get_string(bytes, length))
            }
            _ => panic!(),
        }
    }
}

impl LengthEncodedValue {
    pub fn get_as_string(bytes: &mut Bytes) -> String {
        if let LengthEncodedValue::String(s) = LengthEncodedValue::from(bytes) {
            s
        } else {
            panic!("Expected a string value")
        }
    }
}

pub fn get_6_bit_integer(bytes: &mut Bytes) -> Option<usize> {
    let val = bytes.get_u8();
    if val & 0b1100_0000 != 0 {
        return None;
    }
    Some(val as usize)
}

fn len_14_bits(first: u8, second: u8) -> usize {
    (((first & 0b0011_1111) as u16) << 8 | (second as u16)) as usize
}

fn get_string(bytes: &mut Bytes, len: usize) -> String {
    String::from_utf8_lossy(&bytes.split_to(len)).to_string()
}

fn get_integer(bytes: &mut Bytes, len: u8) -> u64 {
    match len & 0b00000011 {
        0b00 => bytes.get_u8() as u64,
        0b01 => bytes.get_u16_le() as u64,
        0b10 => bytes.get_u32_le() as u64,
        _ => panic!("Invalid integer length"),
    }
}
