use anyhow::Result;
use bytes::{Buf, Bytes};
use hashbrown::HashMap;

use crate::store::Value;

use super::length_encoded_value::{get_6_bit_integer, LengthEncodedValue};

const AUXILIARY_FIELD: u8 = 0xFA;
const _EXPIRE_MS: u8 = 0xFD;
const _EXPIRE_S: u8 = 0xFC;
const _EOF: u8 = 0xFF;
const DB_SELECTOR: u8 = 0xFE;

#[derive(Debug)]
pub struct RdbFile {
    _header: Vec<u8>,
    _metadata: Vec<u8>,
    pub sections: Vec<DatabaseSection>,
}

#[derive(Debug)]
pub struct DatabaseSection {
    pub index: u8,
    pub data: HashMap<String, (RdbValue, Option<u64>)>,
}

#[derive(Debug)]
pub enum RdbValue {
    String(String),
}

impl From<RdbValue> for Value {
    fn from(value: RdbValue) -> Self {
        match value {
            RdbValue::String(s) => Value::String(s),
        }
    }
}

impl TryFrom<&mut Bytes> for RdbFile {
    type Error = anyhow::Error;
    fn try_from(bytes: &mut Bytes) -> Result<Self> {
        let _header = split_until_value(bytes, AUXILIARY_FIELD).to_vec();
        bytes.advance(1);
        let _metadata = split_until_value(bytes, DB_SELECTOR).to_vec();
        let mut sections = Vec::new();
        print_mixed_bytes(bytes.chunk());

        while bytes.has_remaining() {
            if bytes.chunk()[0] != DB_SELECTOR {
                break;
            }
            sections.push(DatabaseSection::try_from(&mut *bytes)?);
        }

        Ok(RdbFile {
            _header,
            _metadata,
            sections,
        })
    }
}

impl TryFrom<&mut Bytes> for DatabaseSection {
    type Error = anyhow::Error;
    fn try_from(bytes: &mut Bytes) -> Result<Self> {
        bytes.advance(1);
        let index = bytes.get_u8();
        bytes.advance(1); // skip FB = resizedb
        let Some(section_size) = get_6_bit_integer(bytes) else {
            anyhow::bail!("Expected 6 bit integer for section size");
        };
        let _expiry_size = get_6_bit_integer(bytes);
        let mut section_data = HashMap::new();

        for _ in 0..section_size {
            let mut value_type = bytes.get_u8();
            let expiry = match value_type {
                0xFD => get_expiry(bytes, true),
                0xFC => get_expiry(bytes, false),
                _ => None,
            };
            if expiry.is_some() {
                value_type = bytes.get_u8();
            }
            let (k, v) = decode_kv(&mut *bytes, value_type);
            section_data.insert(k, (v, expiry));
        }
        dbg!(&section_data);

        Ok(DatabaseSection {
            index,
            data: section_data,
        })
    }
}

fn get_expiry(bytes: &mut Bytes, is_sec: bool) -> Option<u64> {
    match is_sec {
        false => Some(bytes.get_u64_le()),
        true => Some(1000 * bytes.get_u32_le() as u64),
    }
}

fn decode_kv(bytes: &mut Bytes, _value_type: u8) -> (String, RdbValue) {
    let key = LengthEncodedValue::get_as_string(bytes);
    let value = RdbValue::String(
        if let LengthEncodedValue::String(val) = LengthEncodedValue::from(bytes) {
            val
        } else {
            panic!("Expected a string value")
        },
    );
    (key, value)
}

fn split_until_value(bytes: &mut Bytes, value: u8) -> Bytes {
    let len = bytes
        .iter()
        .position(|&b| b == value)
        .unwrap_or(bytes.len());
    bytes.split_to(len)
}

fn print_mixed_bytes(bytes: &[u8]) {
    let mut i = 0;
    while i < bytes.len() {
        // Try to find the longest valid UTF-8 sequence
        let mut len = 0;
        for j in i..bytes.len() {
            if std::str::from_utf8(&bytes[i..=j]).is_ok() {
                len = j - i + 1;
            } else {
                break;
            }
        }
        if len > 0 {
            // Print valid UTF-8 as text
            let s = std::str::from_utf8(&bytes[i..i + len]).unwrap();
            print!("{}", s);
            i += len;
        } else {
            // Print single byte as hex using the hex crate
            print!("\\x{}", hex::encode(&bytes[i..i + 1]));
            i += 1;
        }
    }
}
