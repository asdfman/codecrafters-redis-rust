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

pub struct RdbFile {
    _header: Vec<u8>,
    _metadata: Vec<u8>,
    pub sections: Vec<DatabaseSection>,
}

pub struct DatabaseSection {
    pub index: u8,
    pub data: HashMap<String, RdbValue>,
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
        let section_size = get_6_bit_integer(bytes);
        let _expiry_size = get_6_bit_integer(bytes);
        let mut section_data = HashMap::new();

        for _ in 0..section_size {
            let expiry_or_type = bytes.get_u8();
            let (k, v) = match expiry_or_type {
                0xfd | 0xfc => panic!("Expiry not supported yet"),
                val => decode_kv(&mut *bytes, val),
            };
            section_data.insert(k, v);
        }

        Ok(DatabaseSection {
            index,
            data: section_data,
        })
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
