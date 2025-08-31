use rust_decimal::{prelude::FromPrimitive, prelude::ToPrimitive, Decimal};

const MIN_LAT: f64 = -85.05112878;
const MAX_LAT: f64 = 85.05112878;
const LAT_RANGE: f64 = MAX_LAT - MIN_LAT;
const MIN_LONG: f64 = -180.0;
const MAX_LONG: f64 = 180.0;
const LONG_RANGE: f64 = MAX_LONG - MIN_LONG;

pub fn validate_coords(lat: f64, long: f64) -> Option<String> {
    if (-180.0..=180.0).contains(&long) && (MIN_LAT..=MAX_LAT).contains(&lat) {
        None
    } else {
        Some(format!("invalid longitude,latitude pair {long},{lat}"))
    }
}

pub fn encode(lat: f64, long: f64) -> Decimal {
    let norm_long = (2u64.pow(26) as f64 * (long - MIN_LONG) / LONG_RANGE) as u32;
    let norm_lat = (2u64.pow(26) as f64 * (lat - MIN_LAT) / LAT_RANGE) as u32;

    let spread_int32_to_int64 = |v: u32| {
        let mut v = v as u64;
        v &= 0xFFFFFFFF;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
        v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
        v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v << 2)) & 0x3333333333333333;
        v = (v | (v << 1)) & 0x5555555555555555;
        v
    };

    let x = spread_int32_to_int64(norm_lat);
    let y = spread_int32_to_int64(norm_long);
    let y_shifted = y << 1;
    Decimal::from_u64(x | y_shifted).unwrap_or_default()
}

pub fn decode(score: Decimal) -> (f64, f64) {
    let score = score.to_u64().unwrap_or(0);
    dbg!(score);
    let x = score;
    let y = score >> 1;

    let compact_int64_to_int32 = |mut v: u64| {
        v &= 0x5555555555555555;
        v = (v | (v >> 1)) & 0x3333333333333333;
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
        ((v | (v >> 16)) & 0x00000000FFFFFFFF) as u32
    };

    let compacted_lat = compact_int64_to_int32(x);
    let compacted_long = compact_int64_to_int32(y);

    let grid_lat_min = MIN_LAT + LAT_RANGE * (compacted_lat as f64 / 2.0f64.powi(26));
    let grid_lat_max = MIN_LAT + LAT_RANGE * ((compacted_lat + 1) as f64 / 2.0f64.powi(26));
    let grid_long_min = MIN_LONG + LONG_RANGE * (compacted_long as f64 / 2.0f64.powi(26));
    let grid_long_max = MIN_LONG + LONG_RANGE * ((compacted_long + 1) as f64 / 2.0f64.powi(26));

    let lat = (grid_lat_min + grid_lat_max) / 2.0;
    let long = (grid_long_min + grid_long_max) / 2.0;
    (lat, long)
}
