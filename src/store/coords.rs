use rust_decimal::{
    dec,
    prelude::{FromPrimitive, ToPrimitive},
    Decimal, MathematicalOps,
};
const MIN_LAT: f64 = -85.05112878;
const MAX_LAT: f64 = 85.05112878;
const LAT_RANGE: f64 = MAX_LAT - MIN_LAT;
const MIN_LONG: f64 = -180.0;
const MAX_LONG: f64 = 180.0;
const LONG_RANGE: f64 = MAX_LONG - MIN_LONG;

#[derive(Clone)]
pub struct Point {
    pub lat: f64,
    pub lon: f64,
}
impl Point {
    pub fn new(lat_str: &str, lon_str: &str) -> Self {
        let lat = lat_str.parse().unwrap_or_default();
        let lon = lon_str.parse().unwrap_or_default();
        Self { lat, lon }
    }
}

pub fn validate_coords(point: &Point) -> Option<String> {
    if (-180.0..=180.0).contains(&point.lon) && (MIN_LAT..=MAX_LAT).contains(&point.lat) {
        None
    } else {
        Some(format!(
            "invalid longitude,latitude pair {},{}",
            point.lon, point.lat
        ))
    }
}

pub fn encode(point: Point) -> Decimal {
    let norm_lat = (2u64.pow(26) as f64 * (point.lat - MIN_LAT) / LAT_RANGE) as u32;
    let norm_long = (2u64.pow(26) as f64 * (point.lon - MIN_LONG) / LONG_RANGE) as u32;

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

pub fn decode(score: Decimal) -> Point {
    let score = score.to_u64().unwrap_or(0);
    let x = score;
    let y = score >> 1;

    let compact_int64_to_int32 = |mut v: u64| {
        v &= 0x5555555555555555;
        v = (v | (v >> 1)) & 0x3333333333333333;
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
        Decimal::from_u32(((v | (v >> 16)) & 0x00000000FFFFFFFF) as u32).unwrap()
    };

    let compacted_lat = compact_int64_to_int32(x);
    let compacted_long = compact_int64_to_int32(y);

    let min_lat = Decimal::from_f64(MIN_LAT).unwrap();
    let lat_range = Decimal::from_f64(MAX_LAT - MIN_LAT).unwrap();
    let min_lon = Decimal::from_f64(MIN_LONG).unwrap();
    let lon_range = Decimal::from_f64(MAX_LONG - MIN_LONG).unwrap();
    let two = dec!(2);

    let grid_lat_min = min_lat + lat_range * (compacted_lat / two.powi(26));
    let grid_lat_max = min_lat + lat_range * ((compacted_lat + dec!(1)) / two.powi(26));
    let grid_long_min = min_lon + lon_range * (compacted_long / two.powi(26));
    let grid_long_max = min_lon + lon_range * ((compacted_long + dec!(1)) / two.powi(26));

    let lat = (grid_lat_min + grid_lat_max) / two;
    let lon = (grid_long_min + grid_long_max) / two;
    Point {
        lat: lat.to_f64().unwrap(),
        lon: lon.to_f64().unwrap(),
    }
}

pub fn haversine(origin: &Point, destination: &Point) -> f64 {
    const EARTH_RADIUS: f64 = 6372797.560856;

    let lat1 = origin.lat.to_radians();
    let lat2 = destination.lat.to_radians();
    let d_lat = lat2 - lat1;
    let d_lon = (destination.lon - origin.lon).to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS * c
}
