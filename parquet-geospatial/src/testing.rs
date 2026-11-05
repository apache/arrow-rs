//! Testing utilities for geospatial Parquet types

/// Build well-known binary representing a point with the given XY coordinate
pub fn wkb_point_xy(x: f64, y: f64) -> Vec<u8> {
    let mut item: [u8; 21] = [0; 21];
    item[0] = 0x01;
    item[1] = 0x01;
    item[5..13].copy_from_slice(x.to_le_bytes().as_slice());
    item[13..21].copy_from_slice(y.to_le_bytes().as_slice());
    item.to_vec()
}

/// Build well-known binary representing a point with the given XYZM coordinate
pub fn wkb_point_xyzm(x: f64, y: f64, z: f64, m: f64) -> Vec<u8> {
    let mut item: [u8; 37] = [0; 37];
    item[0] = 0x01;
    item[1..5].copy_from_slice(3001_u32.to_le_bytes().as_slice());
    item[5..13].copy_from_slice(x.to_le_bytes().as_slice());
    item[13..21].copy_from_slice(y.to_le_bytes().as_slice());
    item[21..29].copy_from_slice(z.to_le_bytes().as_slice());
    item[29..37].copy_from_slice(m.to_le_bytes().as_slice());
    item.to_vec()
}

#[cfg(test)]
mod test {

    use wkb::reader::Wkb;

    use super::*;

    #[test]
    fn test_wkb_item() {
        let bytes = wkb_point_xy(1.0, 2.0);
        let geometry = Wkb::try_new(&bytes).unwrap();
        let mut wkt = String::new();
        wkt::to_wkt::write_geometry(&mut wkt, &geometry).unwrap();
        assert_eq!(wkt, "POINT(1 2)");
    }

    #[test]
    fn test_wkb_point_xyzm() {
        let bytes = wkb_point_xyzm(1.0, 2.0, 3.0, 4.0);
        let geometry = Wkb::try_new(&bytes).unwrap();
        let mut wkt = String::new();
        wkt::to_wkt::write_geometry(&mut wkt, &geometry).unwrap();
        assert_eq!(wkt, "POINT ZM(1 2 3 4)");
    }
}
