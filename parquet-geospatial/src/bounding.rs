// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;

use arrow_schema::ArrowError;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use wkb::reader::{Dimension as WkbDimension, Wkb};

use crate::interval::{Interval, IntervalTrait, WraparoundInterval};

/// Geometry bounder
///
/// Utility to accumulate statistics for geometries as they are written.
/// This bounder is designed to output statistics accumulated according
/// to the Parquet specification such that the output can be written to
/// Parquet statistics with minimal modification.
///
/// See the [IntervalTrait] for an in-depth discussion of wraparound bounding
/// (which adds some complexity to this implementation).
#[derive(Debug)]
pub struct GeometryBounder {
    /// Union of all contiguous x intervals to the left of the wraparound midpoint
    x_left: Interval,
    /// Union of all contiguous x intervals that intersect the wraparound midpoint
    x_mid: Interval,
    /// Union of all contiguous x intervals to the right of the wraparound midpoint
    x_right: Interval,
    /// Union of all y intervals
    y: Interval,
    /// Union of all z intervals
    z: Interval,
    /// Union of all m intervals
    m: Interval,
    /// Unique geometry type codes encountered by the bounder
    ///
    /// The integer codes are identical to the ISO WKB geometry type codes and
    /// are documented as part of the Parquet specification:
    /// <https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types>
    geometry_types: HashSet<i32>,
    wraparound_hint: Interval,
}

impl GeometryBounder {
    /// Create a new, empty bounder that represents empty input
    pub fn empty() -> Self {
        Self {
            x_left: Interval::empty(),
            x_mid: Interval::empty(),
            x_right: Interval::empty(),
            y: Interval::empty(),
            z: Interval::empty(),
            m: Interval::empty(),
            geometry_types: HashSet::<i32>::default(),
            wraparound_hint: Interval::empty(),
        }
    }

    /// Set the hint to use for generation of potential wraparound xmin/xmax output
    ///
    /// Usually this value should be set to (-180, 180), as wraparound is primarily
    /// targeted at lon/lat coordinate systems where collections of features with
    /// components at the very far left and very far right of the coordinate system
    /// are actually very close to each other.
    ///
    /// It is safe to set this value even when the actual coordinate system of the
    /// input is unknown: if the input has coordinate values that are outside the
    /// range of the wraparound hint, wraparound xmin/xmax values will not be
    /// generated. If the input has coordinate values that are well inside of the
    /// range of the wraparound hint, the wraparound xmin/xmax value will be
    /// substantially wider than the non-wraparound version and will not be returned.
    pub fn with_wraparound_hint(self, wraparound_hint: impl Into<Interval>) -> Self {
        Self {
            wraparound_hint: wraparound_hint.into(),
            ..self
        }
    }

    /// Calculate the final xmin and xmax for geometries encountered by this bounder
    ///
    /// The interval returned may wraparound if a hint was set and the input
    /// encountered by this bounder were exclusively at the far left and far right
    /// of the input range. See [IntervalTrait] for an in-depth description of
    /// wraparound intervals.
    pub fn x(&self) -> WraparoundInterval {
        let out_all = Interval::empty()
            .merge_interval(&self.x_left)
            .merge_interval(&self.x_mid)
            .merge_interval(&self.x_right);

        // Check if this even makes sense: if anything is covering the midpoint
        // of the wraparound hint or the bounds don't make sense for the provided
        // wraparound hint, just return the Cartesian bounds.
        if !self.x_mid.is_empty() || !self.wraparound_hint.contains_interval(&out_all) {
            return out_all.into();
        }

        // Check if our wraparound bounds are any better than our Cartesian bounds
        // If the Cartesian bounds are tighter, return them.
        let out_width = (self.x_left.hi() - self.wraparound_hint.lo())
            + (self.wraparound_hint.hi() - self.x_right.lo());
        if out_all.width() < out_width {
            return out_all.into();
        }

        // Wraparound!
        WraparoundInterval::new(self.x_right.lo(), self.x_left.hi())
    }

    /// Calculate the final ymin and ymax for geometries encountered by this bounder
    pub fn y(&self) -> Interval {
        self.y
    }

    /// Calculate the final zmin and zmax for geometries encountered by this bounder
    pub fn z(&self) -> Interval {
        self.z
    }

    /// Calculate the final mmin and mmax values for geometries encountered by this bounder
    pub fn m(&self) -> Interval {
        self.m
    }

    /// Calculate the final geometry type set
    ///
    /// Returns a copy of the unique geometry type/dimension combinations encountered
    /// by this bounder. These identifiers are ISO WKB identifiers (e.g., 1001
    /// for PointZ). The output is always returned sorted.
    pub fn geometry_types(&self) -> Vec<i32> {
        let mut out = self.geometry_types.iter().copied().collect::<Vec<_>>();
        out.sort_unstable();
        out
    }

    /// Update this bounder with one WKB-encoded geometry
    ///
    /// Parses and accumulates the bounds of one WKB-encoded geometry. This function
    /// will error for invalid WKB input and for geometry types beyond the seven
    /// basic types supported for bounds. Clients may ignore such an error when
    /// writing statistics.
    pub fn update_wkb(&mut self, wkb: &[u8]) -> Result<(), ArrowError> {
        validate_supported_wkb_geometry_types(wkb)?;
        let wkb = Wkb::try_new(wkb).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        self.update_geometry(&wkb)?;
        Ok(())
    }

    fn update_geometry(&mut self, geom: &impl GeometryTrait<T = f64>) -> Result<(), ArrowError> {
        let geometry_type = geometry_type(geom)?;
        self.geometry_types.insert(geometry_type);

        visit_intervals(geom, 'x', &mut |x| self.update_x(&x))?;
        visit_intervals(geom, 'y', &mut |y| self.y.update_interval(&y))?;
        visit_intervals(geom, 'z', &mut |z| self.z.update_interval(&z))?;
        visit_intervals(geom, 'm', &mut |m| self.m.update_interval(&m))?;

        Ok(())
    }

    fn update_x(&mut self, x: &Interval) {
        if x.hi() < self.wraparound_hint.mid() {
            // If the x interval is completely to the left of the midpoint, merge it
            // with x_left
            self.x_left.update_interval(x);
        } else if x.lo() > self.wraparound_hint.mid() {
            // If the x interval is completely to the right of the midpoint, merge it
            // with x_right
            self.x_right.update_interval(x);
        } else {
            // Otherwise, merge it with x_mid
            self.x_mid.update_interval(x);
        }
    }
}

/// Validate every WKB type header before passing the geometry to `wkb`.
///
/// The `wkb` crate currently identifies geometries using the lowest three bits
/// of the type code. ISO WKB extension types such as Triangle (17) can therefore
/// be parsed as Point (1), producing plausible but incorrect bounds. Walking
/// nested collection members here ensures the same check applies below a
/// GeometryCollection or multi-geometry.
fn validate_supported_wkb_geometry_types(wkb: &[u8]) -> Result<(), ArrowError> {
    type ChildConstraint = (u32, WkbDimension, u8, bool);

    let mut offset = 0;
    let mut pending_geometries = vec![(1_usize, None::<ChildConstraint>)];

    while let Some((remaining, expected_child)) = pending_geometries.last_mut() {
        if *remaining == 0 {
            pending_geometries.pop();
            continue;
        }
        *remaining -= 1;
        let expected_child = *expected_child;

        let byte_order = *wkb.get(offset).ok_or_else(invalid_wkb_header)?;
        offset = offset.checked_add(1).ok_or_else(invalid_wkb_header)?;
        let code_bytes: [u8; 4] = wkb
            .get(offset..offset.checked_add(4).ok_or_else(invalid_wkb_header)?)
            .ok_or_else(invalid_wkb_header)?
            .try_into()
            .map_err(|_| invalid_wkb_header())?;
        offset += 4;

        let code = match byte_order {
            0 => u32::from_be_bytes(code_bytes),
            1 => u32::from_le_bytes(code_bytes),
            _ => return Err(invalid_wkb_header()),
        };

        let ewkb_z = code & 0x8000_0000 != 0;
        let ewkb_m = code & 0x4000_0000 != 0;
        let ewkb_srid = code & 0x2000_0000 != 0;
        let code_without_ewkb_flags = code & 0x1fff_ffff;

        let (base_type, dimension) = if ewkb_z || ewkb_m || ewkb_srid {
            if !(1..=7).contains(&code_without_ewkb_flags) {
                return Err(unsupported_wkb_geometry_type(code));
            }
            let dimension = match (ewkb_z, ewkb_m) {
                (true, true) => WkbDimension::Xyzm,
                (true, false) => WkbDimension::Xyz,
                (false, true) => WkbDimension::Xym,
                (false, false) => WkbDimension::Xy,
            };
            (code_without_ewkb_flags, dimension)
        } else {
            let dimension = match code_without_ewkb_flags {
                1..=7 => WkbDimension::Xy,
                1001..=1007 => WkbDimension::Xyz,
                2001..=2007 => WkbDimension::Xym,
                3001..=3007 => WkbDimension::Xyzm,
                _ => return Err(unsupported_wkb_geometry_type(code)),
            };
            (code_without_ewkb_flags % 1000, dimension)
        };

        let dimensions = match dimension {
            WkbDimension::Xy => 2,
            WkbDimension::Xyz | WkbDimension::Xym => 3,
            WkbDimension::Xyzm => 4,
        };

        if let Some((expected_type, expected_dimensions, expected_byte_order, srid_allowed)) =
            expected_child
            && (base_type != expected_type
                || dimension != expected_dimensions
                || byte_order != expected_byte_order
                || (ewkb_srid && !srid_allowed))
        {
            return Err(invalid_wkb_header());
        }

        if ewkb_srid {
            skip_wkb_bytes(wkb, &mut offset, 4)?;
        }

        match base_type {
            1 => skip_wkb_coordinates(wkb, &mut offset, 1, dimensions)?,
            2 => {
                let point_count = read_wkb_u32(wkb, &mut offset, byte_order)? as usize;
                skip_wkb_coordinates(wkb, &mut offset, point_count, dimensions)?;
            }
            3 => {
                let ring_count = read_wkb_u32(wkb, &mut offset, byte_order)? as usize;
                for _ in 0..ring_count {
                    let point_count = read_wkb_u32(wkb, &mut offset, byte_order)? as usize;
                    skip_wkb_coordinates(wkb, &mut offset, point_count, dimensions)?;
                }
            }
            4..=7 => {
                let geometry_count = read_wkb_u32(wkb, &mut offset, byte_order)? as usize;
                let child_constraint = match base_type {
                    4 => Some((1, dimension, byte_order, false)),
                    5 => Some((2, dimension, byte_order, true)),
                    6 => Some((3, dimension, byte_order, true)),
                    _ => None,
                };
                pending_geometries.push((geometry_count, child_constraint));
            }
            _ => return Err(unsupported_wkb_geometry_type(code)),
        }
    }

    Ok(())
}

fn read_wkb_u32(wkb: &[u8], offset: &mut usize, byte_order: u8) -> Result<u32, ArrowError> {
    let end = offset.checked_add(4).ok_or_else(invalid_wkb_header)?;
    let bytes: [u8; 4] = wkb
        .get(*offset..end)
        .ok_or_else(invalid_wkb_header)?
        .try_into()
        .map_err(|_| invalid_wkb_header())?;
    *offset = end;

    match byte_order {
        0 => Ok(u32::from_be_bytes(bytes)),
        1 => Ok(u32::from_le_bytes(bytes)),
        _ => Err(invalid_wkb_header()),
    }
}

fn skip_wkb_coordinates(
    wkb: &[u8],
    offset: &mut usize,
    point_count: usize,
    dimensions: usize,
) -> Result<(), ArrowError> {
    let byte_count = point_count
        .checked_mul(dimensions)
        .and_then(|count| count.checked_mul(std::mem::size_of::<f64>()))
        .ok_or_else(invalid_wkb_header)?;
    skip_wkb_bytes(wkb, offset, byte_count)
}

fn skip_wkb_bytes(wkb: &[u8], offset: &mut usize, byte_count: usize) -> Result<(), ArrowError> {
    let end = offset
        .checked_add(byte_count)
        .ok_or_else(invalid_wkb_header)?;
    if end > wkb.len() {
        return Err(invalid_wkb_header());
    }
    *offset = end;
    Ok(())
}

fn invalid_wkb_header() -> ArrowError {
    ArrowError::InvalidArgumentError("Invalid WKB geometry header or body".to_string())
}

fn unsupported_wkb_geometry_type(code: u32) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "Unsupported WKB geometry type code {code}; bounds are available only for the seven basic geometry types"
    ))
}

/// Visit contiguous intervals for a given dimension within a [GeometryTrait]
///
/// Here, contiguous intervals refers to intervals that must not be separated
/// by wraparound bounding. Point components of a geometry are visited as
/// degenerate intervals of a single value; linestring or polygon ring components
/// are visited as single intervals.
fn visit_intervals(
    geom: &impl GeometryTrait<T = f64>,
    dimension: char,
    func: &mut impl FnMut(Interval),
) -> Result<(), ArrowError> {
    let Some(n) = dimension_index(geom.dim(), dimension) else {
        return Ok(());
    };

    match geom.as_type() {
        GeometryType::Point(pt) => {
            if let Some(coord) = PointTrait::coord(pt) {
                visit_point(coord, n, func);
            }
        }
        GeometryType::LineString(ls) => {
            visit_sequence(ls.coords(), n, func);
        }
        GeometryType::Polygon(pl) => {
            if let Some(exterior) = pl.exterior() {
                visit_sequence(exterior.coords(), n, func);
            }

            for interior in pl.interiors() {
                visit_sequence(interior.coords(), n, func);
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            visit_collection(multi_pt.points(), dimension, func)?;
        }
        GeometryType::MultiLineString(multi_ls) => {
            visit_collection(multi_ls.line_strings(), dimension, func)?;
        }
        GeometryType::MultiPolygon(multi_pl) => {
            visit_collection(multi_pl.polygons(), dimension, func)?;
        }
        GeometryType::GeometryCollection(collection) => {
            visit_collection(collection.geometries(), dimension, func)?;
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "GeometryType not supported for dimension bounds".to_string(),
            ));
        }
    }

    Ok(())
}

/// Visit a point
///
/// Points can be separated by wraparound bounding even if they occur within
/// the same feature, so we visit them as individual degenerate intervals.
fn visit_point(coord: impl CoordTrait<T = f64>, n: usize, func: &mut impl FnMut(Interval)) {
    let val = unsafe { coord.nth_unchecked(n) };
    func((val, val).into());
}

/// Visit contiguous sequences
///
/// Sequences (e.g., linestrings or polygon rings) must always be considered
/// together (i.e., are never separated by wraparound bounding).
fn visit_sequence(
    coords: impl IntoIterator<Item = impl CoordTrait<T = f64>>,
    n: usize,
    func: &mut impl FnMut(Interval),
) {
    let mut interval = Interval::empty();
    for coord in coords {
        interval.update_value(unsafe { coord.nth_unchecked(n) });
    }

    func(interval);
}

/// Visit intervals in a collection of geometries
fn visit_collection(
    collection: impl IntoIterator<Item = impl GeometryTrait<T = f64>>,
    target: char,
    func: &mut impl FnMut(Interval),
) -> Result<(), ArrowError> {
    for geom in collection {
        visit_intervals(&geom, target, func)?;
    }

    Ok(())
}

/// Extract the geometry type code encountered by the bounder
///
/// The integer code is a ISO WKB geometry type codes is documented as part
/// of the Parquet specification:
/// <https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types>
///
/// This can also be derived from bytes 2-5 (possibly endian-swapped according to byte 1)
/// of the input WKB buffer but is slightly clearer recomputed.
fn geometry_type(geom: &impl GeometryTrait<T = f64>) -> Result<i32, ArrowError> {
    let dimension_type = match geom.dim() {
        Dimensions::Xy => 0,
        Dimensions::Xyz => 1000,
        Dimensions::Xym => 2000,
        Dimensions::Xyzm => 3000,
        Dimensions::Unknown(_) => {
            return Err(ArrowError::InvalidArgumentError(
                "Unsupported dimensions".to_string(),
            ));
        }
    };

    let geometry_type = match geom.as_type() {
        GeometryType::Point(_) => 1,
        GeometryType::LineString(_) => 2,
        GeometryType::Polygon(_) => 3,
        GeometryType::MultiPoint(_) => 4,
        GeometryType::MultiLineString(_) => 5,
        GeometryType::MultiPolygon(_) => 6,
        GeometryType::GeometryCollection(_) => 7,
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "GeometryType not supported for dimension bounds".to_string(),
            ));
        }
    };

    Ok(dimension_type + geometry_type)
}

fn dimension_index(dim: Dimensions, target: char) -> Option<usize> {
    match target {
        'x' => return Some(0),
        'y' => return Some(1),
        _ => {}
    }

    match (dim, target) {
        (Dimensions::Xyz, 'z') => Some(2),
        (Dimensions::Xym, 'm') => Some(2),
        (Dimensions::Xyzm, 'z') => Some(2),
        (Dimensions::Xyzm, 'm') => Some(3),
        (_, _) => None,
    }
}

#[cfg(test)]
mod test {

    use std::str::FromStr;

    use wkt::Wkt;

    use super::*;

    fn wkt_bounds(
        wkt_values: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<GeometryBounder, ArrowError> {
        wkt_bounds_with_wraparound(wkt_values, Interval::empty())
    }

    fn wkt_bounds_with_wraparound(
        wkt_values: impl IntoIterator<Item = impl AsRef<str>>,
        wraparound: impl Into<Interval>,
    ) -> Result<GeometryBounder, ArrowError> {
        let mut bounder = GeometryBounder::empty().with_wraparound_hint(wraparound);
        for wkt_value in wkt_values {
            let wkt: Wkt = Wkt::from_str(wkt_value.as_ref())
                .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
            bounder.update_geometry(&wkt)?;
        }
        Ok(bounder)
    }

    #[test]
    fn test_wkb() {
        let wkt: Wkt = Wkt::from_str("LINESTRING (0 1, 2 3)").unwrap();
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &wkt, &Default::default()).unwrap();

        let mut bounds = GeometryBounder::empty();
        bounds.update_wkb(&wkb).unwrap();

        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());

        let wkt: Wkt = Wkt::from_str("GEOMETRYCOLLECTION (POINT (0 1), POINT (2 3))").unwrap();
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &wkt, &Default::default()).unwrap();

        let mut bounds = GeometryBounder::empty();
        bounds.update_wkb(&wkb).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());
    }

    #[test]
    fn test_update_wkb_rejects_big_endian_triangle_type() {
        let mut wkb = vec![0];
        wkb.extend_from_slice(&1017_u32.to_be_bytes());
        wkb.extend_from_slice(&1_u32.to_be_bytes());
        wkb.extend_from_slice(&4_u32.to_be_bytes());
        for coordinate in [
            (10.0_f64, 20.0_f64, 30.0_f64),
            (11.0, 20.0, 30.0),
            (10.0, 21.0, 30.0),
            (10.0, 20.0, 30.0),
        ] {
            for value in [coordinate.0, coordinate.1, coordinate.2] {
                wkb.extend_from_slice(&value.to_be_bytes());
            }
        }

        assert!(GeometryBounder::empty().update_wkb(&wkb).is_err());
    }

    #[test]
    fn test_update_wkb_accepts_ewkb_with_z_m_and_srid_flags() {
        let mut wkb = vec![1];
        wkb.extend_from_slice(&0xe000_0001_u32.to_le_bytes());
        wkb.extend_from_slice(&4326_u32.to_le_bytes());
        for value in [1.0_f64, 2.0, 3.0, 4.0] {
            wkb.extend_from_slice(&value.to_le_bytes());
        }

        let mut bounds = GeometryBounder::empty();
        bounds.update_wkb(&wkb).unwrap();

        assert_eq!(bounds.geometry_types(), vec![3001]);
        assert_eq!(bounds.x(), (1, 1).into());
        assert_eq!(bounds.y(), (2, 2).into());
        assert_eq!(bounds.z(), (3, 3).into());
        assert_eq!(bounds.m(), (4, 4).into());
    }

    #[test]
    fn test_update_wkb_rejects_unsupported_iso_geometry_types() {
        let polygon_z = || {
            let mut wkb = Vec::new();
            wkb.push(1);
            wkb.extend_from_slice(&1003_u32.to_le_bytes());
            wkb.extend_from_slice(&1_u32.to_le_bytes()); // exterior ring
            wkb.extend_from_slice(&4_u32.to_le_bytes()); // coordinate count
            for coordinate in [
                (0.0_f64, 0.0_f64, 0.0_f64),
                (1.0, 0.0, 0.0),
                (0.0, 1.0, 0.0),
                (0.0, 0.0, 0.0),
            ] {
                for value in [coordinate.0, coordinate.1, coordinate.2] {
                    wkb.extend_from_slice(&value.to_le_bytes());
                }
            }
            wkb
        };

        let mut polyhedral_surface_z = Vec::new();
        polyhedral_surface_z.push(1);
        polyhedral_surface_z.extend_from_slice(&1015_u32.to_le_bytes());
        polyhedral_surface_z.extend_from_slice(&1_u32.to_le_bytes());
        polyhedral_surface_z.extend_from_slice(&polygon_z());

        let mut triangle_z = Vec::new();
        triangle_z.push(1);
        triangle_z.extend_from_slice(&1017_u32.to_le_bytes());
        triangle_z.extend_from_slice(&1_u32.to_le_bytes()); // ring count
        triangle_z.extend_from_slice(&4_u32.to_le_bytes()); // coordinate count
        for coordinate in [
            (10.0_f64, 20.0_f64, 30.0_f64),
            (11.0, 20.0, 30.0),
            (10.0, 21.0, 30.0),
            (10.0, 20.0, 30.0),
        ] {
            for value in [coordinate.0, coordinate.1, coordinate.2] {
                triangle_z.extend_from_slice(&value.to_le_bytes());
            }
        }

        let mut tin_z = Vec::new();
        tin_z.push(1);
        tin_z.extend_from_slice(&1016_u32.to_le_bytes());

        for (name, wkb) in [
            ("PolyhedralSurface Z", polyhedral_surface_z),
            ("Triangle Z", triangle_z),
            ("TIN Z", tin_z),
        ] {
            assert!(
                GeometryBounder::empty().update_wkb(&wkb).is_err(),
                "{name} must not be interpreted as one of the seven basic WKB types"
            );
        }
    }

    #[test]
    fn test_update_wkb_rejects_extension_type_nested_in_collection() {
        let mut triangle_z = vec![1];
        triangle_z.extend_from_slice(&1017_u32.to_le_bytes());
        triangle_z.extend_from_slice(&1_u32.to_le_bytes());
        triangle_z.extend_from_slice(&4_u32.to_le_bytes());
        for coordinate in [
            (10.0_f64, 20.0_f64, 30.0_f64),
            (11.0, 20.0, 30.0),
            (10.0, 21.0, 30.0),
            (10.0, 20.0, 30.0),
        ] {
            for value in [coordinate.0, coordinate.1, coordinate.2] {
                triangle_z.extend_from_slice(&value.to_le_bytes());
            }
        }

        let mut collection_z = vec![1];
        collection_z.extend_from_slice(&1007_u32.to_le_bytes());
        collection_z.extend_from_slice(&1_u32.to_le_bytes());
        collection_z.extend_from_slice(&triangle_z);

        assert!(GeometryBounder::empty().update_wkb(&collection_z).is_err());
    }

    #[test]
    fn test_update_wkb_rejects_multipoint_with_srid_child() {
        let mut wkb = vec![1];
        wkb.extend_from_slice(&4_u32.to_le_bytes()); // MultiPoint
        wkb.extend_from_slice(&1_u32.to_le_bytes()); // one Point
        wkb.push(1);
        wkb.extend_from_slice(&0x2000_0001_u32.to_le_bytes()); // Point with SRID
        wkb.extend_from_slice(&4326_u32.to_le_bytes());
        wkb.extend_from_slice(&1.0_f64.to_le_bytes());
        wkb.extend_from_slice(&2.0_f64.to_le_bytes());

        assert!(GeometryBounder::empty().update_wkb(&wkb).is_err());
    }

    #[test]
    fn test_update_wkb_rejects_wrong_multipoint_child_type() {
        let mut wkb = vec![1];
        wkb.extend_from_slice(&4_u32.to_le_bytes()); // MultiPoint
        wkb.extend_from_slice(&1_u32.to_le_bytes()); // one member
        wkb.push(1);
        wkb.extend_from_slice(&2_u32.to_le_bytes()); // LineString, not Point
        wkb.extend_from_slice(&1_u32.to_le_bytes());
        wkb.extend_from_slice(&10.0_f64.to_le_bytes());
        wkb.extend_from_slice(&20.0_f64.to_le_bytes());

        assert!(GeometryBounder::empty().update_wkb(&wkb).is_err());
    }

    #[test]
    fn test_update_wkb_rejects_z_multipoint_with_m_point_child() {
        let mut wkb = vec![1];
        wkb.extend_from_slice(&1004_u32.to_le_bytes()); // MultiPoint Z
        wkb.extend_from_slice(&1_u32.to_le_bytes());
        wkb.push(1);
        wkb.extend_from_slice(&2001_u32.to_le_bytes()); // Point M
        for value in [10.0_f64, 20.0, 30.0] {
            wkb.extend_from_slice(&value.to_le_bytes());
        }

        assert!(GeometryBounder::empty().update_wkb(&wkb).is_err());
    }

    #[test]
    fn test_geometry_types() {
        let empties = [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ];

        assert_eq!(
            wkt_bounds(empties).unwrap().geometry_types(),
            vec![1, 2, 3, 4, 5, 6, 7]
        );

        let empties_z = [
            "POINT Z EMPTY",
            "LINESTRING Z EMPTY",
            "POLYGON Z EMPTY",
            "MULTIPOINT Z EMPTY",
            "MULTILINESTRING Z EMPTY",
            "MULTIPOLYGON Z EMPTY",
            "GEOMETRYCOLLECTION Z EMPTY",
        ];

        assert_eq!(
            wkt_bounds(empties_z).unwrap().geometry_types(),
            vec![1001, 1002, 1003, 1004, 1005, 1006, 1007]
        );

        let empties_m = [
            "POINT M EMPTY",
            "LINESTRING M EMPTY",
            "POLYGON M EMPTY",
            "MULTIPOINT M EMPTY",
            "MULTILINESTRING M EMPTY",
            "MULTIPOLYGON M EMPTY",
            "GEOMETRYCOLLECTION M EMPTY",
        ];

        assert_eq!(
            wkt_bounds(empties_m).unwrap().geometry_types(),
            vec![2001, 2002, 2003, 2004, 2005, 2006, 2007]
        );

        let empties_zm = [
            "POINT ZM EMPTY",
            "LINESTRING ZM EMPTY",
            "POLYGON ZM EMPTY",
            "MULTIPOINT ZM EMPTY",
            "MULTILINESTRING ZM EMPTY",
            "MULTIPOLYGON ZM EMPTY",
            "GEOMETRYCOLLECTION ZM EMPTY",
        ];

        assert_eq!(
            wkt_bounds(empties_zm).unwrap().geometry_types(),
            vec![3001, 3002, 3003, 3004, 3005, 3006, 3007]
        );
    }

    #[test]
    fn test_bounds_empty() {
        let empties = [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ];

        let bounds = wkt_bounds(empties).unwrap();
        assert!(bounds.x().is_empty());
        assert!(bounds.y().is_empty());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());

        // With wraparound, still empty
        let bounds = wkt_bounds_with_wraparound(empties, (-180, 180)).unwrap();
        assert!(bounds.x().is_empty());
        assert!(bounds.y().is_empty());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());
    }

    #[test]
    fn test_bounds_coord() {
        let bounds = wkt_bounds(["POINT (0 1)", "POINT (2 3)"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());

        let bounds = wkt_bounds(["POINT Z (0 1 2)", "POINT Z (3 4 5)"]).unwrap();
        assert_eq!(bounds.x(), (0, 3).into());
        assert_eq!(bounds.y(), (1, 4).into());
        assert_eq!(bounds.z(), (2, 5).into());
        assert!(bounds.m().is_empty());

        let bounds = wkt_bounds(["POINT M (0 1 2)", "POINT M (3 4 5)"]).unwrap();
        assert_eq!(bounds.x(), (0, 3).into());
        assert_eq!(bounds.y(), (1, 4).into());
        assert!(bounds.z().is_empty());
        assert_eq!(bounds.m(), (2, 5).into());

        let bounds = wkt_bounds(["POINT ZM (0 1 2 3)", "POINT ZM (4 5 6 7)"]).unwrap();
        assert_eq!(bounds.x(), (0, 4).into());
        assert_eq!(bounds.y(), (1, 5).into());
        assert_eq!(bounds.z(), (2, 6).into());
        assert_eq!(bounds.m(), (3, 7).into());
    }

    #[test]
    fn test_bounds_sequence() {
        let bounds = wkt_bounds(["LINESTRING (0 1, 2 3)"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());

        let bounds = wkt_bounds(["LINESTRING Z (0 1 2, 3 4 5)"]).unwrap();
        assert_eq!(bounds.x(), (0, 3).into());
        assert_eq!(bounds.y(), (1, 4).into());
        assert_eq!(bounds.z(), (2, 5).into());
        assert!(bounds.m().is_empty());

        let bounds = wkt_bounds(["LINESTRING M (0 1 2, 3 4 5)"]).unwrap();
        assert_eq!(bounds.x(), (0, 3).into());
        assert_eq!(bounds.y(), (1, 4).into());
        assert!(bounds.z().is_empty());
        assert_eq!(bounds.m(), (2, 5).into());

        let bounds = wkt_bounds(["LINESTRING ZM (0 1 2 3, 4 5 6 7)"]).unwrap();
        assert_eq!(bounds.x(), (0, 4).into());
        assert_eq!(bounds.y(), (1, 5).into());
        assert_eq!(bounds.z(), (2, 6).into());
        assert_eq!(bounds.m(), (3, 7).into());
    }

    #[test]
    fn test_bounds_geometry_type() {
        let bounds = wkt_bounds(["POINT (0 1)", "POINT (2 3)"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());

        let bounds = wkt_bounds(["LINESTRING (0 1, 2 3)"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());

        // Normally interiors are supposed to be inside the exterior; however, we
        // include a poorly formed polygon just to make sure they are considered
        let bounds =
            wkt_bounds(["POLYGON ((0 0, 0 1, 1 0, 0 0), (10 10, 10 11, 11 10, 10 10))"]).unwrap();
        assert_eq!(bounds.x(), (0, 11).into());
        assert_eq!(bounds.y(), (0, 11).into());

        let bounds = wkt_bounds(["MULTIPOINT ((0 1), (2 3))"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());

        let bounds = wkt_bounds(["MULTILINESTRING ((0 1, 2 3))"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());

        let bounds = wkt_bounds(["MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))"]).unwrap();
        assert_eq!(bounds.x(), (0, 1).into());
        assert_eq!(bounds.y(), (0, 1).into());

        let bounds = wkt_bounds(["GEOMETRYCOLLECTION (POINT (0 1), POINT (2 3))"]).unwrap();
        assert_eq!(bounds.x(), (0, 2).into());
        assert_eq!(bounds.y(), (1, 3).into());
    }

    #[test]
    fn test_bounds_wrap_basic() {
        let geoms = ["POINT (-170 0)", "POINT (170 0)"];

        // No wraparound because it was disabled
        let bounds = wkt_bounds_with_wraparound(geoms, Interval::empty()).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound that can't happen because something is covering
        // the midpoint.
        let mut geoms_with_mid = geoms.to_vec();
        geoms_with_mid.push("LINESTRING (-10 0, 10 0)");
        let bounds = wkt_bounds_with_wraparound(geoms_with_mid, (-180, 180)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box is *not* better
        let bounds = wkt_bounds_with_wraparound(geoms, (-1000, 1000)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box is inappropriate because it is
        // outside the wrap hint
        let bounds = wkt_bounds_with_wraparound(geoms, (-10, 10)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box *is* better
        let bounds = wkt_bounds_with_wraparound(geoms, (-180, 180)).unwrap();
        assert_eq!(bounds.x(), (170, -170).into());

        // The Cartesian bounds are tighter than the wraparound bounds.
        let geoms = [
            "POINT (-10 0)",
            "POINT (-2 0)",
            "POINT (170 0)",
            "POINT (175 0)",
        ];
        let bounds = wkt_bounds_with_wraparound(geoms, (-180, 180)).unwrap();
        assert_eq!(bounds.x(), (-10, 175).into());
    }

    #[test]
    fn test_bounds_wrap_multipart() {
        let fiji = "MULTIPOLYGON (
        ((-180 -15.51, -180 -19.78, -178.61 -21.14, -178.02 -18.22, -178.57 -16.04, -180 -15.51)),
        ((180 -15.51, 177.98 -16.25, 176.67 -17.14, 177.83 -19.31, 180 -19.78, 180 -15.51))
        )";

        let bounds = wkt_bounds_with_wraparound([fiji], (-180, 180)).unwrap();
        assert!(bounds.x().is_wraparound());
        assert_eq!(bounds.x(), (176.67, -178.02).into());
        assert_eq!(bounds.y(), (-21.14, -15.51).into());
    }
}
