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

use crate::interval::{Interval, IntervalTrait, WraparoundInterval};

#[derive(Debug)]
pub struct Bounder {
    x_left: Interval,
    x_mid: Interval,
    x_right: Interval,
    y: Interval,
    z: Interval,
    m: Interval,
    geometry_types: HashSet<i32>,
    wraparound_hint: Interval,
}

impl Bounder {
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

    pub fn with_wraparound_hint(self, wraparound_hint: impl Into<Interval>) -> Self {
        Self {
            wraparound_hint: wraparound_hint.into(),
            ..self
        }
    }

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
            + (self.wraparound_hint.hi() - self.x_right.hi());
        if out_all.width() < out_width {
            return out_all.into();
        }

        // Wraparound!
        WraparoundInterval::new(self.x_right.lo(), self.x_left.hi())
    }

    pub fn y(&self) -> Interval {
        self.y
    }

    pub fn z(&self) -> Interval {
        self.z
    }

    pub fn m(&self) -> Interval {
        self.m
    }

    pub fn geometry_types(&self) -> Vec<i32> {
        let mut out = self.geometry_types.iter().cloned().collect::<Vec<_>>();
        out.sort();
        out
    }

    pub fn update(&mut self, geom: &impl GeometryTrait<T = f64>) -> Result<(), ArrowError> {
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
            self.x_left.update_interval(x);
        } else if x.lo() > self.wraparound_hint.mid() {
            self.x_right.update_interval(x);
        } else {
            self.x_mid.update_interval(x);
        }
    }

    pub fn update_geometry_types(&mut self, geometry_type: i32) {
        self.geometry_types.insert(geometry_type);
    }
}

fn visit_intervals(
    geom: &impl GeometryTrait<T = f64>,
    target: char,
    func: &mut impl FnMut(Interval),
) -> Result<(), ArrowError> {
    let n = if let Some(n) = dimension_index(geom.dim(), target) {
        n
    } else {
        return Ok(());
    };

    match geom.as_type() {
        GeometryType::Point(pt) => {
            if let Some(coord) = PointTrait::coord(pt) {
                visit_coord(coord, n, func);
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
            visit_collection(multi_pt.points(), target, func)?;
        }
        GeometryType::MultiLineString(multi_ls) => {
            visit_collection(multi_ls.line_strings(), target, func)?;
        }
        GeometryType::MultiPolygon(multi_pl) => {
            visit_collection(multi_pl.polygons(), target, func)?;
        }
        GeometryType::GeometryCollection(collection) => {
            visit_collection(collection.geometries(), target, func)?;
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "GeometryType not supported for dimension bounds".to_string(),
            ))
        }
    }

    Ok(())
}

fn visit_coord(coord: impl CoordTrait<T = f64>, n: usize, func: &mut impl FnMut(Interval)) {
    let val = unsafe { coord.nth_unchecked(n) };
    func((val, val).into());
}

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

fn geometry_type(geom: &impl GeometryTrait<T = f64>) -> Result<i32, ArrowError> {
    let dimension_type = match geom.dim() {
        Dimensions::Xy => 0,
        Dimensions::Xyz => 1000,
        Dimensions::Xym => 2000,
        Dimensions::Xyzm => 3000,
        Dimensions::Unknown(_) => {
            return Err(ArrowError::InvalidArgumentError(
                "Unsupported dimensions".to_string(),
            ))
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
            ))
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
        (geo_traits::Dimensions::Xyz, 'z') => Some(2),
        (geo_traits::Dimensions::Xym, 'm') => Some(2),
        (geo_traits::Dimensions::Xyzm, 'z') => Some(2),
        (geo_traits::Dimensions::Xyzm, 'm') => Some(3),
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
    ) -> Result<Bounder, ArrowError> {
        wkt_bounds_with_wraparound(wkt_values, Interval::empty())
    }

    fn wkt_bounds_with_wraparound(
        wkt_values: impl IntoIterator<Item = impl AsRef<str>>,
        wraparound: impl Into<Interval>,
    ) -> Result<Bounder, ArrowError> {
        let mut bounder = Bounder::empty().with_wraparound_hint(wraparound);
        for wkt_value in wkt_values {
            let wkt: Wkt = Wkt::from_str(wkt_value.as_ref())
                .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
            bounder.update(&wkt)?;
        }
        Ok(bounder)
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
