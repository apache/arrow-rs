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
        // TODO: geometry type
        update_dimension_bounds(geom, "x", &mut |x| self.update_x(x))?;
        update_dimension_bounds(geom, "y", &mut |y| self.update_y(y))?;
        update_dimension_bounds(geom, "z", &mut |z| self.update_z(z))?;
        update_dimension_bounds(geom, "m", &mut |m| self.update_m(m))?;
        Ok(())
    }

    pub fn update_x(&mut self, x: impl Into<Interval>) {
        let x: Interval = x.into();
        if x.hi() < self.wraparound_hint.mid() {
            self.x_left.update_interval(&x);
        } else if x.lo() > self.wraparound_hint.mid() {
            self.x_right.update_interval(&x);
        } else {
            self.x_mid.update_interval(&x);
        }
    }

    pub fn update_y(&mut self, y: impl Into<Interval>) {
        self.y.update_interval(&y.into());
    }

    pub fn update_z(&mut self, z: impl Into<Interval>) {
        self.z.update_interval(&z.into());
    }

    pub fn update_m(&mut self, m: impl Into<Interval>) {
        self.m.update_interval(&m.into());
    }

    pub fn update_geometry_types(&mut self, geometry_type: i32) {
        self.geometry_types.insert(geometry_type);
    }
}

fn update_dimension_bounds(
    geom: &impl GeometryTrait<T = f64>,
    target: &str,
    updater: &mut impl FnMut(Interval),
) -> Result<(), ArrowError> {
    let n = if let Some(n) = dimension_index(geom.dim(), target) {
        n
    } else {
        return Ok(());
    };

    match geom.as_type() {
        GeometryType::Point(pt) => {
            if let Some(coord) = PointTrait::coord(pt) {
                update_coord(updater, coord, n);
            }
        }
        GeometryType::LineString(ls) => {
            update_sequence(updater, ls.coords(), n);
        }
        GeometryType::Polygon(pl) => {
            if let Some(exterior) = pl.exterior() {
                update_sequence(&mut *updater, exterior.coords(), n);
            }

            for interior in pl.interiors() {
                update_sequence(&mut *updater, interior.coords(), n);
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            update_collection(updater, multi_pt.points(), target)?;
        }
        GeometryType::MultiLineString(multi_ls) => {
            update_collection(updater, multi_ls.line_strings(), target)?;
        }
        GeometryType::MultiPolygon(multi_pl) => {
            update_collection(updater, multi_pl.polygons(), target)?;
        }
        GeometryType::GeometryCollection(collection) => {
            update_collection(updater, collection.geometries(), target)?;
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "GeometryType not supported for dimension bounds".to_string(),
            ))
        }
    }

    Ok(())
}

fn update_coord(updater: &mut impl FnMut(Interval), coord: impl CoordTrait<T = f64>, n: usize) {
    let val = unsafe { coord.nth_unchecked(n) };
    updater((val, val).into());
}

fn update_sequence(
    updater: &mut impl FnMut(Interval),
    coords: impl IntoIterator<Item = impl CoordTrait<T = f64>>,
    n: usize,
) {
    let mut interval = Interval::empty();
    for coord in coords {
        interval.update_value(unsafe { coord.nth_unchecked(n) });
    }

    updater(interval);
}

fn update_collection(
    updater: &mut impl FnMut(Interval),
    collection: impl IntoIterator<Item = impl GeometryTrait<T = f64>>,
    target: &str,
) -> Result<(), ArrowError> {
    for geom in collection {
        update_dimension_bounds(&geom, target, updater)?;
    }

    Ok(())
}

fn dimension_index(dim: Dimensions, target: &str) -> Option<usize> {
    match target {
        "x" => return Some(0),
        "y" => return Some(1),
        _ => {}
    }

    match (dim, target) {
        (geo_traits::Dimensions::Xyz, "z") => Some(2),
        (geo_traits::Dimensions::Xym, "m") => Some(2),
        (geo_traits::Dimensions::Xyzm, "z") => Some(2),
        (geo_traits::Dimensions::Xyzm, "m") => Some(3),
        (_, _) => None,
    }
}

#[cfg(test)]
mod test {

    use std::str::FromStr;

    use wkt::Wkt;

    use super::*;

    pub fn wkt_bounds(
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
    pub fn test_bounds_empty() {
        let empties = [
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ];

        let bounds = wkt_bounds(empties, Interval::empty()).unwrap();
        assert!(bounds.geometry_types().is_empty());
        assert!(bounds.x().is_empty());
        assert!(bounds.y().is_empty());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());

        // With wraparound, still empty
        let bounds = wkt_bounds(empties, (-180, 180)).unwrap();
        assert!(bounds.geometry_types().is_empty());
        assert!(bounds.x().is_empty());
        assert!(bounds.y().is_empty());
        assert!(bounds.z().is_empty());
        assert!(bounds.m().is_empty());
    }

    #[test]
    pub fn test_bounds_wrap_basic() {
        let geoms = ["POINT (-170 0)", "POINT (170 0)"];

        // No wraparound
        let bounds = wkt_bounds(geoms, Interval::empty()).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound that can't happen because something is covering
        // the midpoint.
        let mut geoms_with_mid = geoms.to_vec();
        geoms_with_mid.push("LINESTRING (-10 0, 10 0)");
        let bounds = wkt_bounds(geoms_with_mid, (-180, 180)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box is *not* better
        let bounds = wkt_bounds(geoms, (-1000, 1000)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box is inappropriate because it is
        // outside the wrap hint
        let bounds = wkt_bounds(geoms, (-10, 10)).unwrap();
        assert_eq!(bounds.x(), (-170, 170).into());

        // Wraparound where the wrapped box *is* better
        let bounds = wkt_bounds(geoms, (-180, 180)).unwrap();
        assert_eq!(bounds.x(), (170, -170).into());
    }
}
