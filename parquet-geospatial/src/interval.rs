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

use arrow_schema::ArrowError;

/// Generic 1D intervals with wraparound support
///
/// This trait specifies common behaviour implemented by the [Interval] and
/// [WraparoundInterval].
///
/// Briefly, "wraparound" support was included in the Parquet specification
/// to ensure that geometries or geographies with components on both sides of
/// antimeridian (180 degrees longitude) can be reasonably summarized. This
/// concept was borrowed from the widely used GeoJSON and matches identical
/// bounding box specifications for GeoParquet and STAC, among others.
///
/// Because the Parquet specification also states that longitude values
/// are always stored as `x` values (i.e., the first coordinate component),
/// this contingency is only available for the `xmin`/`xmax` component of the
/// GeoStatistics. Thus, the `xmin`/`xmax` pair may either represent a regular
/// interval (specified by xmin = 10 and xmax = 20):
///
/// ```text
///           10         20
///            |==========|
/// ```
///
/// ...or a "wraparound" interval (specified by xmin = 20 and xmax = 10). This
/// interval is the union of the two regular intervals (-Inf, 10] and (20, Inf).
/// Infinity was chosen rather than any particular value to ensure that Parquet
/// implementations did not have to consider the value of the coordinate
/// reference system when comparing intervals.
///
/// ```text
///           10         20
/// <==========|          |============>
/// ```
///
/// In general, one should use [Interval] unless specifically working with
/// wraparound, as the contingency of wraparound incurs overhead (particularly
/// in a loop). This trait is mostly used to simplify testing and unify
/// documentation for the two concrete implementations.
pub trait IntervalTrait: std::fmt::Debug + PartialEq {
    /// Create an interval from lo and hi values
    fn new(lo: f64, hi: f64) -> Self;

    /// Create an empty interval that intersects nothing (except the full interval)
    fn empty() -> Self;

    /// Create the full interval (that intersects everything, including the empty interval)
    fn full() -> Self;

    /// Lower bound
    ///
    /// If `is_wraparound()` returns false, this is also the minimum value. When empty,
    /// this value is Infinity; when full, this value is -Infinity.
    fn lo(&self) -> f64;

    /// Upper bound
    ///
    /// If `is_wraparound()` returns false, this is also the maximum value. When empty,
    /// this value is -Infinity; when full, this value is Infinity.
    fn hi(&self) -> f64;

    /// Check for wraparound
    ///
    /// If `is_wraparound()` returns false, this interval represents the values that are
    /// between lo and hi. If `is_wraparound()` returns true, this interval represents
    /// the values that are *not* between lo and hi.
    ///
    /// It is recommended to work directly with an [Interval] where this is guaranteed to
    /// return false unless wraparound support is specifically required.
    fn is_wraparound(&self) -> bool;

    /// Check for potential intersection with a value
    ///
    /// Note that intervals always contain their endpoints (for both the wraparound and
    /// non-wraparound case).
    fn intersects_value(&self, value: f64) -> bool;

    /// Check for potential intersection with an interval
    ///
    /// Note that intervals always contain their endpoints (for both the wraparound and
    /// non-wraparound case).
    ///
    /// This method accepts Self for performance reasons to prevent unnecessary checking of
    /// `is_wraparound()` when not required for an implementation.
    fn intersects_interval(&self, other: &Self) -> bool;

    /// Check for potential containment of an interval
    ///
    /// Note that intervals always contain their endpoints (for both the wraparound and
    /// non-wraparound case).
    ///
    /// This method accepts Self for performance reasons to prevent unnecessary checking of
    /// `is_wraparound()` when not required for an implementation.
    fn contains_interval(&self, other: &Self) -> bool;

    /// The width of the interval
    ///
    /// For the non-wraparound case, this is the distance between lo and hi. For the wraparound
    /// case, this is infinity.
    fn width(&self) -> f64;

    /// The midpoint of the interval
    ///
    /// For the non-wraparound case, this is the point exactly between lo and hi. For the wraparound
    /// case, this is arbitrarily chosen as infinity (to preserve the property that intervals intersect
    /// their midpoint).
    fn mid(&self) -> f64;

    /// True if this interval is empty (i.e. intersects no values)
    fn is_empty(&self) -> bool;

    /// Compute a new interval that is the union of both
    ///
    /// When accumulating intervals in a loop, use [Interval::update_interval].
    fn merge_interval(&self, other: &Self) -> Self;

    /// Compute a new interval that is the union of both
    ///
    /// When accumulating intervals in a loop, use [Interval::update_value].
    fn merge_value(&self, other: f64) -> Self;

    /// Expand this interval by a given distance
    ///
    /// Returns a new interval where both endpoints are moved outward by the given distance.
    /// For regular intervals, this expands both lo and hi by the distance.
    /// For wraparound intervals, this may result in the full interval if expansion is large enough.
    fn expand_by(&self, distance: f64) -> Self;
}

/// 1D Interval that never wraps around
///
/// Represents a minimum and maximum value without wraparound logic (see [WraparoundInterval]
/// for a wraparound implementation).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    /// Lower bound
    lo: f64,

    /// Upper bound
    hi: f64,
}

impl Interval {
    /// Expand this interval to the union of self and other in place
    ///
    /// Note that NaN values are ignored when updating bounds.
    pub fn update_interval(&mut self, other: &Self) {
        self.lo = self.lo.min(other.lo);
        self.hi = self.hi.max(other.hi);
    }

    /// Expand this interval to the union of self and other in place
    ///
    /// Note that NaN values are ignored when updating bounds.
    pub fn update_value(&mut self, other: f64) {
        self.lo = self.lo.min(other);
        self.hi = self.hi.max(other);
    }
}

impl From<(f64, f64)> for Interval {
    fn from(value: (f64, f64)) -> Self {
        Interval::new(value.0, value.1)
    }
}

impl From<(i32, i32)> for Interval {
    fn from(value: (i32, i32)) -> Self {
        Interval::new(value.0 as f64, value.1 as f64)
    }
}

impl TryFrom<WraparoundInterval> for Interval {
    type Error = ArrowError;

    fn try_from(value: WraparoundInterval) -> Result<Self, Self::Error> {
        if value.is_wraparound() {
            Err(ArrowError::InvalidArgumentError(format!(
                "Can't convert wraparound interval {value:?} to Interval"
            )))
        } else {
            Ok(Interval::new(value.lo(), value.hi()))
        }
    }
}

impl IntervalTrait for Interval {
    fn new(lo: f64, hi: f64) -> Self {
        Self { lo, hi }
    }

    fn empty() -> Self {
        Self {
            lo: f64::INFINITY,
            hi: -f64::INFINITY,
        }
    }

    fn full() -> Self {
        Self {
            lo: -f64::INFINITY,
            hi: f64::INFINITY,
        }
    }

    fn lo(&self) -> f64 {
        self.lo
    }

    fn hi(&self) -> f64 {
        self.hi
    }

    fn is_wraparound(&self) -> bool {
        false
    }

    fn intersects_value(&self, value: f64) -> bool {
        value >= self.lo && value <= self.hi
    }

    fn intersects_interval(&self, other: &Self) -> bool {
        self.lo <= other.hi && other.lo <= self.hi
    }

    fn contains_interval(&self, other: &Self) -> bool {
        self.lo <= other.lo && self.hi >= other.hi
    }

    fn width(&self) -> f64 {
        self.hi - self.lo
    }

    fn mid(&self) -> f64 {
        self.lo + self.width() / 2.0
    }

    fn is_empty(&self) -> bool {
        self.width() == -f64::INFINITY
    }

    fn merge_interval(&self, other: &Self) -> Self {
        let mut out = *self;
        out.update_interval(other);
        out
    }

    fn merge_value(&self, other: f64) -> Self {
        let mut out = *self;
        out.update_value(other);
        out
    }

    fn expand_by(&self, distance: f64) -> Self {
        if self.is_empty() || distance.is_nan() || distance < 0.0 {
            return *self;
        }

        Self::new(self.lo - distance, self.hi + distance)
    }
}

/// 1D Interval that may or may not wrap around
///
/// Concrete implementation that handles both the wraparound and regular
/// interval case. This is separated from the [Interval] because the
/// [Interval] is faster and most operations will use it directly (invoking
/// this struct when it is specifically required).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WraparoundInterval {
    inner: Interval,
}

impl WraparoundInterval {
    /// Splits this interval into exactly two non-wraparound intervals
    ///
    /// If this interval does not wrap around, one of these intervals will
    /// be empty.
    fn split(&self) -> (Interval, Interval) {
        if self.is_wraparound() {
            (
                Interval {
                    lo: -f64::INFINITY,
                    hi: self.inner.hi,
                },
                Interval {
                    lo: self.inner.lo,
                    hi: f64::INFINITY,
                },
            )
        } else {
            (self.inner, Interval::empty())
        }
    }
}

impl From<(f64, f64)> for WraparoundInterval {
    fn from(value: (f64, f64)) -> Self {
        WraparoundInterval::new(value.0, value.1)
    }
}

impl From<(i32, i32)> for WraparoundInterval {
    fn from(value: (i32, i32)) -> Self {
        WraparoundInterval::new(value.0 as f64, value.1 as f64)
    }
}

impl From<Interval> for WraparoundInterval {
    fn from(value: Interval) -> Self {
        WraparoundInterval::new(value.lo(), value.hi())
    }
}

impl IntervalTrait for WraparoundInterval {
    fn new(lo: f64, hi: f64) -> Self {
        Self {
            inner: Interval::new(lo, hi),
        }
    }

    fn empty() -> Self {
        Self {
            inner: Interval::empty(),
        }
    }

    fn full() -> Self {
        Self {
            inner: Interval::full(),
        }
    }

    fn lo(&self) -> f64 {
        self.inner.lo
    }

    fn hi(&self) -> f64 {
        self.inner.hi
    }

    fn is_wraparound(&self) -> bool {
        !self.is_empty() && self.inner.width() < 0.0
    }

    fn intersects_value(&self, value: f64) -> bool {
        let (left, right) = self.split();
        left.intersects_value(value) || right.intersects_value(value)
    }

    fn intersects_interval(&self, other: &Self) -> bool {
        let (left, right) = self.split();
        let (other_left, other_right) = other.split();
        left.intersects_interval(&other_left)
            || left.intersects_interval(&other_right)
            || right.intersects_interval(&other_left)
            || right.intersects_interval(&other_right)
    }

    fn contains_interval(&self, other: &Self) -> bool {
        let (left, right) = self.split();
        let (other_left, other_right) = other.split();
        left.contains_interval(&other_left) && right.contains_interval(&other_right)
    }

    fn width(&self) -> f64 {
        if self.is_wraparound() {
            f64::INFINITY
        } else {
            self.inner.width()
        }
    }

    fn mid(&self) -> f64 {
        if self.is_wraparound() {
            f64::INFINITY
        } else {
            self.inner.mid()
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn merge_interval(&self, other: &Self) -> Self {
        if self.is_empty() {
            return *other;
        }

        if other.is_empty() {
            return *self;
        }

        let (wraparound, not_wraparound) = match (self.is_wraparound(), other.is_wraparound()) {
            // Handle wraparound/not wraparound below
            (true, false) => (self, other),
            (false, true) => (other, self),
            // Both are wraparound: Merge the two left intervals, then merge the two right intervals
            // and check if we need the full interval
            (true, true) => {
                let (left, right) = self.split();
                let (other_left, other_right) = other.split();

                let new_left = left.merge_interval(&other_left);
                let new_right = right.merge_interval(&other_right);

                // If the left and right intervals intersect each other, we need the full interval
                if new_left.intersects_interval(&new_right) {
                    return WraparoundInterval::full();
                } else {
                    return WraparoundInterval::new(new_right.lo(), new_left.hi());
                }
            }
            // Neither are wraparound: just merge the inner intervals
            (false, false) => {
                return Self {
                    inner: self.inner.merge_interval(&other.inner),
                };
            }
        };

        let (left, right) = wraparound.split();
        let distance_not_wraparound_left = (not_wraparound.mid() - left.hi()).abs();
        let distance_not_wraparound_right = (not_wraparound.mid() - right.lo()).abs();
        let (new_left, new_right) = if distance_not_wraparound_left < distance_not_wraparound_right
        {
            (left.merge_interval(&not_wraparound.inner), right)
        } else {
            (left, right.merge_interval(&not_wraparound.inner))
        };

        // If the left and right intervals intersect each other, we need the full interval
        if new_left.intersects_interval(&new_right) {
            WraparoundInterval::full()
        } else {
            WraparoundInterval::new(new_right.lo(), new_left.hi())
        }
    }

    fn merge_value(&self, value: f64) -> Self {
        if self.intersects_value(value) || value.is_nan() {
            return *self;
        }

        if !self.is_wraparound() {
            return Self {
                inner: self.inner.merge_value(value),
            };
        }

        // Move only one of the endpoints
        let distance_left = value - self.inner.hi;
        let distance_right = self.inner.lo - value;
        debug_assert!(distance_left > 0.0);
        debug_assert!(distance_right > 0.0);
        if distance_left < distance_right {
            Self {
                inner: Interval {
                    lo: self.inner.lo,
                    hi: value,
                },
            }
        } else {
            Self {
                inner: Interval {
                    lo: value,
                    hi: self.inner.hi,
                },
            }
        }
    }

    fn expand_by(&self, distance: f64) -> Self {
        if self.is_empty() || distance.is_nan() || distance < 0.0 {
            return *self;
        }

        if !self.is_wraparound() {
            // For non-wraparound, just expand the inner interval
            return Self {
                inner: self.inner.expand_by(distance),
            };
        }

        // For wraparound intervals, expanding means including more values
        // Wraparound interval (a, b) where a > b excludes the region (b, a)
        // To expand by distance d, we shrink the excluded region from (b, a) to (b+d, a-d)
        // This means the new wraparound interval becomes (a-d, b+d)
        let excluded_lo = self.inner.hi + distance; // b + d
        let excluded_hi = self.inner.lo - distance; // a - d

        // If the excluded region disappears (excluded_lo >= excluded_hi), we get the full interval
        if excluded_lo >= excluded_hi {
            return Self::full();
        }

        // The new wraparound interval excludes (excluded_lo, excluded_hi)
        // So the interval itself is (excluded_hi, excluded_lo)
        Self::new(excluded_hi, excluded_lo)
    }
}

#[cfg(test)]
mod test {
    use core::f64;

    use super::*;

    fn test_empty<T: IntervalTrait>(empty: T) {
        // Equals itself
        #[allow(clippy::eq_op)]
        {
            assert_eq!(empty, empty);
        }

        // Empty intersects no values
        assert!(!empty.intersects_value(0.0));
        assert!(!empty.intersects_value(f64::INFINITY));
        assert!(!empty.intersects_value(-f64::INFINITY));
        assert!(!empty.intersects_value(f64::NAN));

        // Empty intersects no intervals
        assert!(!empty.intersects_interval(&T::new(-10.0, 10.0)));
        assert!(!empty.intersects_interval(&T::empty()));

        // ...except the full interval
        assert!(empty.intersects_interval(&T::full()));

        // Empty contains no intervals
        assert!(!empty.contains_interval(&T::new(-10.0, 10.0)));
        assert!(!empty.contains_interval(&T::full()));

        // ...except empty itself (empty set is subset of itself)
        assert!(empty.contains_interval(&T::empty()));

        // Merging NaN is still empty
        assert_eq!(empty.merge_value(f64::NAN), empty);

        // Merging an empty interval results in an empty interval
        assert_eq!(empty.merge_interval(&empty), empty);

        // Merging a value results in a interval with equal lo/hi
        assert_eq!(empty.merge_value(12.0), T::new(12.0, 12.0));

        // Merging a non-empty interval results in the other interval
        assert_eq!(
            empty.merge_interval(&T::new(10.0, 20.0)),
            T::new(10.0, 20.0)
        );

        // Expanding empty interval keeps it empty
        assert_eq!(empty.expand_by(5.0), empty);
        assert_eq!(empty.expand_by(0.0), empty);
        assert_eq!(empty.expand_by(-1.0), empty);
        assert_eq!(empty.expand_by(f64::NAN), empty);
    }

    #[test]
    fn interval_empty() {
        let empty = Interval::empty();
        test_empty(empty);
    }

    #[test]
    fn wraparound_interval_empty() {
        let empty = WraparoundInterval::empty();

        // Should pass all the regular interval tests
        test_empty(empty);

        // Empty shouldn't be treated as wraparound
        assert!(!empty.is_wraparound());

        // When merging an interval where the other one is a
        // wraparound, we should get the other interval
        assert_eq!(
            empty.merge_interval(&WraparoundInterval::new(20.0, 10.0)),
            WraparoundInterval::new(20.0, 10.0)
        );
    }

    fn test_finite<T: IntervalTrait>(finite: T) {
        // Check accessors
        assert_eq!(finite.lo(), 10.0);
        assert_eq!(finite.hi(), 20.0);
        assert_eq!(finite.mid(), 15.0);
        assert_eq!(finite.width(), 10.0);
        assert!(!finite.is_wraparound());
        assert!(!finite.is_empty());

        // Intersects endpoints and midpoint
        assert!(finite.intersects_value(10.0));
        assert!(finite.intersects_value(15.0));
        assert!(finite.intersects_value(20.0));

        // Doesn't intersect infinite values, NaN, or finite values outside
        // the range
        assert!(!finite.intersects_value(0.0));
        assert!(!finite.intersects_value(f64::INFINITY));
        assert!(!finite.intersects_value(-f64::INFINITY));
        assert!(!finite.intersects_value(f64::NAN));

        // Intervals that intersect
        assert!(finite.intersects_interval(&T::new(14.0, 16.0)));
        assert!(finite.intersects_interval(&T::new(5.0, 15.0)));
        assert!(finite.intersects_interval(&T::new(15.0, 25.0)));
        assert!(finite.intersects_interval(&T::new(5.0, 25.0)));
        assert!(finite.intersects_interval(&T::full()));

        // Barely touching ones count
        assert!(finite.intersects_interval(&T::new(5.0, 10.0)));
        assert!(finite.intersects_interval(&T::new(20.0, 25.0)));

        // Intervals that don't intersect
        assert!(!finite.intersects_interval(&T::new(0.0, 5.0)));
        assert!(!finite.intersects_interval(&T::new(25.0, 30.0)));
        assert!(!finite.intersects_interval(&T::empty()));

        // Intervals that are contained
        assert!(finite.contains_interval(&T::new(14.0, 16.0)));
        assert!(finite.contains_interval(&T::new(10.0, 15.0)));
        assert!(finite.contains_interval(&T::new(15.0, 20.0)));
        assert!(finite.contains_interval(&T::new(10.0, 20.0))); // itself
        assert!(finite.contains_interval(&T::empty()));

        // Intervals that are not contained
        assert!(!finite.contains_interval(&T::new(5.0, 15.0))); // extends below
        assert!(!finite.contains_interval(&T::new(15.0, 25.0))); // extends above
        assert!(!finite.contains_interval(&T::new(5.0, 25.0))); // extends both ways
        assert!(!finite.contains_interval(&T::new(0.0, 5.0))); // completely below
        assert!(!finite.contains_interval(&T::new(25.0, 30.0))); // completely above
        assert!(!finite.contains_interval(&T::full())); // full interval is larger

        // Merging NaN
        assert_eq!(finite.merge_value(f64::NAN), finite);

        // Merging Infinities
        assert_eq!(
            finite.merge_value(f64::INFINITY),
            T::new(finite.lo(), f64::INFINITY)
        );
        assert_eq!(
            finite.merge_value(-f64::INFINITY),
            T::new(-f64::INFINITY, finite.hi())
        );

        // Merging a value within the interval
        assert_eq!(finite.merge_value(15.0), finite);

        // Merging a value above
        assert_eq!(finite.merge_value(25.0), T::new(10.0, 25.0));

        // Merging a value below
        assert_eq!(finite.merge_value(5.0), T::new(5.0, 20.0));

        // Merging an empty interval
        assert_eq!(finite.merge_interval(&T::empty()), finite);

        // Merging an interval with itself
        assert_eq!(finite.merge_interval(&finite), finite);

        // Merging an interval with the full interval
        assert_eq!(finite.merge_interval(&T::full()), T::full());

        // Merging an interval within the interval
        assert_eq!(finite.merge_interval(&T::new(14.0, 16.0)), finite);

        // Merging a partially overlapping interval below
        assert_eq!(finite.merge_interval(&T::new(5.0, 15.0)), T::new(5.0, 20.0));

        // Merging a partially overlapping interval above
        assert_eq!(
            finite.merge_interval(&T::new(15.0, 25.0)),
            T::new(10.0, 25.0)
        );

        // Merging a disjoint interval below
        assert_eq!(finite.merge_interval(&T::new(0.0, 5.0)), T::new(0.0, 20.0));

        // Merging a disjoint interval above
        assert_eq!(
            finite.merge_interval(&T::new(25.0, 30.0)),
            T::new(10.0, 30.0)
        );

        // Expanding by positive distance
        assert_eq!(finite.expand_by(2.0), T::new(8.0, 22.0));
        assert_eq!(finite.expand_by(5.0), T::new(5.0, 25.0));

        // Expanding by zero does nothing
        assert_eq!(finite.expand_by(0.0), finite);

        // Expanding by negative distance does nothing
        assert_eq!(finite.expand_by(-1.0), finite);

        // Expanding by NaN does nothing
        assert_eq!(finite.expand_by(f64::NAN), finite);
    }

    #[test]
    fn interval_finite() {
        let finite = Interval::new(10.0, 20.0);
        test_finite(finite);
    }

    #[test]
    fn wraparound_interval_finite() {
        let finite = WraparoundInterval::new(10.0, 20.0);
        test_finite(finite);

        // Convert to an Interval
        let interval: Interval = finite.try_into().unwrap();
        assert_eq!(interval, Interval::new(10.0, 20.0));
    }

    #[test]
    fn wraparound_interval_actually_wraparound_accessors() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);
        assert!(wraparound.is_wraparound());
        assert!(!wraparound.is_empty());
        assert_eq!(wraparound.mid(), f64::INFINITY);
    }

    #[test]
    fn wraparound_interval_actually_wraparound_intersects_value() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Intersects endpoints but not a point between them
        assert!(wraparound.intersects_value(10.0));
        assert!(wraparound.intersects_value(20.0));
        assert!(!wraparound.intersects_value(15.0));

        // Intersects positive and negative infinity
        assert!(wraparound.intersects_value(f64::INFINITY));
        assert!(wraparound.intersects_value(-f64::INFINITY));

        // ...but not NaN
        assert!(!wraparound.intersects_value(f64::NAN));
    }

    #[test]
    fn wraparound_interval_actually_wraparound_intersects_interval() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Intersects itself
        assert!(wraparound.intersects_interval(&wraparound));

        // Intersects the full interval
        assert!(wraparound.intersects_interval(&WraparoundInterval::full()));

        // Interval completely between endpoints doesn't intersect
        assert!(!wraparound.intersects_interval(&WraparoundInterval::new(14.0, 16.0)));
        // ...unless it's also wraparound
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(16.0, 14.0)));

        // Intervals overlapping endpoints intersect whether the are or aren't wraparound
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(5.0, 15.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(15.0, 5.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(15.0, 25.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(25.0, 15.0)));

        // Barely touching ones still intersect whether the are or aren't wraparound
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(5.0, 10.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(10.0, 5.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(20.0, 25.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(25.0, 20.0)));

        // Intervals completely above and below endpoints do intersect whether they
        // are or aren't wraparound
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(0.0, 5.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(5.0, 0.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(25.0, 30.0)));
        assert!(wraparound.intersects_interval(&WraparoundInterval::new(30.0, 25.0)));
    }

    #[test]
    fn wraparound_interval_actually_wraparound_contains_interval() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Contains itself
        assert!(wraparound.contains_interval(&wraparound));

        // Empty is contained by everything
        assert!(wraparound.contains_interval(&WraparoundInterval::empty()));

        // Does not contain the full interval
        assert!(!wraparound.contains_interval(&WraparoundInterval::full()));

        // Regular interval completely between endpoints is not contained
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(14.0, 16.0)));

        // Wraparound intervals that exclude more (narrower included regions) are contained
        assert!(wraparound.contains_interval(&WraparoundInterval::new(22.0, 8.0))); // excludes (8,22) which is larger than (10,20)
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(18.0, 12.0))); // excludes (12,18) which is smaller than (10,20)

        // Regular intervals don't work the same way due to the split logic
        // For a regular interval (a, b), split gives (left=(a,b), right=empty)
        // For wraparound to contain it, we need both parts to be contained
        // This means (-inf, 10] must contain (a,b) AND [20, inf) must contain empty
        // The second is always true, but the first requires b <= 10
        assert!(wraparound.contains_interval(&WraparoundInterval::new(0.0, 5.0))); // completely within left part
        assert!(wraparound.contains_interval(&WraparoundInterval::new(-5.0, 10.0))); // fits in left part
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(25.0, 30.0))); // doesn't fit in left part
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(20.0, 25.0))); // doesn't fit in left part

        // Regular intervals that overlap the excluded zone are not contained
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(5.0, 15.0))); // overlaps excluded zone
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(15.0, 25.0))); // overlaps excluded zone

        // Wraparound intervals that exclude less (wider included regions) are not contained
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(15.0, 5.0))); // excludes (5,15) which is smaller
        assert!(!wraparound.contains_interval(&WraparoundInterval::new(25.0, 15.0)));
        // excludes (15,25) which is smaller
    }

    #[test]
    fn wraparound_interval_actually_wraparound_merge_value() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Merging NaN
        assert_eq!(wraparound.merge_value(f64::NAN), wraparound);

        // Merging a value closer to the left endpoint should move
        // that endpoint
        assert_eq!(
            wraparound.merge_value(12.0),
            WraparoundInterval::new(20.0, 12.0)
        );

        // Merging a value closer to the right endpoint should move
        // that endpoint
        assert_eq!(
            wraparound.merge_value(18.0),
            WraparoundInterval::new(18.0, 10.0)
        );

        // Merging a value that is already intersecting shouldn't change the interval
        assert_eq!(wraparound.merge_value(5.0), wraparound);
        assert_eq!(wraparound.merge_value(10.0), wraparound);
        assert_eq!(wraparound.merge_value(20.0), wraparound);
        assert_eq!(wraparound.merge_value(25.0), wraparound);
    }

    #[test]
    fn wraparound_interval_actually_wraparound_merge_interval() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Merging an empty interval
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::empty()),
            wraparound
        );

        // Merging an interval with itself
        assert_eq!(wraparound.merge_interval(&wraparound), wraparound);

        // Merging a wraparound interval with a "larger" wraparound interval
        //           10         20
        // <==========|          |============>
        // <==============|  |================>
        //               14  16
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(16.0, 14.0)),
            WraparoundInterval::new(16.0, 14.0)
        );

        // Merging a wraparound interval with a "smaller" wraparound interval
        //           10         20
        // <==========|          |============>
        // <=====|                    |=======>
        //       5                    25
        // <==========|          |============>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(25.0, 5.0)),
            wraparound
        );

        // Merge with partially intersecting wraparounds
        //           10         20
        // <==========|          |============>
        // <=====|          |=================>
        //       5          15
        // <==========|     |=================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(15.0, 5.0)),
            WraparoundInterval::new(15.0, 10.0)
        );

        //           10         20
        // <==========|          |============>
        // <================|          |======>
        //                  15         25
        // <================|    |============>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(25.0, 15.0)),
            WraparoundInterval::new(20.0, 15.0)
        );

        // Merge wraparound with wraparound whose union is the full interval
        //           10         20
        // <==========|          |=========================>
        // <=============================|          |======>
        //                               25         30
        // <===============================================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(30.0, 25.0)),
            WraparoundInterval::full()
        );

        //                    10         20
        // <===================|          |================>
        // <==|          |=================================>
        //    0          5
        // <===============================================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(5.0, 0.0)),
            WraparoundInterval::full()
        );

        // Merge wraparound with a regular interval completely contained by the original
        //                  10         20
        // <=================|          |==================>
        //                                   |=========|
        //                                  25         30
        // <=================|          |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(25.0, 30.0)),
            wraparound
        );

        //                  10         20
        // <=================|          |==================>
        //  |=========|
        //  0         5
        // <=================|          |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(0.0, 5.0)),
            wraparound
        );

        // Merge wraparound with a partially intersecting regular interval that
        // should extend the left side
        //                  10         20
        // <=================|          |==================>
        //              |=========|
        //              5         15
        // <======================|     |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(5.0, 15.0)),
            WraparoundInterval::new(20.0, 15.0)
        );

        // Merge wraparound with a partially intersecting regular interval that
        // should extend the right side
        //                  10         20
        // <=================|          |==================>
        //                         |=========|
        //                         15        25
        // <=================|     |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(15.0, 25.0)),
            WraparoundInterval::new(15.0, 10.0)
        );

        // Merge wraparound with a disjoint regular interval that should extend the left side
        //                  10         20
        // <=================|          |==================>
        //                     |==|
        //                    12  15
        // <======================|     |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(12.0, 15.0)),
            WraparoundInterval::new(20.0, 15.0)
        );

        // Merge wraparound with a disjoint regular interval that should extend the right side
        //                  10         20
        // <=================|          |==================>
        //                         |==|
        //                        15  18
        // <=================|     |==================>
        assert_eq!(
            wraparound.merge_interval(&WraparoundInterval::new(15.0, 18.0)),
            WraparoundInterval::new(15.0, 10.0)
        );
    }

    #[test]
    fn wraparound_interval_actually_wraparound_expand_by() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Expanding by a small amount shrinks the excluded region
        // Original excludes (10, 20), expanding by 2 should exclude (12, 18)
        // So the new interval should be (18, 12) = everything except (12, 18)
        assert_eq!(
            wraparound.expand_by(2.0),
            WraparoundInterval::new(18.0, 12.0)
        ); // now excludes (12, 18)

        // Expanding by 4 should exclude (14, 16)
        assert_eq!(
            wraparound.expand_by(4.0),
            WraparoundInterval::new(16.0, 14.0)
        ); // now excludes (14, 16)

        // Expanding by 5.0 should exactly eliminate the excluded region
        // excluded region (10, 20) shrinks to (15, 15) which is empty
        assert_eq!(wraparound.expand_by(5.0), WraparoundInterval::full()); // excluded region disappears

        // Any expansion greater than 5.0 should also give full interval
        assert_eq!(wraparound.expand_by(6.0), WraparoundInterval::full());

        assert_eq!(wraparound.expand_by(100.0), WraparoundInterval::full());

        // Expanding by zero does nothing
        assert_eq!(wraparound.expand_by(0.0), wraparound);

        // Expanding by negative distance does nothing
        assert_eq!(wraparound.expand_by(-1.0), wraparound);

        // Expanding by NaN does nothing
        assert_eq!(wraparound.expand_by(f64::NAN), wraparound);

        // Test a finite (non-wraparound) wraparound interval
        let non_wraparound = WraparoundInterval::new(10.0, 20.0);
        assert!(!non_wraparound.is_wraparound());
        assert_eq!(
            non_wraparound.expand_by(2.0),
            WraparoundInterval::new(8.0, 22.0)
        );

        // Test another wraparound case - excludes (5, 15) with width 10
        let wraparound2 = WraparoundInterval::new(15.0, 5.0);
        // Expanding by 3 should shrink excluded region from (5, 15) to (8, 12)
        assert_eq!(
            wraparound2.expand_by(3.0),
            WraparoundInterval::new(12.0, 8.0)
        );

        // Expanding by 5 should make excluded region disappear: (5+5, 15-5) = (10, 10)
        assert_eq!(wraparound2.expand_by(5.0), WraparoundInterval::full());
    }

    #[test]
    fn wraparound_interval_actually_wraparound_convert() {
        // Everything *except* the interval (10, 20)
        let wraparound = WraparoundInterval::new(20.0, 10.0);

        // Can't convert a wraparound interval that actually wraps around to an Interval
        let err = Interval::try_from(wraparound).unwrap_err();
        assert!(
            err.to_string()
                .contains("Can't convert wraparound interval")
        );
    }
}
