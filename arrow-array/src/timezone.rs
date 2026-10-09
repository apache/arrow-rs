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

//! Timezone for timestamp arrays

use arrow_schema::ArrowError;
use chrono::FixedOffset;
pub use private::{Tz, TzOffset};

/// Parses a fixed offset of the form "+09:00", "-09" or "+0930"
fn parse_fixed_offset(tz: &str) -> Option<FixedOffset> {
    let bytes = tz.as_bytes();

    let mut values = match bytes.len() {
        // [+-]XX:XX
        6 if bytes[3] == b':' => [bytes[1], bytes[2], bytes[4], bytes[5]],
        // [+-]XXXX
        5 => [bytes[1], bytes[2], bytes[3], bytes[4]],
        // [+-]XX
        3 => [bytes[1], bytes[2], b'0', b'0'],
        _ => return None,
    };
    values.iter_mut().for_each(|x| *x = x.wrapping_sub(b'0'));
    if values.iter().any(|x| *x > 9) {
        return None;
    }
    let secs =
        (values[0] * 10 + values[1]) as i32 * 60 * 60 + (values[2] * 10 + values[3]) as i32 * 60;

    match bytes[0] {
        b'+' => FixedOffset::east_opt(secs),
        b'-' => FixedOffset::west_opt(secs),
        _ => None,
    }
}

#[cfg(feature = "jiff")]
mod private {
    use super::*;
    use chrono::offset::TimeZone;
    use chrono::{Datelike, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset, Timelike};
    use jiff::Timestamp;
    use jiff::civil::DateTime as JiffDateTime;
    use jiff::tz::{AmbiguousOffset, TimeZone as JiffTimeZone};
    use std::collections::HashMap;
    use std::fmt::Display;
    use std::str::FromStr;
    use std::sync::{OnceLock, RwLock};

    /// An [`Offset`] for [`Tz`]
    #[derive(Debug, Copy, Clone)]
    pub struct TzOffset {
        tz: Tz,
        offset: FixedOffset,
    }

    impl std::fmt::Display for TzOffset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.offset.fmt(f)
        }
    }

    impl Offset for TzOffset {
        fn fix(&self) -> FixedOffset {
            self.offset
        }
    }

    /// A named timezone resolved by jiff, together with the name it was parsed from.
    ///
    /// [`Tz`] is `Copy` whereas [`JiffTimeZone`] is reference counted, so each distinct name is
    /// resolved once and leaked to give `Tz` a `'static` reference. The set of names jiff accepts
    /// is bounded by the bundled database, so the leaked memory is bounded too, and the hot path
    /// (offset lookup) takes no lock.
    #[derive(Debug)]
    struct NamedTz {
        name: Box<str>,
        tz: JiffTimeZone,
    }

    fn named_tz(name: &str) -> Result<&'static NamedTz, ArrowError> {
        static CACHE: OnceLock<RwLock<HashMap<Box<str>, &'static NamedTz>>> = OnceLock::new();
        let cache = CACHE.get_or_init(Default::default);
        if let Some(tz) = cache.read().unwrap().get(name) {
            return Ok(tz);
        }
        // Same message as the chrono-tz path, so callers see one error regardless of feature
        let tz = JiffTimeZone::get(name).map_err(|_| {
            ArrowError::ParseError(format!(
                "Invalid timezone \"{name}\": failed to parse timezone"
            ))
        })?;
        let mut cache = cache.write().unwrap();
        Ok(cache.entry(name.into()).or_insert_with(|| {
            Box::leak(Box::new(NamedTz {
                name: name.into(),
                tz,
            }))
        }))
    }

    /// An Arrow [`TimeZone`]
    #[derive(Debug, Copy, Clone)]
    pub struct Tz(TzInner);

    #[derive(Debug, Copy, Clone)]
    enum TzInner {
        Timezone(&'static NamedTz),
        Offset(FixedOffset),
    }

    impl FromStr for Tz {
        type Err = ArrowError;

        fn from_str(tz: &str) -> Result<Self, Self::Err> {
            match parse_fixed_offset(tz) {
                Some(offset) => Ok(Self(TzInner::Offset(offset))),
                None => Ok(Self(TzInner::Timezone(named_tz(tz)?))),
            }
        }
    }

    impl Display for Tz {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                TzInner::Timezone(tz) => f.write_str(&tz.name),
                TzInner::Offset(offset) => offset.fmt(f),
            }
        }
    }

    fn fixed_offset(offset: jiff::tz::Offset) -> FixedOffset {
        // Real timezone data never holds an offset of a day or more, so this cannot fail
        FixedOffset::east_opt(offset.seconds()).expect("timezone offset within a day")
    }

    /// The instant jiff should consult for a UTC datetime. jiff covers years -9999 to 9999;
    /// anything outside that range takes the offset in force at the nearest covered instant.
    fn jiff_timestamp(utc: &NaiveDateTime) -> Timestamp {
        let seconds = utc.and_utc().timestamp();
        Timestamp::from_second(seconds).unwrap_or(if seconds < 0 {
            Timestamp::MIN
        } else {
            Timestamp::MAX
        })
    }

    /// The civil datetime jiff should resolve for a local datetime, or `None` when the year is
    /// outside jiff's range. Sub-second precision is irrelevant to the offset in force.
    fn jiff_civil(local: &NaiveDateTime) -> Option<JiffDateTime> {
        JiffDateTime::new(
            i16::try_from(local.year()).ok()?,
            local.month() as i8,
            local.day() as i8,
            local.hour() as i8,
            local.minute() as i8,
            local.second() as i8,
            0,
        )
        .ok()
    }

    impl Tz {
        fn offset(&self, offset: FixedOffset) -> TzOffset {
            TzOffset { tz: *self, offset }
        }
    }

    impl TimeZone for Tz {
        type Offset = TzOffset;

        fn from_offset(offset: &Self::Offset) -> Self {
            offset.tz
        }

        fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
            match self.0 {
                TzInner::Offset(tz) => tz
                    .offset_from_local_date(local)
                    .map(|x| self.offset(x.fix())),
                TzInner::Timezone(_) => {
                    // A date has no single offset on a day with a transition. As chrono-tz
                    // does, prefer any offset that was in force at some point during the day
                    // over reporting the ambiguity, so that unambiguous times on such a day
                    // still resolve.
                    let earliest = self.offset_from_local_datetime(&local.and_time(NaiveTime::MIN));
                    let latest =
                        self.offset_from_local_datetime(&local.and_hms_opt(23, 59, 59).unwrap());
                    match (earliest, latest) {
                        (result @ LocalResult::Single(_), _) => result,
                        (_, result @ LocalResult::Single(_)) => result,
                        (LocalResult::Ambiguous(offset, _), _) => LocalResult::Single(offset),
                        (_, LocalResult::Ambiguous(offset, _)) => LocalResult::Single(offset),
                        (LocalResult::None, LocalResult::None) => LocalResult::None,
                    }
                }
            }
        }

        fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
            match self.0 {
                TzInner::Offset(tz) => tz
                    .offset_from_local_datetime(local)
                    .map(|x| self.offset(x.fix())),
                TzInner::Timezone(tz) => match jiff_civil(local) {
                    None => LocalResult::Single(self.offset_from_utc_datetime(local)),
                    Some(dt) => match tz.tz.to_ambiguous_timestamp(dt).offset() {
                        AmbiguousOffset::Unambiguous { offset } => {
                            LocalResult::Single(self.offset(fixed_offset(offset)))
                        }
                        AmbiguousOffset::Gap { .. } => LocalResult::None,
                        AmbiguousOffset::Fold { before, after } => LocalResult::Ambiguous(
                            self.offset(fixed_offset(before)),
                            self.offset(fixed_offset(after)),
                        ),
                    },
                },
            }
        }

        fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
            self.offset_from_utc_datetime(&utc.and_time(NaiveTime::MIN))
        }

        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
            match self.0 {
                TzInner::Offset(tz) => self.offset(tz.offset_from_utc_datetime(utc).fix()),
                TzInner::Timezone(tz) => {
                    self.offset(fixed_offset(tz.tz.to_offset(jiff_timestamp(utc))))
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::{Timelike, Utc};

        #[test]
        fn test_with_timezone() {
            let vals = [
                Utc.timestamp_millis_opt(37800000).unwrap(),
                Utc.timestamp_millis_opt(86339000).unwrap(),
            ];

            assert_eq!(10, vals[0].hour());
            assert_eq!(23, vals[1].hour());

            let tz: Tz = "America/Los_Angeles".parse().unwrap();

            assert_eq!(2, vals[0].with_timezone(&tz).hour());
            assert_eq!(15, vals[1].with_timezone(&tz).hour());
        }

        #[test]
        fn test_dst_transitions_from_utc() {
            let tz: Tz = "Australia/Sydney".parse().unwrap();
            let standard = FixedOffset::east_opt(10 * 60 * 60).unwrap();
            let daylight = FixedOffset::east_opt(11 * 60 * 60).unwrap();
            let at = |y, m, d, h, min| {
                NaiveDate::from_ymd_opt(y, m, d)
                    .unwrap()
                    .and_hms_opt(h, min, 0)
                    .unwrap()
            };
            // Daylight time ended 2021-04-04T03:00+11:00, which is 2021-04-03T16:00Z
            assert_eq!(
                tz.offset_from_utc_datetime(&at(2021, 4, 3, 15, 30)).fix(),
                daylight
            );
            assert_eq!(
                tz.offset_from_utc_datetime(&at(2021, 4, 3, 16, 30)).fix(),
                standard
            );
            // Daylight time started 2021-10-03T02:00+10:00, which is 2021-10-02T16:00Z
            assert_eq!(
                tz.offset_from_utc_datetime(&at(2021, 10, 2, 15, 30)).fix(),
                standard
            );
            assert_eq!(
                tz.offset_from_utc_datetime(&at(2021, 10, 2, 16, 30)).fix(),
                daylight
            );
        }

        #[test]
        fn test_dst_transitions_from_local() {
            let tz: Tz = "America/New_York".parse().unwrap();
            let eastern_standard = FixedOffset::west_opt(5 * 60 * 60).unwrap();
            let eastern_daylight = FixedOffset::west_opt(4 * 60 * 60).unwrap();
            let at = |m, d, h, min| {
                NaiveDate::from_ymd_opt(2024, m, d)
                    .unwrap()
                    .and_hms_opt(h, min, 0)
                    .unwrap()
            };
            match tz.offset_from_local_datetime(&at(1, 15, 12, 0)) {
                LocalResult::Single(offset) => assert_eq!(offset.fix(), eastern_standard),
                other => panic!("expected a single offset, got {other:?}"),
            }
            // 02:30 on 2024-03-10 was skipped when clocks went forward
            assert!(matches!(
                tz.offset_from_local_datetime(&at(3, 10, 2, 30)),
                LocalResult::None
            ));
            // 01:30 on 2024-11-03 happened twice; the daylight reading comes first
            match tz.offset_from_local_datetime(&at(11, 3, 1, 30)) {
                LocalResult::Ambiguous(first, second) => {
                    assert_eq!(first.fix(), eastern_daylight);
                    assert_eq!(second.fix(), eastern_standard);
                }
                other => panic!("expected an ambiguous offset, got {other:?}"),
            }
            // A date on a transition day still resolves to an offset in force that day
            assert!(matches!(
                tz.offset_from_local_date(&NaiveDate::from_ymd_opt(2024, 3, 10).unwrap()),
                LocalResult::Single(_)
            ));
        }

        /// chrono-tz stops applying daylight saving after its last precomputed transition,
        /// around 2099. jiff applies the zone's rules indefinitely, as `java.time` and libc do.
        #[test]
        fn test_dst_applies_after_2099() {
            let tz: Tz = "America/Los_Angeles".parse().unwrap();
            let pacific_standard = FixedOffset::west_opt(8 * 60 * 60).unwrap();
            let pacific_daylight = FixedOffset::west_opt(7 * 60 * 60).unwrap();
            for year in [2024, 2100, 2300, 9999] {
                let summer = NaiveDate::from_ymd_opt(year, 7, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap();
                let winter = NaiveDate::from_ymd_opt(year, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap();
                assert_eq!(
                    tz.offset_from_utc_datetime(&summer).fix(),
                    pacific_daylight,
                    "summer {year}"
                );
                assert_eq!(
                    tz.offset_from_utc_datetime(&winter).fix(),
                    pacific_standard,
                    "winter {year}"
                );
                match tz.offset_from_local_datetime(&summer) {
                    LocalResult::Single(offset) => {
                        assert_eq!(offset.fix(), pacific_daylight, "local summer {year}")
                    }
                    other => panic!("expected a single offset for {year}, got {other:?}"),
                }
            }
        }

        #[test]
        fn test_beyond_jiff_range() {
            let tz: Tz = "America/Los_Angeles".parse().unwrap();
            let far = NaiveDate::from_ymd_opt(20_000, 7, 1)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap();
            // Falls back to the offset in force at jiff's last covered instant rather than failing
            let utc = tz.offset_from_utc_datetime(&far).fix();
            assert!(utc.local_minus_utc() == -8 * 60 * 60 || utc.local_minus_utc() == -7 * 60 * 60);
            assert!(matches!(
                tz.offset_from_local_datetime(&far),
                LocalResult::Single(_)
            ));
        }

        #[test]
        fn test_timezone_display() {
            let test_cases = ["UTC", "America/Los_Angeles", "-08:00", "+05:30"];
            for &case in &test_cases {
                let tz: Tz = case.parse().unwrap();
                assert_eq!(tz.to_string(), case);
            }
        }

        #[test]
        fn test_invalid_timezone() {
            let err = "Not/AZone".parse::<Tz>().unwrap_err().to_string();
            assert!(err.contains("Invalid timezone \"Not/AZone\""), "{err}");
        }
    }
}

#[cfg(all(feature = "chrono-tz", not(feature = "jiff")))]
mod private {
    use super::*;
    use chrono::offset::TimeZone;
    use chrono::{LocalResult, NaiveDate, NaiveDateTime, Offset};
    use std::fmt::Display;
    use std::str::FromStr;

    /// An [`Offset`] for [`Tz`]
    #[derive(Debug, Copy, Clone)]
    pub struct TzOffset {
        tz: Tz,
        offset: FixedOffset,
    }

    impl std::fmt::Display for TzOffset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.offset.fmt(f)
        }
    }

    impl Offset for TzOffset {
        fn fix(&self) -> FixedOffset {
            self.offset
        }
    }

    /// An Arrow [`TimeZone`]
    #[derive(Debug, Copy, Clone)]
    pub struct Tz(TzInner);

    #[derive(Debug, Copy, Clone)]
    enum TzInner {
        Timezone(chrono_tz::Tz),
        Offset(FixedOffset),
    }

    impl FromStr for Tz {
        type Err = ArrowError;

        fn from_str(tz: &str) -> Result<Self, Self::Err> {
            match parse_fixed_offset(tz) {
                Some(offset) => Ok(Self(TzInner::Offset(offset))),
                None => Ok(Self(TzInner::Timezone(tz.parse().map_err(|e| {
                    ArrowError::ParseError(format!("Invalid timezone \"{tz}\": {e}"))
                })?))),
            }
        }
    }

    impl Display for Tz {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                TzInner::Timezone(tz) => tz.fmt(f),
                TzInner::Offset(offset) => offset.fmt(f),
            }
        }
    }

    macro_rules! tz {
        ($s:ident, $tz:ident, $b:block) => {
            match $s.0 {
                TzInner::Timezone($tz) => $b,
                TzInner::Offset($tz) => $b,
            }
        };
    }

    impl TimeZone for Tz {
        type Offset = TzOffset;

        fn from_offset(offset: &Self::Offset) -> Self {
            offset.tz
        }

        fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
            tz!(self, tz, {
                tz.offset_from_local_date(local).map(|x| TzOffset {
                    tz: *self,
                    offset: x.fix(),
                })
            })
        }

        fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
            tz!(self, tz, {
                tz.offset_from_local_datetime(local).map(|x| TzOffset {
                    tz: *self,
                    offset: x.fix(),
                })
            })
        }

        fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
            tz!(self, tz, {
                TzOffset {
                    tz: *self,
                    offset: tz.offset_from_utc_date(utc).fix(),
                }
            })
        }

        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
            tz!(self, tz, {
                TzOffset {
                    tz: *self,
                    offset: tz.offset_from_utc_datetime(utc).fix(),
                }
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::{Timelike, Utc};

        #[test]
        fn test_with_timezone() {
            let vals = [
                Utc.timestamp_millis_opt(37800000).unwrap(),
                Utc.timestamp_millis_opt(86339000).unwrap(),
            ];

            assert_eq!(10, vals[0].hour());
            assert_eq!(23, vals[1].hour());

            let tz: Tz = "America/Los_Angeles".parse().unwrap();

            assert_eq!(2, vals[0].with_timezone(&tz).hour());
            assert_eq!(15, vals[1].with_timezone(&tz).hour());
        }

        #[test]
        fn test_using_chrono_tz_and_utc_naive_date_time() {
            let sydney_tz = "Australia/Sydney".to_string();
            let tz: Tz = sydney_tz.parse().unwrap();
            let sydney_offset_without_dst = FixedOffset::east_opt(10 * 60 * 60).unwrap();
            let sydney_offset_with_dst = FixedOffset::east_opt(11 * 60 * 60).unwrap();
            // Daylight savings ends
            // When local daylight time was about to reach
            // Sunday, 4 April 2021, 3:00:00 am clocks were turned backward 1 hour to
            // Sunday, 4 April 2021, 2:00:00 am local standard time instead.

            // Daylight savings starts
            // When local standard time was about to reach
            // Sunday, 3 October 2021, 2:00:00 am clocks were turned forward 1 hour to
            // Sunday, 3 October 2021, 3:00:00 am local daylight time instead.

            // Sydney 2021-04-04T02:30:00+11:00 is 2021-04-03T15:30:00Z
            let utc_just_before_sydney_dst_ends = NaiveDate::from_ymd_opt(2021, 4, 3)
                .unwrap()
                .and_hms_nano_opt(15, 30, 0, 0)
                .unwrap();
            assert_eq!(
                tz.offset_from_utc_datetime(&utc_just_before_sydney_dst_ends)
                    .fix(),
                sydney_offset_with_dst
            );
            // Sydney 2021-04-04T02:30:00+10:00 is 2021-04-03T16:30:00Z
            let utc_just_after_sydney_dst_ends = NaiveDate::from_ymd_opt(2021, 4, 3)
                .unwrap()
                .and_hms_nano_opt(16, 30, 0, 0)
                .unwrap();
            assert_eq!(
                tz.offset_from_utc_datetime(&utc_just_after_sydney_dst_ends)
                    .fix(),
                sydney_offset_without_dst
            );
            // Sydney 2021-10-03T01:30:00+10:00 is 2021-10-02T15:30:00Z
            let utc_just_before_sydney_dst_starts = NaiveDate::from_ymd_opt(2021, 10, 2)
                .unwrap()
                .and_hms_nano_opt(15, 30, 0, 0)
                .unwrap();
            assert_eq!(
                tz.offset_from_utc_datetime(&utc_just_before_sydney_dst_starts)
                    .fix(),
                sydney_offset_without_dst
            );
            // Sydney 2021-04-04T03:30:00+11:00 is 2021-10-02T16:30:00Z
            let utc_just_after_sydney_dst_starts = NaiveDate::from_ymd_opt(2022, 10, 2)
                .unwrap()
                .and_hms_nano_opt(16, 30, 0, 0)
                .unwrap();
            assert_eq!(
                tz.offset_from_utc_datetime(&utc_just_after_sydney_dst_starts)
                    .fix(),
                sydney_offset_with_dst
            );
        }

        #[test]
        fn test_timezone_display() {
            let test_cases = ["UTC", "America/Los_Angeles", "-08:00", "+05:30"];
            for &case in &test_cases {
                let tz: Tz = case.parse().unwrap();
                assert_eq!(tz.to_string(), case);
            }
        }
    }
}

#[cfg(not(any(feature = "chrono-tz", feature = "jiff")))]
mod private {
    use super::*;
    use chrono::offset::TimeZone;
    use chrono::{LocalResult, NaiveDate, NaiveDateTime, Offset};
    use std::str::FromStr;

    /// An [`Offset`] for [`Tz`]
    #[derive(Debug, Copy, Clone)]
    pub struct TzOffset(FixedOffset);

    impl std::fmt::Display for TzOffset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl Offset for TzOffset {
        fn fix(&self) -> FixedOffset {
            self.0
        }
    }

    /// An Arrow [`TimeZone`]
    #[derive(Debug, Copy, Clone)]
    pub struct Tz(FixedOffset);

    impl FromStr for Tz {
        type Err = ArrowError;

        fn from_str(tz: &str) -> Result<Self, Self::Err> {
            let offset = parse_fixed_offset(tz).ok_or_else(|| {
                ArrowError::ParseError(format!(
                    "Invalid timezone \"{tz}\": only offset based timezones supported without chrono-tz or jiff feature"
                ))
            })?;
            Ok(Self(offset))
        }
    }

    impl TimeZone for Tz {
        type Offset = TzOffset;

        fn from_offset(offset: &Self::Offset) -> Self {
            Self(offset.0)
        }

        fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
            self.0.offset_from_local_date(local).map(TzOffset)
        }

        fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
            self.0.offset_from_local_datetime(local).map(TzOffset)
        }

        fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
            TzOffset(self.0.offset_from_utc_date(utc).fix())
        }

        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
            TzOffset(self.0.offset_from_utc_datetime(utc).fix())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Offset, TimeZone};

    #[test]
    fn test_with_offset() {
        let t = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

        let tz: Tz = "-00:00".parse().unwrap();
        assert_eq!(tz.offset_from_utc_date(&t).fix().local_minus_utc(), 0);
        let tz: Tz = "+00:00".parse().unwrap();
        assert_eq!(tz.offset_from_utc_date(&t).fix().local_minus_utc(), 0);

        let tz: Tz = "-10:00".parse().unwrap();
        assert_eq!(
            tz.offset_from_utc_date(&t).fix().local_minus_utc(),
            -10 * 60 * 60
        );
        let tz: Tz = "+09:00".parse().unwrap();
        assert_eq!(
            tz.offset_from_utc_date(&t).fix().local_minus_utc(),
            9 * 60 * 60
        );

        let tz = "+09".parse::<Tz>().unwrap();
        assert_eq!(
            tz.offset_from_utc_date(&t).fix().local_minus_utc(),
            9 * 60 * 60
        );

        let tz = "+0900".parse::<Tz>().unwrap();
        assert_eq!(
            tz.offset_from_utc_date(&t).fix().local_minus_utc(),
            9 * 60 * 60
        );

        let err = "+9:00".parse::<Tz>().unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }
}
