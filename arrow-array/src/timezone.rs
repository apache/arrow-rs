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
use chrono::{DateTime, FixedOffset, NaiveDateTime};
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

#[cfg(feature = "chrono-tz")]
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

    #[inline]
    pub(super) fn new_gap_info(local: &NaiveDateTime, tz: &Tz) -> Option<GapInfo> {
        let TzInner::Timezone(ctz) = &tz.0 else {
            return None;
        };
        chrono_tz::GapInfo::new(local, ctz).map(|gi| GapInfo {
            begin: gi.begin.map(|(ndt, ctz)| {
                (
                    ndt,
                    TzOffset {
                        tz: *tz,
                        offset: ctz.fix(),
                    },
                )
            }),
            end: gi.end.map(|dt| dt.with_timezone(tz)),
        })
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

        fn naive(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_opt(h, mi, 0)
                .unwrap()
        }

        fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<Utc> {
            Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
        }

        #[test]
        fn test_gap_info_not_in_gap() {
            let tz: Tz = "America/New_York".parse().unwrap();
            // Just before and at the end of the 2021 spring forward gap
            assert!(GapInfo::new(&naive(2021, 3, 14, 1, 59), &tz).is_none());
            assert!(GapInfo::new(&naive(2021, 3, 14, 3, 0), &tz).is_none());
            // Ambiguous time during the 2021 fall back overlap is not a gap
            assert!(GapInfo::new(&naive(2021, 11, 7, 1, 30), &tz).is_none());
        }

        #[test]
        fn test_gap_info_america_new_york() {
            // Clocks went forward from 2021-03-14T02:00:00-05:00 to 2021-03-14T03:00:00-04:00
            let tz: Tz = "America/New_York".parse().unwrap();
            for local in [
                naive(2021, 3, 14, 2, 0),
                naive(2021, 3, 14, 2, 30),
                naive(2021, 3, 14, 2, 59),
            ] {
                assert!(tz.from_local_datetime(&local).single().is_none());

                let gap = GapInfo::new(&local, &tz).unwrap();
                let (begin, begin_offset) = gap.begin.unwrap();
                assert_eq!(begin, naive(2021, 3, 14, 2, 0));
                assert_eq!(
                    begin_offset.fix(),
                    FixedOffset::west_opt(5 * 60 * 60).unwrap()
                );

                let end = gap.end.unwrap();
                assert_eq!(end.naive_local(), naive(2021, 3, 14, 3, 0));
                assert_eq!(
                    end.offset().fix(),
                    FixedOffset::west_opt(4 * 60 * 60).unwrap()
                );
                assert_eq!(end, utc(2021, 3, 14, 7, 0));
                assert_eq!(end.timezone().to_string(), "America/New_York");
            }
        }

        #[test]
        fn test_gap_info_europe_london() {
            // Clocks went forward from 2021-03-28T01:00:00+00:00 to 2021-03-28T02:00:00+01:00
            let tz: Tz = "Europe/London".parse().unwrap();
            let gap = GapInfo::new(&naive(2021, 3, 28, 1, 30), &tz).unwrap();

            let (begin, begin_offset) = gap.begin.unwrap();
            assert_eq!(begin, naive(2021, 3, 28, 1, 0));
            assert_eq!(begin_offset.fix(), FixedOffset::east_opt(0).unwrap());

            let end = gap.end.unwrap();
            assert_eq!(end.naive_local(), naive(2021, 3, 28, 2, 0));
            assert_eq!(end.offset().fix(), FixedOffset::east_opt(60 * 60).unwrap());
            assert_eq!(end, utc(2021, 3, 28, 1, 0));
        }

        #[test]
        fn test_gap_info_australia_sydney() {
            // Clocks went forward from 2021-10-03T02:00:00+10:00 to 2021-10-03T03:00:00+11:00
            let tz: Tz = "Australia/Sydney".parse().unwrap();
            let gap = GapInfo::new(&naive(2021, 10, 3, 2, 30), &tz).unwrap();

            let (begin, begin_offset) = gap.begin.unwrap();
            assert_eq!(begin, naive(2021, 10, 3, 2, 0));
            assert_eq!(
                begin_offset.fix(),
                FixedOffset::east_opt(10 * 60 * 60).unwrap()
            );

            let end = gap.end.unwrap();
            assert_eq!(end.naive_local(), naive(2021, 10, 3, 3, 0));
            assert_eq!(
                end.offset().fix(),
                FixedOffset::east_opt(11 * 60 * 60).unwrap()
            );
            assert_eq!(end, utc(2021, 10, 2, 16, 0));
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

#[cfg(not(feature = "chrono-tz"))]
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
                    "Invalid timezone \"{tz}\": only offset based timezones supported without chrono-tz feature"
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

    #[inline]
    pub(super) fn new_gap_info(_local: &NaiveDateTime, _tz: &Tz) -> Option<GapInfo> {
        // Fixed offsets never contain gaps
        None
    }
}

/// Represents the information of a gap.
///
/// This returns useful information that can be used when converting a local [`NaiveDateTime`]
/// to a timezone-aware [`DateTime`] with [`chrono::TimeZone::from_local_datetime`] and a gap
/// ([`chrono::LocalResult::None`]) is found.
///
/// # Examples
///
/// In the `America/New_York` timezone the clocks went forward one hour at `2026-03-08T02:00:00`.
/// The local time `2026-03-08T01:30:00` exists, but `2026-03-08T02:30:00` falls in the gap and
/// does not.
///
/// ```
/// # #[cfg(feature = "chrono-tz")]
/// # {
/// use arrow_array::timezone::{GapInfo, Tz};
/// use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, Offset, TimeZone};
///
/// let tz: Tz = "America/New_York".parse().unwrap();
/// let expect_offset: FixedOffset = "-0500".parse().unwrap();
///
/// // A local time before the gap has a single offset, and is not in a gap.
/// let exists: NaiveDateTime = "2026-03-08T01:30:00".parse().unwrap();
/// let offset = tz.offset_from_local_datetime(&exists).single().unwrap();
/// assert_eq!(offset.fix(), expect_offset);
/// assert!(GapInfo::new(&exists, &tz).is_none());
///
/// // A local time inside the gap has no offset...
/// let not_exist: NaiveDateTime = "2026-03-08T02:30:00".parse().unwrap();
/// let offset = tz.offset_from_local_datetime(&not_exist);
/// assert!(matches!(offset, LocalResult::None));
///
/// // ...but `GapInfo` describes the gap it falls in.
/// let GapInfo { begin, end } = GapInfo::new(&not_exist, &tz).unwrap();
///
/// // The gap begins at 02:00 local time. The offset is the one in effect before the gap.
/// let (begin_datetime, begin_offset) = begin.unwrap();
/// let expect_begin: NaiveDateTime = "2026-03-08T02:00:00".parse().unwrap();
/// assert_eq!(begin_datetime, expect_begin);
/// assert_eq!(begin_offset.fix(), expect_offset);
///
/// // The gap ends at 03:00 local time, with the offset in effect after the gap.
/// let expect_end: DateTime<FixedOffset> = "2026-03-08T03:00:00-0400".parse().unwrap();
/// assert_eq!(end.unwrap(), expect_end.with_timezone(&tz));
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct GapInfo {
    /// When available it contains information about the beginning of the gap.
    ///
    /// The time represents the first instant in which the gap starts.
    /// This means that it is the first instant that when used with
    /// [`chrono::TimeZone::from_local_datetime`] it will return
    /// [`chrono::LocalResult::None`].
    ///
    /// The offset represents the offset of the first instant before the gap.
    pub begin: Option<(NaiveDateTime, TzOffset)>,

    /// When available it contains the first instant after the gap.
    pub end: Option<DateTime<Tz>>,
}

impl GapInfo {
    /// Return information about a gap.
    ///
    /// It returns `None` if `local` is not in a gap for the current timezone. Timezones
    /// that represent fixed offsets contain no gaps and therefore always return `None`.
    ///
    /// If `local` is at the limits of the known timestamps the fields `begin` or `end` in
    /// [`GapInfo`] will be `None`.
    pub fn new(local: &NaiveDateTime, tz: &Tz) -> Option<Self> {
        private::new_gap_info(local, tz)
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

    #[test]
    fn test_gap_info_fixed_offset() {
        let times = [
            NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            // Times that are in a DST gap for some named timezones
            NaiveDate::from_ymd_opt(2021, 3, 14)
                .unwrap()
                .and_hms_opt(2, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2021, 3, 28)
                .unwrap()
                .and_hms_opt(1, 30, 0)
                .unwrap(),
        ];

        for tz in ["+00:00", "-05:00", "+01:00", "+09:30"] {
            let tz: Tz = tz.parse().unwrap();
            for t in &times {
                assert!(GapInfo::new(t, &tz).is_none(), "{tz:?} {t}");
            }
        }
    }
}
