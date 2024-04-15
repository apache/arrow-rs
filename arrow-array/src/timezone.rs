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

#[cfg(feature = "chrono-tz")]
mod private {
    use super::*;
    use chrono::offset::TimeZone;
    use chrono::{LocalResult, NaiveDate, NaiveDateTime, Offset};
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
