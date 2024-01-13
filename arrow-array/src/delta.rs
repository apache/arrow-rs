// MIT License
//
// Copyright (c) 2020-2022 Oliver Margetts
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Copied from chronoutil crate

//! Contains utility functions for shifting Date objects.
use chrono::{DateTime, Datelike, Days, Months, TimeZone};
use std::cmp::Ordering;

/// Shift a date by the given number of months.
pub(crate) fn shift_months<D>(date: D, months: i32) -> D
where
    D: Datelike + std::ops::Add<Months, Output = D> + std::ops::Sub<Months, Output = D>,
{
    match months.cmp(&0) {
        Ordering::Equal => date,
        Ordering::Greater => date + Months::new(months as u32),
        Ordering::Less => date - Months::new(months.unsigned_abs()),
    }
}

/// Add the given number of months to the given datetime.
///
/// Returns `None` when it will result in overflow.
pub(crate) fn add_months_datetime<Tz: TimeZone>(
    dt: DateTime<Tz>,
    months: i32,
) -> Option<DateTime<Tz>> {
    match months.cmp(&0) {
        Ordering::Equal => Some(dt),
        Ordering::Greater => dt.checked_add_months(Months::new(months as u32)),
        Ordering::Less => dt.checked_sub_months(Months::new(months.unsigned_abs())),
    }
}

/// Add the given number of days to the given datetime.
///
/// Returns `None` when it will result in overflow.
pub(crate) fn add_days_datetime<Tz: TimeZone>(dt: DateTime<Tz>, days: i32) -> Option<DateTime<Tz>> {
    match days.cmp(&0) {
        Ordering::Equal => Some(dt),
        Ordering::Greater => dt.checked_add_days(Days::new(days as u64)),
        Ordering::Less => dt.checked_sub_days(Days::new(days.unsigned_abs() as u64)),
    }
}

/// Substract the given number of months to the given datetime.
///
/// Returns `None` when it will result in overflow.
pub(crate) fn sub_months_datetime<Tz: TimeZone>(
    dt: DateTime<Tz>,
    months: i32,
) -> Option<DateTime<Tz>> {
    match months.cmp(&0) {
        Ordering::Equal => Some(dt),
        Ordering::Greater => dt.checked_sub_months(Months::new(months as u32)),
        Ordering::Less => dt.checked_add_months(Months::new(months.unsigned_abs())),
    }
}

/// Substract the given number of days to the given datetime.
///
/// Returns `None` when it will result in overflow.
pub(crate) fn sub_days_datetime<Tz: TimeZone>(dt: DateTime<Tz>, days: i32) -> Option<DateTime<Tz>> {
    match days.cmp(&0) {
        Ordering::Equal => Some(dt),
        Ordering::Greater => dt.checked_sub_days(Days::new(days as u64)),
        Ordering::Less => dt.checked_add_days(Days::new(days.unsigned_abs() as u64)),
    }
}

#[cfg(test)]
mod tests {

    use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};

    use super::*;

    #[test]
    fn test_shift_months() {
        let base = NaiveDate::from_ymd_opt(2020, 1, 31).unwrap();

        assert_eq!(
            shift_months(base, 0),
            NaiveDate::from_ymd_opt(2020, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 1),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            shift_months(base, 2),
            NaiveDate::from_ymd_opt(2020, 3, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 3),
            NaiveDate::from_ymd_opt(2020, 4, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 4),
            NaiveDate::from_ymd_opt(2020, 5, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 5),
            NaiveDate::from_ymd_opt(2020, 6, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 6),
            NaiveDate::from_ymd_opt(2020, 7, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 7),
            NaiveDate::from_ymd_opt(2020, 8, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 8),
            NaiveDate::from_ymd_opt(2020, 9, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 9),
            NaiveDate::from_ymd_opt(2020, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 10),
            NaiveDate::from_ymd_opt(2020, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 11),
            NaiveDate::from_ymd_opt(2020, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 12),
            NaiveDate::from_ymd_opt(2021, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 13),
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );

        assert_eq!(
            shift_months(base, -1),
            NaiveDate::from_ymd_opt(2019, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -2),
            NaiveDate::from_ymd_opt(2019, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, -3),
            NaiveDate::from_ymd_opt(2019, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -4),
            NaiveDate::from_ymd_opt(2019, 9, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, -5),
            NaiveDate::from_ymd_opt(2019, 8, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -6),
            NaiveDate::from_ymd_opt(2019, 7, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -7),
            NaiveDate::from_ymd_opt(2019, 6, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, -8),
            NaiveDate::from_ymd_opt(2019, 5, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -9),
            NaiveDate::from_ymd_opt(2019, 4, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, -10),
            NaiveDate::from_ymd_opt(2019, 3, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -11),
            NaiveDate::from_ymd_opt(2019, 2, 28).unwrap()
        );
        assert_eq!(
            shift_months(base, -12),
            NaiveDate::from_ymd_opt(2019, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -13),
            NaiveDate::from_ymd_opt(2018, 12, 31).unwrap()
        );

        assert_eq!(
            shift_months(base, 1265),
            NaiveDate::from_ymd_opt(2125, 6, 30).unwrap()
        );
    }

    #[test]
    fn test_shift_months_with_overflow() {
        let base = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();

        assert_eq!(shift_months(base, 0), base);
        assert_eq!(
            shift_months(base, 1),
            NaiveDate::from_ymd_opt(2021, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 2),
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );
        assert_eq!(
            shift_months(base, 12),
            NaiveDate::from_ymd_opt(2021, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 18),
            NaiveDate::from_ymd_opt(2022, 6, 30).unwrap()
        );

        assert_eq!(
            shift_months(base, -1),
            NaiveDate::from_ymd_opt(2020, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, -2),
            NaiveDate::from_ymd_opt(2020, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -10),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            shift_months(base, -12),
            NaiveDate::from_ymd_opt(2019, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, -18),
            NaiveDate::from_ymd_opt(2019, 6, 30).unwrap()
        );
    }

    #[test]
    fn test_shift_months_datetime() {
        let date = NaiveDate::from_ymd_opt(2020, 1, 31).unwrap();
        let o_clock = NaiveTime::from_hms_opt(1, 2, 3).unwrap();

        let base = NaiveDateTime::new(date, o_clock);

        assert_eq!(
            shift_months(base, 0).date(),
            NaiveDate::from_ymd_opt(2020, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 1).date(),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            shift_months(base, 2).date(),
            NaiveDate::from_ymd_opt(2020, 3, 31).unwrap()
        );
        assert_eq!(shift_months(base, 0).time(), o_clock);
        assert_eq!(shift_months(base, 1).time(), o_clock);
        assert_eq!(shift_months(base, 2).time(), o_clock);
    }
}
