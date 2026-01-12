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
use chrono::{DateTime, Days, Months, NaiveDate, TimeZone};
use std::cmp::Ordering;

/// Add the given number of months to the given date.
///
/// Returns `None` when it will result in overflow.
pub(crate) fn add_months_date(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    match months.cmp(&0) {
        Ordering::Equal => Some(date),
        Ordering::Greater => date.checked_add_months(Months::new(months as u32)),
        Ordering::Less => date.checked_sub_months(Months::new(months.unsigned_abs())),
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
