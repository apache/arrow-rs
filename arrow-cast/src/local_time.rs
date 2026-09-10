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

//! Resolving wall clock ("local") readings to instants.
//!
//! Reading a [`NaiveDateTime`] as a wall clock time in an IANA timezone does not
//! always identify a unique instant, because of daylight savings transitions.
//! Everything that has to make that choice -- the cast kernel and the string
//! parser -- goes through [`resolve_local_offset`] so that the two cannot drift
//! apart.

use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeDelta, TimeZone};

/// Returns the offset to use when interpreting `local` as a wall clock reading
/// in `tz`, or `None` if it cannot be resolved.
///
/// In an IANA timezone a wall clock reading does not always identify a unique
/// instant, and this function picks one following the same rules as PostgreSQL
/// and DuckDB:
///
/// * **Ambiguous** -- when the clocks go back ("fall back") the same reading
///   occurs twice. The *later* instant is chosen, i.e. the offset in effect
///   after the transition. For example `2024-11-03T01:30:00` in
///   `America/New_York` is read as `-05:00` (EST), not `-04:00` (EDT).
/// * **Nonexistent** -- when the clocks go forward ("spring forward") the
///   reading never occurs. It is shifted forward by the length of the gap,
///   which is the same as reading it with the offset in effect *before* the
///   transition. For example `2024-03-10T02:30:00` in `America/New_York` is
///   read as `-05:00` (EST) and therefore denotes `2024-03-10T03:30:00-04:00`.
///
/// Timezones with a fixed offset are never ambiguous and have no gaps.
///
/// See <https://github.com/apache/arrow-rs/issues/11037> for the PostgreSQL and
/// ICU (DuckDB) sources these rules are taken from.
pub(crate) fn resolve_local_offset<T: TimeZone>(
    tz: &T,
    local: &NaiveDateTime,
) -> Option<T::Offset> {
    match tz.offset_from_local_datetime(local) {
        LocalResult::Single(offset) => Some(offset),
        // The second offset of `Ambiguous` is the one that yields the later instant.
        LocalResult::Ambiguous(_, later) => Some(later),
        LocalResult::None => {
            // The reading falls in a gap. Recover the offset in effect before the
            // transition by probing 24 hours earlier: the timezone database
            // contains no two transitions within 24 hours of each other, so that
            // probe lands on the other side of this transition and is itself
            // resolvable. If it somehow is not, give up and let the caller apply
            // the usual error / null handling.
            tz.offset_from_local_datetime(&(*local - TimeDelta::hours(24)))
                .earliest()
        }
    }
}

/// Reads `local` as a wall clock time in `tz`, resolving daylight savings
/// ambiguities and gaps as described on [`resolve_local_offset`].
///
/// This is [`TimeZone::from_local_datetime`] with the two unresolvable
/// [`LocalResult`] cases decided rather than rejected.
pub(crate) fn resolve_local_datetime<T: TimeZone>(
    tz: &T,
    local: &NaiveDateTime,
) -> Option<DateTime<T>> {
    let offset = resolve_local_offset(tz, local)?;
    Some(tz.from_utc_datetime(&(*local - offset.fix())))
}
