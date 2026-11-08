// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Timezone for timestamp arrays
//!
//! Without additional features, [`Tz`] only supports fixed offsets such as `+09:00`. The
//! `chrono-tz` and `jiff` features add named timezones from the [IANA time zone database], such
//! as `America/Los_Angeles`:
//!
//! * `chrono-tz` resolves names with [chrono-tz], which precomputes each zone's transitions up to
//!   2099 and keeps the last offset for any later instant, so daylight saving no longer changes it.
//! * `jiff` resolves names with [jiff], which applies each zone's daylight saving rules to any
//!   instant, as `java.time` and libc do.
//!
//! When both are enabled, `jiff` is used. Cargo unifies features across a build, so enabling
//! `jiff` for any crate changes how named timezones are resolved for every crate in the build.
//!
//! Each feature bundles its own copy of the database, which may be a different release, so the two
//! can also disagree wherever the database changed between releases. [`tzdb_version`] returns the
//! release in use.
//!
//! [IANA time zone database]: https://www.iana.org/time-zones
//! [chrono-tz]: https://docs.rs/chrono-tz
//! [jiff]: https://docs.rs/jiff

use arrow_schema::ArrowError;
use chrono::{DateTime, FixedOffset, NaiveDateTime};
pub use private::{Tz, TzOffset};

/// Returns the release of the [IANA time zone database] that resolves named timezones, such as
/// `"2026c"`.
///
/// Returns `None` when neither the `chrono-tz` nor the `jiff` feature is enabled, as only fixed
/// offsets are then supported, or if the bundled database does not record its release.
///
/// [IANA time zone database]: https://www.iana.org/time-zones
pub fn tzdb_version() -> Option<&'static str> {
    #[cfg(feature = "jiff")]
    {
        jiff_tzdb::VERSION
    }
    #[cfg(all(feature = "chrono-tz", not(feature = "jiff")))]
    {
        Some(chrono_tz::IANA_TZDB_VERSION)
    }
    #[cfg(not(any(feature = "chrono-tz", feature = "jiff")))]
    {
        None
    }
}

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
    use std::sync::OnceLock;

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

    /// A named timezone resolved by jiff, together with its name.
    struct NamedTz {
        name: &'static str,
        tz: JiffTimeZone,
    }

    /// Prints only the name, so that `Debug` output matches the `chrono-tz` feature.
    impl std::fmt::Debug for NamedTz {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.name)
        }
    }

    /// Builds a timezone from the database in jiff-tzdb.
    ///
    /// Names are not resolved through `jiff::tz::db()`: if any crate in the build enables jiff's
    /// `tzdb-zoneinfo` feature, that reads the host's zoneinfo files, or `$TZDIR`, instead. Reading
    /// jiff-tzdb directly also means [`tzdb_version`] always describes the data in use.
    fn bundled_tz(name: &str) -> Option<JiffTimeZone> {
        // As jiff's own bundled database does, so that UTC is not backed by TZif data
        if name == "UTC" {
            return Some(JiffTimeZone::UTC);
        }
        let (canonical_name, tzif) = jiff_tzdb::get(name)?;
        JiffTimeZone::tzif(canonical_name, tzif).ok()
    }

    /// Resolves a name in the bundled database. Unlike jiff, and like chrono-tz, the name must
    /// match exactly, including its case.
    ///
    /// [`Tz`] is `Copy` whereas [`JiffTimeZone`] is reference counted, so `Tz` holds a `'static`
    /// reference into a table with one slot for each name in the database, each resolved on first
    /// use. The table's keys are fixed when it is built, so its memory is bounded by the database
    /// and lookups take no lock.
    fn named_tz(name: &str) -> Result<&'static NamedTz, ArrowError> {
        type Slots = HashMap<&'static str, OnceLock<Option<NamedTz>>, ahash::RandomState>;
        static SLOTS: OnceLock<Slots> = OnceLock::new();
        let slots = SLOTS.get_or_init(|| {
            jiff_tzdb::available()
                // A placeholder for an unknown local timezone, which chrono-tz does not accept
                .filter(|name| *name != "Factory")
                .map(|name| (name, OnceLock::new()))
                .collect()
        });
        slots
            .get_key_value(name)
            .and_then(|(name, slot)| {
                slot.get_or_init(|| {
                    let tz = bundled_tz(name)?;
                    Some(NamedTz { name, tz })
                })
                .as_ref()
            })
            // Same message as the chrono-tz path, so callers see one error regardless of feature
            .ok_or_else(|| {
                ArrowError::ParseError(format!(
                    "Invalid timezone \"{name}\": failed to parse timezone"
                ))
            })
    }

    /// An Arrow [`TimeZone`]
    #[derive(Debug, Copy, Clone)]
    pub struct Tz(TzInner);

    #[derive(Debug, Copy, Clone)]
    enum TzInner {
        Timezone(&'static NamedTz),
        Offset(FixedOffset),
    }

    // These are copied per value by chrono, so catch any accidental growth
    #[cfg(target_pointer_width = "64")]
    const _: () = {
        assert!(std::mem::size_of::<Tz>() == 16);
        assert!(std::mem::size_of::<TzOffset>() == 24);
    };

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
                TzInner::Timezone(tz) => f.write_str(tz.name),
                TzInner::Offset(offset) => offset.fmt(f),
            }
        }
    }

    fn fixed_offset(offset: jiff::tz::Offset) -> FixedOffset {
        // The bundled database never holds an offset of a day or more, so this cannot fail
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
                    // Outside jiff's range, treat the local datetime as if it were UTC. Either
                    // way it takes the offset in force at the nearest covered instant.
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

    pub(super) fn new_gap_info(local: &NaiveDateTime, tz: &Tz) -> Option<GapInfo> {
        let TzInner::Timezone(named) = tz.0 else {
            return None;
        };
        // A local datetime outside jiff's range resolves to a single offset, so it is not in a gap
        let civil = jiff_civil(local)?;
        let AmbiguousOffset::Gap { before, after } =
            named.tz.to_ambiguous_timestamp(civil).offset()
        else {
            return None;
        };
        // Reading a local datetime in the gap with the later offset gives an instant before the
        // transition, so the next transition is the one that opened the gap
        let from = after.to_timestamp(civil).ok()?;
        let transition = named.tz.following(from).next()?;
        let end = DateTime::from_timestamp(transition.timestamp().as_second(), 0)?;
        let begin = end.naive_utc() + chrono::TimeDelta::seconds(before.seconds().into());
        Some(GapInfo {
            begin: Some((begin, tz.offset(fixed_offset(before)))),
            end: Some(end.with_timezone(tz)),
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

        /// chrono-tz keeps the offset of its last precomputed transition, around 2099, for any
        /// later instant: standard time in the northern hemisphere and daylight time in the
        /// southern. jiff applies the zone's rules indefinitely, as `java.time` and libc do.
        #[test]
        fn test_dst_applies_after_2099() {
            let hours = |h| FixedOffset::east_opt(h * 60 * 60).unwrap();
            // Each zone's offsets in January and July
            let zones = [
                ("America/Los_Angeles", hours(-8), hours(-7)),
                ("Australia/Sydney", hours(11), hours(10)),
            ];
            for (zone, january, july) in zones {
                let tz: Tz = zone.parse().unwrap();
                for year in [2024, 2100, 2300, 9999] {
                    for (month, expected) in [(1, january), (7, july)] {
                        let at = NaiveDate::from_ymd_opt(year, month, 1)
                            .unwrap()
                            .and_hms_opt(12, 0, 0)
                            .unwrap();
                        assert_eq!(
                            tz.offset_from_utc_datetime(&at).fix(),
                            expected,
                            "{zone} {at}"
                        );
                        match tz.offset_from_local_datetime(&at) {
                            LocalResult::Single(offset) => {
                                assert_eq!(offset.fix(), expected, "local {zone} {at}")
                            }
                            other => panic!("expected a single offset for {at}, got {other:?}"),
                        }
                    }
                }
            }
        }

        #[test]
        fn test_beyond_jiff_range() {
            let tz: Tz = "America/Los_Angeles".parse().unwrap();
            let at = |year| {
                NaiveDate::from_ymd_opt(year, 7, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap()
            };
            // jiff covers years -9999 to 9999. Beyond that, rather than failing, the offset in
            // force at the nearest covered instant applies: standard time at the end of 9999,
            // and local mean time before the first transition.
            let pacific_standard = FixedOffset::west_opt(8 * 60 * 60).unwrap();
            let local_mean_time = FixedOffset::west_opt(7 * 60 * 60 + 52 * 60 + 58).unwrap();
            for (year, expected) in [(20_000, pacific_standard), (-20_000, local_mean_time)] {
                assert_eq!(
                    tz.offset_from_utc_datetime(&at(year)).fix(),
                    expected,
                    "{year}"
                );
                match tz.offset_from_local_datetime(&at(year)) {
                    LocalResult::Single(offset) => {
                        assert_eq!(offset.fix(), expected, "local {year}")
                    }
                    other => panic!("expected a single offset for {year}, got {other:?}"),
                }
            }
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
        fn test_bundled_names() {
            for name in jiff_tzdb::available() {
                if name != "Factory" {
                    let tz: Tz = name.parse().unwrap();
                    assert_eq!(tz.to_string(), name);
                }
            }
        }

        #[test]
        fn test_invalid_timezone() {
            // jiff ignores case and knows some names that chrono-tz does not, but both features
            // accept the same names
            let names = [
                "Not/AZone",
                "america/los_angeles",
                "utc",
                "Etc/Unknown",
                "Factory",
            ];
            for name in names {
                let err = name.parse::<Tz>().unwrap_err().to_string();
                let expected = format!("Invalid timezone \"{name}\"");
                assert!(err.contains(&expected), "{err}");
            }
        }

        #[test]
        fn test_debug_matches_chrono_tz() {
            let tz: Tz = "America/Los_Angeles".parse().unwrap();
            assert_eq!(format!("{tz:?}"), "Tz(Timezone(America/Los_Angeles))");
            let offset = tz.offset_from_utc_datetime(
                &NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            );
            assert_eq!(
                format!("{offset:?}"),
                "TzOffset { tz: Tz(Timezone(America/Los_Angeles)), offset: -08:00 }"
            );
        }

        /// With `--features chrono-tz,jiff` both engines are compiled, so these compare them
        /// directly. They are expected to fail when either crate moves to a new release of the
        /// IANA database, which is how such a change gets noticed.
        #[cfg(feature = "chrono-tz")]
        mod compare_with_chrono_tz {
            use super::*;
            use std::collections::BTreeSet;

            /// Zones whose offsets differ only because the two engines bundle different releases.
            /// When this test fails after a dependency update, review the differences and update
            /// this list.
            const KNOWN_DIFFERENCES: &[&str] = &[
                "Africa/Casablanca",
                "Africa/El_Aaiun",
                "America/Edmonton",
                "America/Ensenada",
                "America/Santa_Isabel",
                "America/Tijuana",
                "America/Vancouver",
                "America/Yellowknife",
                "Canada/Mountain",
                "Canada/Pacific",
                "Europe/Chisinau",
                "Europe/Tiraspol",
                "Mexico/BajaNorte",
            ];

            fn releases() -> String {
                format!(
                    "chrono-tz has tzdb {}, jiff-tzdb has tzdb {}",
                    chrono_tz::IANA_TZDB_VERSION,
                    jiff_tzdb::VERSION.unwrap_or("unknown")
                )
            }

            fn at(seconds: i64) -> NaiveDateTime {
                DateTime::from_timestamp(seconds, 0).unwrap().naive_utc()
            }

            /// The parts of a gap that both engines report: where it begins and ends
            type Gap = (Option<(NaiveDateTime, FixedOffset)>, Option<NaiveDateTime>);

            fn chrono_tz_gap(tz: &chrono_tz::Tz, local: &NaiveDateTime) -> Option<Gap> {
                chrono_tz::GapInfo::new(local, tz).map(|gap| {
                    (
                        gap.begin.map(|(begin, offset)| (begin, offset.fix())),
                        gap.end.map(|end| end.naive_utc()),
                    )
                })
            }

            fn jiff_gap(tz: &Tz, local: &NaiveDateTime) -> Option<Gap> {
                GapInfo::new(local, tz).map(|gap| {
                    (
                        gap.begin.map(|(begin, offset)| (begin, offset.fix())),
                        gap.end.map(|end| end.naive_utc()),
                    )
                })
            }

            /// Whether the engines agree when `datetime` is read as UTC and as local time
            fn agree(chrono_tz: chrono_tz::Tz, jiff: Tz, datetime: &NaiveDateTime) -> bool {
                chrono_tz_gap(&chrono_tz, datetime) == jiff_gap(&jiff, datetime)
                    && chrono_tz.offset_from_utc_datetime(datetime).fix()
                        == jiff.offset_from_utc_datetime(datetime).fix()
                    && chrono_tz
                        .offset_from_local_datetime(datetime)
                        .map(|o| o.fix())
                        == jiff.offset_from_local_datetime(datetime).map(|o| o.fix())
            }

            #[test]
            fn test_same_names() {
                let chrono: BTreeSet<_> =
                    chrono_tz::TZ_VARIANTS.iter().map(|tz| tz.name()).collect();
                let jiff: BTreeSet<_> = jiff_tzdb::available()
                    .filter(|name| *name != "Factory")
                    .collect();
                assert_eq!(chrono, jiff, "{}", releases());
            }

            #[test]
            fn test_same_offsets() {
                // 1900-01-01 to 2099-01-01, before chrono-tz stops applying daylight saving
                const START: i64 = -2_208_988_800;
                const END: i64 = 4_070_908_800;
                const WEEK: usize = 7 * 24 * 60 * 60;

                let mut different = BTreeSet::new();
                for variant in chrono_tz::TZ_VARIANTS {
                    let name = variant.name();
                    let jiff: Tz = name.parse().unwrap();
                    let TzInner::Timezone(named) = jiff.0 else {
                        unreachable!("{name} is not an offset");
                    };

                    // Weekly samples find transitions that only one engine knows about
                    let mut same = (START..END)
                        .step_by(WEEK)
                        .all(|s| agree(variant, jiff, &at(s)));

                    // Probing each side of every transition covers gaps and folds
                    let start = Timestamp::from_second(START).unwrap();
                    let transitions = named.tz.following(start);
                    for t in transitions.take_while(|t| t.timestamp().as_second() < END) {
                        let instant = t.timestamp().as_second();
                        let after = i64::from(t.offset().seconds());
                        let before = i64::from(
                            named
                                .tz
                                .to_offset(Timestamp::from_second(instant - 1).unwrap())
                                .seconds(),
                        );
                        same &= [
                            instant - 1,
                            instant,
                            instant + before - 1,
                            instant + before,
                            instant + after - 1,
                            instant + after,
                        ]
                        .into_iter()
                        .all(|s| agree(variant, jiff, &at(s)));
                    }
                    if !same {
                        different.insert(name);
                    }
                }
                let known = KNOWN_DIFFERENCES.iter().copied().collect();
                assert_eq!(different, known, "{}", releases());
            }
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
/// # #[cfg(any(feature = "chrono-tz", feature = "jiff"))]
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
    fn test_tzdb_version() {
        let version = tzdb_version();
        if cfg!(any(feature = "chrono-tz", feature = "jiff")) {
            // Such as "2026c"
            let version = version.unwrap();
            assert_eq!(version.len(), 5, "{version}");
            assert!(
                version[..4].bytes().all(|b| b.is_ascii_digit()),
                "{version}"
            );
            assert!(version.as_bytes()[4].is_ascii_lowercase(), "{version}");
        } else {
            assert_eq!(version, None);
        }
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

    #[cfg(any(feature = "chrono-tz", feature = "jiff"))]
    mod gap_info {
        use super::*;
        use chrono::Utc;

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
    }
}
