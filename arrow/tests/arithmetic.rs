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

use arrow_arith::numeric::{add, sub};
use arrow_arith::temporal::{date_part, DatePart};
use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::as_datetime_with_timezone;
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use chrono::{DateTime, TimeZone};

#[test]
fn test_temporal_array_timestamp_hour_with_timezone_using_chrono_tz() {
    let a =
        TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("Asia/Kolkata".to_string());
    let b = date_part(&a, DatePart::Hour).unwrap();
    let b = b.as_primitive::<Int32Type>();
    assert_eq!(15, b.value(0));
}

#[test]
fn test_temporal_array_timestamp_hour_with_dst_timezone_using_chrono_tz() {
    //
    // 1635577147 converts to 2021-10-30 17:59:07 in time zone Australia/Sydney (AEDT)
    // The offset (difference to UTC) is +11:00. Note that daylight savings is in effect on 2021-10-30.
    // When daylight savings is not in effect, Australia/Sydney has an offset difference of +10:00.

    let a = TimestampMillisecondArray::from(vec![Some(1635577147000)])
        .with_timezone("Australia/Sydney".to_string());
    let b = date_part(&a, DatePart::Hour).unwrap();
    let b = b.as_primitive::<Int32Type>();
    assert_eq!(17, b.value(0));
}

fn test_timestamp_with_timezone_impl<T: ArrowTimestampType>(tz_str: &str) {
    let tz: Tz = tz_str.parse().unwrap();

    let transform_array = |x: &dyn Array| -> Vec<DateTime<_>> {
        x.as_primitive::<T>()
            .values()
            .into_iter()
            .map(|x| as_datetime_with_timezone::<T>(*x, tz).unwrap())
            .collect()
    };

    let values = vec![
        tz.with_ymd_and_hms(1970, 1, 28, 23, 0, 0)
            .unwrap()
            .naive_utc(),
        tz.with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
            .unwrap()
            .naive_utc(),
        tz.with_ymd_and_hms(2010, 4, 1, 4, 0, 20)
            .unwrap()
            .naive_utc(),
        tz.with_ymd_and_hms(1960, 1, 30, 4, 23, 20)
            .unwrap()
            .naive_utc(),
        tz.with_ymd_and_hms(2023, 3, 25, 14, 0, 0)
            .unwrap()
            .naive_utc(),
    ]
    .into_iter()
    .map(|x| T::make_value(x).unwrap())
    .collect();

    let a = PrimitiveArray::<T>::new(values, None).with_timezone(tz_str);

    // IntervalYearMonth
    let b = IntervalYearMonthArray::from(vec![
        IntervalYearMonthType::make_value(0, 1),
        IntervalYearMonthType::make_value(5, 34),
        IntervalYearMonthType::make_value(-2, 4),
        IntervalYearMonthType::make_value(7, -4),
        IntervalYearMonthType::make_value(0, 1),
    ]);
    let r1 = add(&a, &b).unwrap();
    assert_eq!(
        &transform_array(r1.as_ref()),
        &[
            tz.with_ymd_and_hms(1970, 2, 28, 23, 0, 0).unwrap(),
            tz.with_ymd_and_hms(1977, 11, 1, 0, 0, 0).unwrap(),
            tz.with_ymd_and_hms(2008, 8, 1, 4, 0, 20).unwrap(),
            tz.with_ymd_and_hms(1966, 9, 30, 4, 23, 20).unwrap(),
            tz.with_ymd_and_hms(2023, 4, 25, 14, 0, 0).unwrap(),
        ]
    );

    let r2 = sub(&r1, &b).unwrap();
    assert_eq!(r2.as_ref(), &a);

    // IntervalDayTime
    let b = IntervalDayTimeArray::from(vec![
        IntervalDayTimeType::make_value(0, 0),
        IntervalDayTimeType::make_value(5, 454000),
        IntervalDayTimeType::make_value(-34, 0),
        IntervalDayTimeType::make_value(7, -4000),
        IntervalDayTimeType::make_value(1, 0),
    ]);
    let r3 = add(&a, &b).unwrap();
    assert_eq!(
        &transform_array(r3.as_ref()),
        &[
            tz.with_ymd_and_hms(1970, 1, 28, 23, 0, 0).unwrap(),
            tz.with_ymd_and_hms(1970, 1, 6, 0, 7, 34).unwrap(),
            tz.with_ymd_and_hms(2010, 2, 26, 4, 0, 20).unwrap(),
            tz.with_ymd_and_hms(1960, 2, 6, 4, 23, 16).unwrap(),
            tz.with_ymd_and_hms(2023, 3, 26, 14, 0, 0).unwrap(),
        ]
    );

    let r4 = sub(&r3, &b).unwrap();
    assert_eq!(r4.as_ref(), &a);

    // IntervalMonthDayNano
    let b = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(1, 0, 0),
        IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
        IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
        IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
        IntervalMonthDayNanoType::make_value(1, 0, 0),
    ]);
    let r5 = add(&a, &b).unwrap();
    assert_eq!(
        &transform_array(r5.as_ref()),
        &[
            tz.with_ymd_and_hms(1970, 2, 28, 23, 0, 0).unwrap(),
            tz.with_ymd_and_hms(1998, 10, 4, 23, 59, 17).unwrap(),
            tz.with_ymd_and_hms(1960, 9, 29, 4, 0, 33).unwrap(),
            tz.with_ymd_and_hms(1960, 7, 2, 4, 31, 33).unwrap(),
            tz.with_ymd_and_hms(2023, 4, 25, 14, 0, 0).unwrap(),
        ]
    );

    let r6 = sub(&r5, &b).unwrap();
    assert_eq!(
        &transform_array(r6.as_ref()),
        &[
            tz.with_ymd_and_hms(1970, 1, 28, 23, 0, 0).unwrap(),
            tz.with_ymd_and_hms(1970, 1, 2, 0, 0, 0).unwrap(),
            tz.with_ymd_and_hms(2010, 4, 2, 4, 0, 20).unwrap(),
            tz.with_ymd_and_hms(1960, 1, 31, 4, 23, 20).unwrap(),
            tz.with_ymd_and_hms(2023, 3, 25, 14, 0, 0).unwrap(),
        ]
    );
}

#[test]
fn test_timestamp_with_offset_timezone() {
    let timezones = ["+00:00", "+01:00", "-01:00", "+03:30"];
    for timezone in timezones {
        test_timestamp_with_timezone_impl::<TimestampSecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampMillisecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampMicrosecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampNanosecondType>(timezone);
    }
}

#[test]
fn test_timestamp_with_timezone() {
    let timezones = [
        "Europe/Paris",
        "Europe/London",
        "Africa/Bamako",
        "America/Dominica",
        "Asia/Seoul",
        "Asia/Shanghai",
    ];
    for timezone in timezones {
        test_timestamp_with_timezone_impl::<TimestampSecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampMillisecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampMicrosecondType>(timezone);
        test_timestamp_with_timezone_impl::<TimestampNanosecondType>(timezone);
    }
}
