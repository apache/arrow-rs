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

use arrow_cast::parse::string_to_datetime;
use chrono::Utc;

#[test]
fn test_parse_timezone() {
    let cases = [
        (
            "2023-01-01 040506 America/Los_Angeles",
            "2023-01-01T12:05:06+00:00",
        ),
        (
            "2023-01-01 04:05:06.345 America/Los_Angeles",
            "2023-01-01T12:05:06.345+00:00",
        ),
        (
            "2023-01-01 04:05:06.345 America/Los_Angeles",
            "2023-01-01T12:05:06.345+00:00",
        ),
        (
            "2023-01-01 04:05:06.789 -08",
            "2023-01-01T12:05:06.789+00:00",
        ),
        (
            "2023-03-12 040506 America/Los_Angeles",
            "2023-03-12T11:05:06+00:00",
        ), // Daylight savings
    ];

    for (s, expected) in cases {
        let actual = string_to_datetime(&Utc, s).unwrap().to_rfc3339();
        assert_eq!(actual, expected, "{s}")
    }
}

#[test]
fn test_parse_timezone_invalid() {
    let cases = [
        (
            "2015-01-20T17:35:20-24:00",
            "Parser error: Invalid timezone \"-24:00\": failed to parse timezone",
        ),
        (
            "2023-01-01 04:05:06.789 +07:30:00",
            "Parser error: Invalid timezone \"+07:30:00\": failed to parse timezone"
        ),
        (
            // Sunday, 12 March 2023, 02:00:00 clocks are turned forward 1 hour to
            // Sunday, 12 March 2023, 03:00:00 local daylight time instead.
            "2023-03-12 02:05:06 America/Los_Angeles",
            "Parser error: Error parsing timestamp from '2023-03-12 02:05:06 America/Los_Angeles': error computing timezone offset",
        ),
        (
            // Sunday, 5 November 2023, 02:00:00 clocks are turned backward 1 hour to
            // Sunday, 5 November 2023, 01:00:00 local standard time instead.
            "2023-11-05 01:30:06 America/Los_Angeles",
            "Parser error: Error parsing timestamp from '2023-11-05 01:30:06 America/Los_Angeles': error computing timezone offset",
        ),
    ];

    for (s, expected) in cases {
        let actual = string_to_datetime(&Utc, s).unwrap_err().to_string();
        assert_eq!(actual, expected)
    }
}
