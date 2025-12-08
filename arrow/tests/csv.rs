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

use core::str;
use std::sync::Arc;

use arrow_array::*;
use arrow_schema::*;

#[test]
fn test_export_csv_timestamps() {
    let schema = Schema::new(vec![
        Field::new(
            "c1",
            DataType::Timestamp(TimeUnit::Millisecond, Some("Australia/Sydney".into())),
            true,
        ),
        Field::new("c2", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]);

    let c1 = TimestampMillisecondArray::from(
        // 1555584887 converts to 2019-04-18, 20:54:47 in time zone Australia/Sydney (AEST).
        // The offset (difference to UTC) is +10:00.
        // 1635577147 converts to 2021-10-30 17:59:07 in time zone Australia/Sydney (AEDT)
        // The offset (difference to UTC) is +11:00. Note that daylight savings is in effect on 2021-10-30.
        //
        vec![Some(1555584887378), Some(1635577147000)],
    )
    .with_timezone("Australia/Sydney".to_string());
    let c2 = TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let mut sw = Vec::new();
    let mut writer = arrow_csv::Writer::new(&mut sw);
    let batches = vec![&batch];
    for batch in batches {
        writer.write(batch).unwrap();
    }
    drop(writer);

    let left = "c1,c2
2019-04-18T20:54:47.378+10:00,2019-04-18T10:54:47.378
2021-10-30T17:59:07+11:00,2021-10-30T06:59:07\n";
    let right = str::from_utf8(&sw).unwrap();
    assert_eq!(left, right);
}
