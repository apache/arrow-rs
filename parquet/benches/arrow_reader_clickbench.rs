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

//! Benchmark for evaluating row filters and projections using the [ClickBench] queries and data.
//!
//! While the actual ClickBench queries often also include some sort of aggregation
//! or limit, this benchmark measures the raw speed of applying filtering
//! and projections, and optimize the performance of the `parquet` filter
//! evaluation in real world scenarios.
//!
//! This benchmark uses the hits_1.parquet file, is a 100,000 row samples of
//! the entire 100M row dataset. It is reasonable in size and speed to run
//! and seems to be a good representative of the entire dataset.
//!
//! See also `arrow_reader_row_filter` for more focused filter evaluation microbenchmarks
//!
//! [ClickBench]: https://benchmark.clickhouse.com/

use arrow::compute::kernels::cmp::{eq, neq};
use arrow::compute::{like, nlike, or};
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType, BooleanArray, PrimitiveArray, StringViewArray};
use arrow_schema::{ArrowError, DataType, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions,
    ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::schema::types::SchemaDescriptor;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

fn async_reader(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut async_group = c.benchmark_group("arrow_reader_clickbench/async");
    let handle = rt.handle();
    for query in all_queries() {
        let query_name = query.to_string();
        let read_test = ReadTest::new(query);
        async_group.bench_function(query_name, |b| {
            b.iter(|| handle.block_on(async { read_test.run_async().await }))
        });
    }
}

fn sync_reader(c: &mut Criterion) {
    let mut sync_group = c.benchmark_group("arrow_reader_clickbench/sync");
    for query in all_queries() {
        let query_name = query.to_string();
        let read_test = ReadTest::new(query);
        sync_group.bench_function(query_name, |b| b.iter(|| read_test.run_sync()));
    }
}

criterion_group!(benches, sync_reader, async_reader);
criterion_main!(benches);

/// Predicate Function.
///
/// Functions are invoked with the requested array and return a [`BooleanArray`]
/// as described in [`ArrowPredicate::evaluate`].
type ColumnPredicateFn =
    dyn FnMut(&ArrayRef) -> Result<BooleanArray, ArrowError> + Send + Sync + 'static;

/// ClickBench query pattern: a particular set of filter and projections used in the
/// [ClickBench queries] when run in [Apache DataFusion].
///
/// [ClickBench queries]: https://github.com/apache/datafusion/blob/main/benchmarks/queries/clickbench/queries.sql
/// [Apache DataFusion]: https://datafusion.apache.org/
struct Query {
    /// Human identifiable name
    name: &'static str,
    /// Which columns will be passed to the predicate functions?
    ///
    /// Must be in the same order as the columns in the schema
    filter_columns: Vec<&'static str>,
    /// Which columns will be projected (decoded after applying filters)
    projection_columns: Vec<&'static str>,
    /// Predicates to apply
    predicates: Vec<ClickBenchPredicate>,
    /// How many rows are expected to pass the predicate? This serves
    /// as a sanity check that the benchmark is working correctly.
    expected_row_count: usize,
}

/// Table that describes each relevant query pattern in the ClickBench queries
fn all_queries() -> Vec<Query> {
    vec![
        // Q0: SELECT COUNT(*) FROM hits;
        // (no filters)
        // Q1: SELECT COUNT(*) FROM hits WHERE "AdvEngineID" <> 0;
        Query {
            name: "Q1",
            filter_columns: vec!["AdvEngineID"],
            projection_columns: vec!["AdvEngineID"],
            predicates: vec![ClickBenchPredicate::neq_literal::<Int16Type>(0, 0)],
            expected_row_count: 3312,
        },
        // no filters in Q2-Q9, Q7 is same filter and projection as Q1
        // Q2: SELECT SUM("AdvEngineID"), COUNT(*), AVG("ResolutionWidth") FROM hits;
        // Q3: SELECT AVG("UserID") FROM hits;
        // Q4: SELECT COUNT(DISTINCT "UserID") FROM hits;
        // Q5: SELECT COUNT(DISTINCT "SearchPhrase") FROM hits;
        // Q6: SELECT MIN("EventDate"), MAX("EventDate") FROM hits;
        // Q7: SELECT "AdvEngineID", COUNT(*) FROM hits WHERE "AdvEngineID" <> 0 GROUP BY "AdvEngineID" ORDER BY COUNT(*) DESC;
        // Q8: SELECT "RegionID", COUNT(DISTINCT "UserID") AS u FROM hits GROUP BY "RegionID" ORDER BY u DESC LIMIT 10;
        // Q9: SELECT "RegionID", SUM("AdvEngineID"), COUNT(*) AS c, AVG("ResolutionWidth"), COUNT(DISTINCT "UserID") FROM hits GROUP BY "RegionID" ORDER BY c DESC LIMIT 10;
        // Q10: SELECT "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> '' GROUP BY "MobilePhoneModel" ORDER BY u DESC LIMIT 10;
        Query {
            name: "Q10",
            filter_columns: vec!["MobilePhoneModel"],
            projection_columns: vec!["MobilePhoneModel", "UserID"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 34276,
        },
        // Q11: SELECT "MobilePhone", "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> '' GROUP BY "MobilePhone", "MobilePhoneModel" ORDER BY u DESC LIMIT 10;
        Query {
            name: "Q11",
            filter_columns: vec!["MobilePhoneModel"],
            projection_columns: vec!["MobilePhone", "MobilePhoneModel", "UserID"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 34276,
        },
        // Q12: SELECT "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;
        Query {
            name: "Q12",
            filter_columns: vec!["SearchPhrase"],
            projection_columns: vec!["SearchPhrase"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 131559,
        },
        // Q13: SELECT "SearchPhrase", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY u DESC LIMIT 10;
        Query {
            name: "Q13",
            filter_columns: vec!["SearchPhrase"],
            projection_columns: vec!["SearchPhrase", "UserID"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 131559,
        },
        // Q14: SELECT "SearchEngineID", "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "SearchPhrase" ORDER BY c DESC LIMIT 10;
        Query {
            name: "Q14",
            filter_columns: vec!["SearchPhrase"],
            projection_columns: vec!["SearchEngineID", "SearchPhrase"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 131559,
        },
        // No predicates in Q15-Q18
        // Q15: SELECT "UserID", COUNT(*) FROM hits GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10;
        // Q16: SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;
        // Q17: SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" LIMIT 10;
        // Q18: SELECT "UserID", extract(minute FROM to_timestamp_seconds("EventTime")) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;
        // Q19: SELECT "UserID" FROM hits WHERE "UserID" = 435090932899640449;
        Query {
            name: "Q19",
            filter_columns: vec!["UserID"],
            projection_columns: vec!["UserID"],
            predicates: vec![
                // Original predicate is `UserID = 435090932899640449`
                // However, 435090932899640449 does not exist in hits_0.parquet. There are 4 rows in the total dataset:
                // ```sql
                // > select count(*) from 'hits.parquet' where "UserID" = 435090932899640449;
                // +----------+
                // | count(*) |
                // +----------+
                // | 4        |
                // +----------+
                // 1 row(s) fetched.
                // ```
                //
                // So use a user ID that actually exists in hits_0 and has 4 rows as well:
                //
                // `sql
                // > select "UserID", count(*) as c from 'hits_1.parquet' GROUP BY "UserID" HAVING c = 4;
                // +---------------------+---+
                // | UserID              | c |
                // +---------------------+---+
                // | 3233473875476175636 | 4 |
                // | ...                 | . |
                // +---------------------+---+
                // ```
                ClickBenchPredicate::eq_literal::<Int64Type>(0, 3233473875476175636),
            ],
            expected_row_count: 4,
        },
        // Q20: SELECT COUNT(*) FROM hits WHERE "URL" LIKE '%google%';
        Query {
            name: "Q20",
            filter_columns: vec!["URL"],
            projection_columns: vec!["URL"],
            predicates: vec![ClickBenchPredicate::like_google(0)],
            expected_row_count: 137,
        },
        // Q21: SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM hits WHERE "URL" LIKE '%google%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;
        Query {
            name: "Q21",
            filter_columns: vec!["URL", "SearchPhrase"],
            projection_columns: vec!["SearchPhrase", "URL"],
            predicates: vec![
                ClickBenchPredicate::like_google(0),
                ClickBenchPredicate::not_empty(1),
            ],
            expected_row_count: 16,
        },
        // Q22: SELECT "SearchPhrase", MIN("URL"), MIN("Title"), COUNT(*) AS c, COUNT(DISTINCT "UserID") FROM hits WHERE "Title" LIKE '%Google%' AND "URL" NOT LIKE '%.google.%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;
        Query {
            name: "Q22",
            filter_columns: vec!["Title", "URL", "SearchPhrase"],
            projection_columns: vec!["SearchPhrase", "URL", "Title", "UserID"],
            predicates: vec![
                ClickBenchPredicate::like_Google(0),
                ClickBenchPredicate::nlike_google(1),
                ClickBenchPredicate::not_empty(2),
            ],
            expected_row_count: 46,
        },
        // Q23: SELECT * FROM hits WHERE "URL" LIKE '%google%';
        Query {
            name: "Q23",
            filter_columns: vec!["URL"],
            projection_columns: vec!["*"], // all columns
            predicates: vec![ClickBenchPredicate::like_google(0)],
            expected_row_count: 137,
        },
        // Q24: SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "EventTime" LIMIT 10;
        Query {
            name: "Q24",
            filter_columns: vec!["SearchPhrase"],
            projection_columns: vec!["SearchPhrase", "EventTime"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 131559,
        },
        // Same filters and projection as Q12
        // Q25: SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "SearchPhrase" LIMIT 10;
        // Same filters and projection as Q24
        // Q26: SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "EventTime", "SearchPhrase" LIMIT 10;
        // Q27: SELECT "CounterID", AVG(length("URL")) AS l, COUNT(*) AS c FROM hits WHERE "URL" <> '' GROUP BY "CounterID" HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
        Query {
            name: "Q27",
            filter_columns: vec!["URL"],
            projection_columns: vec!["CounterID", "URL"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 999978,
        },
        // Q28: SELECT REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM hits WHERE "Referer" <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
        Query {
            name: "Q28",
            filter_columns: vec!["Referer"],
            projection_columns: vec!["Referer"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 925813,
        },
        // No predicates in Q29
        // Q29: SELECT SUM("ResolutionWidth"), SUM("ResolutionWidth" + 1), SUM("ResolutionWidth" + 2), SUM("ResolutionWidth" + 3), SUM("ResolutionWidth" + 4), SUM("ResolutionWidth" + 5), SUM("ResolutionWidth" + 6), SUM("ResolutionWidth" + 7), SUM("ResolutionWidth" + 8), SUM("ResolutionWidth" + 9), SUM("ResolutionWidth" + 10), SUM("ResolutionWidth" + 11), SUM("ResolutionWidth" + 12), SUM("ResolutionWidth" + 13), SUM("ResolutionWidth" + 14), SUM("ResolutionWidth" + 15), SUM("ResolutionWidth" + 16), SUM("ResolutionWidth" + 17), SUM("ResolutionWidth" + 18), SUM("ResolutionWidth" + 19), SUM("ResolutionWidth" + 20), SUM("ResolutionWidth" + 21), SUM("ResolutionWidth" + 22), SUM("ResolutionWidth" + 23), SUM("ResolutionWidth" + 24), SUM("ResolutionWidth" + 25), SUM("ResolutionWidth" + 26), SUM("ResolutionWidth" + 27), SUM("ResolutionWidth" + 28), SUM("ResolutionWidth" + 29), SUM("ResolutionWidth" + 30), SUM("ResolutionWidth" + 31), SUM("ResolutionWidth" + 32), SUM("ResolutionWidth" + 33), SUM("ResolutionWidth" + 34), SUM("ResolutionWidth" + 35), SUM("ResolutionWidth" + 36), SUM("ResolutionWidth" + 37), SUM("ResolutionWidth" + 38), SUM("ResolutionWidth" + 39), SUM("ResolutionWidth" + 40), SUM("ResolutionWidth" + 41), SUM("ResolutionWidth" + 42), SUM("ResolutionWidth" + 43), SUM("ResolutionWidth" + 44), SUM("ResolutionWidth" + 45), SUM("ResolutionWidth" + 46), SUM("ResolutionWidth" + 47), SUM("ResolutionWidth" + 48), SUM("ResolutionWidth" + 49), SUM("ResolutionWidth" + 50), SUM("ResolutionWidth" + 51), SUM("ResolutionWidth" + 52), SUM("ResolutionWidth" + 53), SUM("ResolutionWidth" + 54), SUM("ResolutionWidth" + 55), SUM("ResolutionWidth" + 56), SUM("ResolutionWidth" + 57), SUM("ResolutionWidth" + 58), SUM("ResolutionWidth" + 59), SUM("ResolutionWidth" + 60), SUM("ResolutionWidth" + 61), SUM("ResolutionWidth" + 62), SUM("ResolutionWidth" + 63), SUM("ResolutionWidth" + 64), SUM("ResolutionWidth" + 65), SUM("ResolutionWidth" + 66), SUM("ResolutionWidth" + 67), SUM("ResolutionWidth" + 68), SUM("ResolutionWidth" + 69), SUM("ResolutionWidth" + 70), SUM("ResolutionWidth" + 71), SUM("ResolutionWidth" + 72), SUM("ResolutionWidth" + 73), SUM("ResolutionWidth" + 74), SUM("ResolutionWidth" + 75), SUM("ResolutionWidth" + 76), SUM("ResolutionWidth" + 77), SUM("ResolutionWidth" + 78), SUM("ResolutionWidth" + 79), SUM("ResolutionWidth" + 80), SUM("ResolutionWidth" + 81), SUM("ResolutionWidth" + 82), SUM("ResolutionWidth" + 83), SUM("ResolutionWidth" + 84), SUM("ResolutionWidth" + 85), SUM("ResolutionWidth" + 86), SUM("ResolutionWidth" + 87), SUM("ResolutionWidth" + 88), SUM("ResolutionWidth" + 89) FROM hits;
        // Q30: SELECT "SearchEngineID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "ClientIP" ORDER BY c DESC LIMIT 10;
        Query {
            name: "Q30",
            filter_columns: vec!["SearchPhrase"],
            projection_columns: vec!["SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth"],
            predicates: vec![ClickBenchPredicate::not_empty(0)],
            expected_row_count: 131559,
        },
        // Same filters and projection as Q30
        // Q31: SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10;
        // No predicates in Q32-Q35
        // Q32: SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"), AVG("ResolutionWidth") FROM hits GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10;
        // Q33: SELECT "URL", COUNT(*) AS c FROM hits GROUP BY "URL" ORDER BY c DESC LIMIT 10;
        // Q34: SELECT 1, "URL", COUNT(*) AS c FROM hits GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10;
        // Q35: SELECT "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3, COUNT(*) AS c FROM hits GROUP BY "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3 ORDER BY c DESC LIMIT 10;
        // Q36: SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "DontCountHits" = 0 AND "IsRefresh" = 0 AND "URL" <> '' GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10;
        Query {
            name: "Q36",
            filter_columns: vec![
                "CounterID",
                "EventDate",
                "DontCountHits",
                "IsRefresh",
                "URL",
            ],
            projection_columns: vec!["URL"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // For now, elide the EventDate predicates
                // The predicates don't contribute to the filter evaluation
                //
                // Note the predicates on EventDate are not pushed down into the Parquet scan in DataFusion
                // This is because the EventDate column is a Int16 and DataFusion doesn't automatically
                // coerce from Int16 to Date
                //
                // DataFusion does `CAST(EventDate AS VARCHAR)` which is not
                // obviously correct in this case.
                //
                // You can cast the Int16 to Date like this:
                // ```sql
                // > select "EventDate"::int::date from 'hits.parquet' limit 10;
                // +------------------------+
                // | hits.parquet.EventDate |
                // +------------------------+
                // | 2013-07-28             |
                // | ....                   |
                // +------------------------+
                // ```
                //
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::eq_literal::<Int16Type>(3, 0),
                ClickBenchPredicate::not_empty(4),
            ],
            expected_row_count: 181198,
        },
        // Q37: SELECT "Title", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "DontCountHits" = 0 AND "IsRefresh" = 0 AND "Title" <> '' GROUP BY "Title" ORDER BY PageViews DESC LIMIT 10;
        Query {
            name: "Q37",
            filter_columns: vec![
                "CounterID",
                "EventDate",
                "DontCountHits",
                "IsRefresh",
                "Title",
            ],
            projection_columns: vec!["Title"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                //ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                //ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::eq_literal::<Int16Type>(3, 0),
                ClickBenchPredicate::not_empty(4),
            ],
            expected_row_count: 178323,
        },
        // Q38: SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "IsRefresh" = 0 AND "IsLink" <> 0 AND "IsDownload" = 0 GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
        Query {
            name: "Q38",
            filter_columns: vec![
                "CounterID",
                "EventDate",
                "IsRefresh",
                "IsLink",
                "IsDownload",
            ],
            projection_columns: vec!["URL"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::neq_literal::<Int16Type>(3, 0),
                ClickBenchPredicate::neq_literal::<Int16Type>(4, 0),
            ],
            expected_row_count: 419,
        },
        // Q39:  SELECT "TraficSourceID", "SearchEngineID", "AdvEngineID", CASE WHEN ("SearchEngineID" = 0 AND "AdvEngineID" = 0) THEN "Referer" ELSE '' END AS Src, "URL" AS Dst, COUNT(*) AS PageViews FROM hits
        // WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "IsRefresh" = 0 GROUP BY "TraficSourceID", "SearchEngineID", "AdvEngineID", Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
        Query {
            name: "Q39",
            filter_columns: vec!["CounterID", "EventDate", "IsRefresh"],
            projection_columns: vec![
                "TraficSourceID",
                "SearchEngineID",
                "AdvEngineID",
                "Referer",
                "URL",
            ],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
            ],
            expected_row_count: 194225,
        },
        // Q40: SELECT "URLHash", "EventDate", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "IsRefresh" = 0 AND "TraficSourceID" IN (-1, 6) AND "RefererHash" = 3594120000172545465 GROUP BY "URLHash", "EventDate" ORDER BY PageViews DESC LIMIT 10 OFFSET 100;
        Query {
            name: "Q40",
            filter_columns: vec![
                "CounterID",
                "EventDate",
                "IsRefresh",
                "TraficSourceID",
                "RefererHash",
            ],
            projection_columns: vec!["URLHash", "EventDate"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::in_list::<Int16Type>(3, (-1, 6)), // IN -1, 6
                ClickBenchPredicate::eq_literal::<Int64Type>(4, 3594120000172545465),
            ],
            expected_row_count: 24793,
        },
        // Q41: SELECT "WindowClientWidth", "WindowClientHeight", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "IsRefresh" = 0 AND "DontCountHits" = 0 AND "URLHash" = 2868770270353813622 GROUP BY "WindowClientWidth", "WindowClientHeight" ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;
        Query {
            name: "Q41",
            filter_columns: vec![
                "CounterID",
                "EventDate",
                "IsRefresh",
                "DontCountHits",
                "URLHash",
            ],
            projection_columns: vec!["WindowClientWidth", "WindowClientHeight"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-01")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-31")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::eq_literal::<Int16Type>(3, 0),
                ClickBenchPredicate::eq_literal::<Int64Type>(4, 2868770270353813622),
            ],
            expected_row_count: 29201,
        },
        // Q42: SELECT DATE_TRUNC('minute', to_timestamp_seconds("EventTime")) AS M, COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-14' AND "EventDate" <= '2013-07-15' AND "IsRefresh" = 0 AND "DontCountHits" = 0 GROUP BY DATE_TRUNC('minute', to_timestamp_seconds("EventTime")) ORDER BY DATE_TRUNC('minute', M) LIMIT 10 OFFSET 1000;
        Query {
            name: "Q42",
            filter_columns: vec!["CounterID", "EventDate", "IsRefresh", "DontCountHits"],
            projection_columns: vec!["EventTime"],
            predicates: vec![
                ClickBenchPredicate::eq_literal::<Int32Type>(0, 62),
                // see Q36 for EventDate predicates
                // ClickBenchPredicate::gt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-14")),
                // ClickBenchPredicate::lt_eq_literal::<Int16Type>(1, str_to_i16_date("2013-07-15")),
                ClickBenchPredicate::eq_literal::<Int16Type>(2, 0),
                ClickBenchPredicate::eq_literal::<Int16Type>(3, 0),
            ],
            expected_row_count: 181200,
        },
    ]
}

/// Evaluate a predicate the input column specified offset relative to
/// the provided filter column
///
/// This provides a level of indirection needed because the columns passed to
/// ArrowPredicateFn is in the order of the schema rather than the order of the
/// specified predicate columns
struct ClickBenchPredicate {
    column_index: usize,
    /// Function that makes the actual [`ColumnPredicateFn`]
    ///
    /// This is necessary (and awkward) because  `ArrowPredicateFn` does not
    /// implement `Clone`, so it must be created for each reader instance.
    predicate_factory: Box<dyn Fn() -> Box<ColumnPredicateFn>>,
}

impl ClickBenchPredicate {
    /// Create a new `ClickBenchPredicate`
    ///
    /// Parameters:
    /// * `column_index`: the index of the column in the `filter_columns` list
    /// * `f`: creates the actual predicate function
    fn new<F: Fn() -> Box<ColumnPredicateFn> + 'static>(
        column_index: usize,
        predicate_factory: F,
    ) -> ClickBenchPredicate {
        Self {
            column_index,
            predicate_factory: Box::new(predicate_factory),
        }
    }

    fn column_index(&self) -> usize {
        self.column_index
    }

    /// return a predicate function
    fn predicate_fn(&self) -> Box<ColumnPredicateFn> {
        (self.predicate_factory)()
    }

    /// Create Predicate: col = literal
    fn eq_literal<T: ArrowPrimitiveType>(column_index: usize, literal_value: T::Native) -> Self {
        Self::new(column_index, move || {
            let literal = PrimitiveArray::<T>::new_scalar(literal_value);
            Box::new(move |col| eq(col, &literal))
        })
    }

    /// Create Predicate: col IN (lit1, lit2)
    fn in_list<T: ArrowPrimitiveType>(
        column_index: usize,
        literal_values: (T::Native, T::Native),
    ) -> Self {
        Self::new(column_index, move || {
            let literal_1 = PrimitiveArray::<T>::new_scalar(literal_values.0);
            let literal_2 = PrimitiveArray::<T>::new_scalar(literal_values.1);
            Box::new(move |col| {
                // evaluate like (col = lit1) OR (col = lit2)
                let match1 = eq(&col, &literal_1)?;
                let match2 = eq(&col, &literal_2)?;
                or(&match1, &match2)
            })
        })
    }

    /// Create predicate: col != ''
    fn neq_literal<T: ArrowPrimitiveType>(column_index: usize, literal_value: T::Native) -> Self {
        Self::new(column_index, move || {
            let literal = PrimitiveArray::<T>::new_scalar(literal_value);
            Box::new(move |col| neq(col, &literal))
        })
    }

    /// Create Predicate: col <> ''
    fn not_empty(column_index: usize) -> Self {
        Self::new(column_index, move || {
            let empty_string = StringViewArray::new_scalar("");
            Box::new(move |col| neq(col, &empty_string))
        })
    }

    /// Create Predicate: col LIKE '%google%'
    fn like_google(column_index: usize) -> Self {
        Self::new(column_index, move || {
            let google_url = StringViewArray::new_scalar("%google%");
            Box::new(move |col| like(col, &google_url))
        })
    }

    /// Create Predicate: col NOT LIKE '%google%'
    fn nlike_google(column_index: usize) -> Self {
        Self::new(column_index, move || {
            let google_url = StringViewArray::new_scalar("%google%");
            Box::new(move |col| nlike(col, &google_url))
        })
    }

    /// Create Predicate: col LIKE '%Google%'
    #[allow(non_snake_case)]
    fn like_Google(column_index: usize) -> Self {
        Self::new(column_index, move || {
            let google_url = StringViewArray::new_scalar("%Google%");
            Box::new(move |col| like(col, &google_url))
        })
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// FULL path to the ClickBench hits_1.parquet file
static HITS_1_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Finds the paths to the ClickBench file, or panics with a useful message
/// explaining how to download if it is not found
fn hits_1() -> &'static Path {
    HITS_1_PATH.get_or_init(|| {

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    println!(
        "Looking for ClickBench files starting in current_dir and all parent directories: {current_dir:?}"

    );

    let Some(hits_1_path) = find_file_if_exists(current_dir.clone(), "hits_1.parquet") else {
        eprintln!(
            "Could not find hits_1.parquet in directory or parents: {current_dir:?}. Download it via",
        );
        eprintln!();
        eprintln!("wget --continue https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet");
        panic!("Stopping");
    };

    hits_1_path
    })
}

/// Searches for `file_name` in `current_dir` and all parent directories
fn find_file_if_exists(mut current_dir: PathBuf, file_name: &str) -> Option<PathBuf> {
    loop {
        let potential_file_path = current_dir.join(file_name);
        if potential_file_path.exists() {
            return Some(potential_file_path);
        }
        // didn't find in current path, try parent
        let Some(parent) = current_dir.parent() else {
            break;
        };
        current_dir = parent.to_path_buf();
    }
    None
}

/// Represents a mapping from each column selected in the `ProjectionMask`
/// created from `filter_columns`, to the corresponding index in the list of
/// `filter_columns`?
///
/// # Example
///
/// If:
/// * the file schema has columns `[A, B, C]`
/// * `filter_columns` is `[C, A]`
/// * ==> `ProjectionMask` will be `[true, false, true]` = `[A, C]`
///
/// `FilterIndices` will be `[1, 0]`, because column `C` (index 0 in
/// filter_columns) is selected at index 1 of the `ProjectionMask` and column
/// `A` (index 1 in `filter_columns`) is selected at index 0 of the
/// `ProjectionMask`.
struct FilterIndices {
    /// * index is offset in Query::filter_columns
    /// * value is offset in column selected by filter ProjectionMask
    inner: Vec<usize>,
}

impl FilterIndices {
    /// Create a new `FilterIndices` from a list of column indices
    ///
    /// Parameters:
    /// * `schema_descriptor`: The schema of the file
    /// * `filter_schema_indices`: a list of column indices in the schema
    fn new(schema_descriptor: &SchemaDescriptor, filter_schema_indices: Vec<usize>) -> Self {
        for &filter_index in &filter_schema_indices {
            assert!(filter_index < schema_descriptor.num_columns());
        }
        // When the columns are selected using a ProjectionMask, they are
        // returned in the order of the schema (not the order they were specified)
        //
        // So if the original schema indices are 5, 1, 3 (select the sixth and
        // second and fourth column),  the RecordBatch returned will select them
        // in order 1, 3, 5,
        //
        // Thus we need a map to convert back to the original selection order
        // `[1, 2, 0]`
        let mut reordered: Vec<_> = filter_schema_indices.iter().enumerate().collect();
        reordered.sort_by_key(|(_projection_idx, original_schema_idx)| **original_schema_idx);
        let mut inner = vec![0; reordered.len()];
        for (output_idx, (projection_idx, _original_schema_idx)) in
            reordered.into_iter().enumerate()
        {
            inner[projection_idx] = output_idx;
        }
        Self { inner }
    }

    /// Given the index of a column in `filter_columns`, return the index of the
    /// column in the columns selected from `ProjectionMask`
    fn map_column(&self, filter_columns_index: usize) -> usize {
        // The selection index is the index in the filter mask
        // The inner index is the index in the filter columns
        self.inner[filter_columns_index]
    }
}

/// Encapsulates the test parameters for a single benchmark
struct ReadTest {
    /// Human identifiable name
    name: &'static str,
    /// Metadata from Parquet file
    arrow_reader_metadata: ArrowReaderMetadata,
    /// Which columns in the file should be projected (decoded after filter)?
    projection_mask: ProjectionMask,
    /// Which columns in the file should be passed to the filter?
    filter_mask: ProjectionMask,
    /// Mapping from column selected in filter mask to `Query::filter_columns`
    filter_indices: FilterIndices,
    /// Predicates to apply
    predicates: Vec<ClickBenchPredicate>,
    /// How many rows are expected to pass the predicate?
    ///
    /// This value is a sanity check that the benchmark is working correctly.
    expected_row_count: usize,
}

impl ReadTest {
    fn new(query: Query) -> Self {
        let Query {
            name,
            filter_columns,
            projection_columns,
            predicates,
            expected_row_count,
        } = query;

        let arrow_reader_metadata = load_metadata(hits_1());
        let schema_descr = arrow_reader_metadata
            .metadata()
            .file_metadata()
            .schema_descr();

        // Determine the correct selection ("ProjectionMask")
        let projection_mask = if projection_columns.contains(&"*") {
            // * means all columns
            ProjectionMask::all()
        } else {
            let projection_schema_indices = column_indices(schema_descr, &projection_columns);
            ProjectionMask::leaves(schema_descr, projection_schema_indices)
        };

        let filter_schema_indices = column_indices(schema_descr, &filter_columns);
        let filter_mask =
            ProjectionMask::leaves(schema_descr, filter_schema_indices.iter().cloned());
        let filter_indices = FilterIndices::new(schema_descr, filter_schema_indices);

        Self {
            name,
            arrow_reader_metadata,
            projection_mask,
            filter_mask,
            filter_indices,
            predicates,
            expected_row_count,
        }
    }

    /// Run the filter and projection using the async `ParquetRecordBatchStream`
    /// reader:
    ///
    /// 1. open the file
    /// 2. apply the filter
    /// 3. read the projection_columns
    async fn run_async(&self) {
        let Ok(parquet_file) = tokio::fs::File::open(hits_1()).await else {
            panic!("Failed to open {:?}", hits_1());
        };

        // setup the reader
        let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
            parquet_file,
            self.arrow_reader_metadata.clone(),
        )
        .with_batch_size(8192)
        .with_projection(self.projection_mask.clone())
        .with_row_filter(self.row_filter())
        .build()
        .unwrap();

        // run the stream to its end
        let mut row_count = 0;
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let num_rows = b.num_rows();
            row_count += num_rows;
        }
        self.check_row_count(row_count);
    }

    /// Like [`Self::run_async`] but for the sync parquet reader
    fn run_sync(&self) {
        let Ok(parquet_file) = std::fs::File::open(hits_1()) else {
            panic!("Failed to open {:?}", hits_1());
        };

        // setup the reader
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            parquet_file,
            self.arrow_reader_metadata.clone(),
        )
        .with_batch_size(8192)
        .with_projection(self.projection_mask.clone())
        .with_row_filter(self.row_filter())
        .build()
        .unwrap();

        // run the stream to its end
        let mut row_count = 0;
        for b in reader {
            let b = b.unwrap();
            let num_rows = b.num_rows();
            row_count += num_rows;
        }
        self.check_row_count(row_count);
    }

    /// Return a `RowFilter` to apply to the reader.
    ///
    /// Note that since `RowFilter` does not implement Clone, we need to create
    /// the filter for each row
    fn row_filter(&self) -> RowFilter {
        // Note: The predicates are in terms columns in the filter mask
        // but the record batch passed back has columns in the order of the file
        // schema

        // Convert the predicates to ArrowPredicateFn to conform to the RowFilter API
        let arrow_predicates: Vec<_> = self
            .predicates
            .iter()
            .map(|pred| {
                let orig_column_index = pred.column_index();
                let column_index = self.filter_indices.map_column(orig_column_index);
                let mut predicate_fn = pred.predicate_fn();
                Box::new(ArrowPredicateFn::new(
                    self.filter_mask.clone(),
                    move |batch| (predicate_fn)(batch.column(column_index)),
                )) as Box<dyn ArrowPredicate>
            })
            .collect();

        RowFilter::new(arrow_predicates)
    }

    fn check_row_count(&self, row_count: usize) {
        let expected_row_count = self.expected_row_count;
        assert_eq!(
            row_count, expected_row_count,
            "Expected {} rows, but got {} in {}",
            expected_row_count, row_count, self.name,
        );
    }
}

/// Return a map from `column_names` in `filter_columns` to the index in the schema
fn column_indices(schema: &SchemaDescriptor, column_names: &Vec<&str>) -> Vec<usize> {
    let fields = schema.root_schema().get_fields();
    let mut indices = vec![];
    for &name in column_names {
        for (idx, field) in fields.iter().enumerate().take(schema.num_columns()) {
            if name == field.name() {
                indices.push(idx)
            }
        }
    }
    indices
}

/// Loads Parquet metadata from the given path, including page indexes
fn load_metadata(path: &Path) -> ArrowReaderMetadata {
    let file = std::fs::File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let orig_metadata =
        ArrowReaderMetadata::load(&file, options.clone()).expect("parquet-metadata loading failed");

    // Update the arrow schema so that it reads View types for binary and utf8 columns
    let new_fields = orig_metadata
        .schema()
        .fields()
        .iter()
        .map(|f| {
            //println!("Converting field: {:?}", f);
            // Read UTF8 fields as Utf8View
            //
            // The clickbench_partitioned dataset has textual fields listed as
            // binary for some historical reason so translate Binary to Utf8View
            if matches!(
                f.data_type(),
                DataType::Utf8 | DataType::Binary | DataType::BinaryView
            ) {
                let new_field = f.as_ref().clone().with_data_type(DataType::Utf8View);
                Arc::new(new_field)
            } else {
                // otherwise use the inferred field type
                Arc::clone(f)
            }
        })
        .collect::<Vec<_>>();

    let new_arrow_schema = Arc::new(Schema::new(new_fields));

    let new_options = options.with_schema(new_arrow_schema);
    ArrowReaderMetadata::try_new(Arc::clone(orig_metadata.metadata()), new_options).unwrap()
}
