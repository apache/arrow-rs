# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import pyspark.sql
import pandas as pd
from tempfile import NamedTemporaryFile, TemporaryDirectory
import subprocess
import pathlib
import pytest


def create_data_and_spark_df(n):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.conf.set("parquet.bloom.filter.enabled", True)
    spark.conf.set("parquet.bloom.filter.expected.ndv", 10)
    spark.conf.set("parquet.bloom.filter.max.bytes", 32)
    data = [(f"id-{i % 10}", f"name-{i%10}") for i in range(n)]
    df = spark.createDataFrame(data, ["id", "name"]).repartition(1)
    return data, df


def create_data_and_pandas_df(n):
    data = [(f"id-{i % 10}", f"name-{i%10}") for i in range(n)]
    df = pd.DataFrame(data, columns=["id", "name"])
    return data, df


def get_expected_output(data):
    expected = ["Row group #0", "=" * 80]
    for v in data:
        expected.append(f"Value {v[0]} is present in bloom filter")
    for v in data:
        expected.append(f"Value {v[1]} is absent in bloom filter")
    expected = "\n".join(expected) + "\n"
    return expected.encode("utf-8")


def get_from_csv_cli_output(schema_file, output_file, csv_file):
    args = [
        "parquet-fromcsv",
        "--schema",
        schema_file,
        "--enable-bloom-filter",
        "true",
        "--input-file",
        csv_file,
        "--output-file",
        output_file,
    ]
    return subprocess.check_output(args)


def get_show_filter_cli_output(output_dir, data, col_name="id"):
    # take the first (and only) parquet file
    (parquet_file,) = sorted(pathlib.Path(output_dir).glob("*.parquet"))
    args = [
        "parquet-show-bloom-filter",
        parquet_file,
        col_name,
    ]
    for v in data:
        args.extend([v[0]])
    for v in data:
        args.extend([v[1]])
    return subprocess.check_output(args)


SCHEMA = b"""message schema {
    required binary id (UTF8);
    required binary name (UTF8);
}"""


@pytest.mark.parametrize("n", [1, 10])
class TestParquetIntegration:
    def test_pyspark_bloom_filter(self, n):
        data, df = create_data_and_spark_df(n)
        with TemporaryDirectory() as output_dir:
            df.write.parquet(output_dir, mode="overwrite")
            cli_output = get_show_filter_cli_output(output_dir, data)
            assert cli_output == get_expected_output(data)

    def test_bloom_filter_round_trip(self, n):
        data, df = create_data_and_pandas_df(n)
        with NamedTemporaryFile(suffix=".csv") as csv_file, NamedTemporaryFile(
            suffix=".schema"
        ) as schema_file, TemporaryDirectory() as output_dir:
            schema_file.write(SCHEMA)
            schema_file.flush()
            df.to_csv(csv_file.name, index=False, header=True)
            parquet_file = pathlib.Path(output_dir) / "output.parquet"
            cli_output = get_from_csv_cli_output(
                schema_file.name, parquet_file, csv_file.name
            )
            assert cli_output == b""
            cli_output = get_show_filter_cli_output(output_dir, data)
            assert cli_output == get_expected_output(data)
