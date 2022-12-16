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
import tempfile
import subprocess
import pathlib


def create_data_and_df():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.conf.set("parquet.bloom.filter.enabled", True)
    spark.conf.set("parquet.bloom.filter.expected.ndv", 10)
    spark.conf.set("parquet.bloom.filter.max.bytes", 32)
    data = [(f"id-{i % 10}", f"name-{i%10}") for i in range(100)]
    df = spark.createDataFrame(data, ["id", "name"]).repartition(1)
    return data, df


def get_expected_output(data):
    expected = ["Row group #0", "=" * 80]
    for v in data:
        expected.append(f"Value {v[0]} is present in bloom filter")
    for v in data:
        expected.append(f"Value {v[1]} is absent in bloom filter")
    expected = "\n".join(expected) + "\n"
    return expected.encode("utf-8")


def get_cli_output(output_dir, data, col_name="id"):
    # take the first (and only) parquet file
    parquet_file = sorted(pathlib.Path(output_dir).glob("*.parquet"))[0]
    args = [
        "parquet-show-bloom-filter",
        "--file-name",
        parquet_file,
        "--column",
        col_name,
    ]
    for v in data:
        args.extend(["--values", v[0]])
    for v in data:
        args.extend(["--values", v[1]])
    return subprocess.check_output(args)


def test_pyspark_bloom_filter():
    data, df = create_data_and_df()
    with tempfile.TemporaryDirectory() as output_dir:
        df.write.parquet(output_dir, mode="overwrite")
        cli_output = get_cli_output(output_dir, data)
        assert cli_output == get_expected_output(data)
