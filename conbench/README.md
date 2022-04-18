<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Arrow Rust + Conbench Integration


## Quick start

```
$ cd ~/arrow-rs/conbench/
$ conda create -y -n conbench python=3.9
$ conda activate conbench
(conbench) $ pip install -r requirements.txt
(conbench) $ conbench arrow-rs
```

## Example output

```
{
    "batch_id": "b68c559358cc43a3aab02d893d2693f4",
    "context": {
        "benchmark_language": "Rust"
    },
    "github": {
        "commit": "ca33a0a50494f95840ade2e9509c3c3d4df35249",
        "repository": "https://github.com/dianaclarke/arrow-rs"
    },
    "info": {},
    "machine_info": {
        "architecture_name": "x86_64",
        "cpu_core_count": "8",
        "cpu_frequency_max_hz": "2400000000",
        "cpu_l1d_cache_bytes": "65536",
        "cpu_l1i_cache_bytes": "131072",
        "cpu_l2_cache_bytes": "4194304",
        "cpu_l3_cache_bytes": "0",
        "cpu_model_name": "Apple M1",
        "cpu_thread_count": "8",
        "gpu_count": "0",
        "gpu_product_names": [],
        "kernel_name": "20.6.0",
        "memory_bytes": "17179869184",
        "name": "diana",
        "os_name": "macOS",
        "os_version": "10.16"
    },
    "run_id": "08353595bde147fb9deebdb4facd019a",
    "stats": {
        "data": [
            "0.000287",
            "0.000286",
            "0.000285",
            "0.000281",
            "0.000282",
            "0.000277",
            "0.000277",
            "0.000286",
            "0.000279",
            "0.000282",
            "0.000277",
            "0.000277",
            "0.000282",
            "0.000276",
            "0.000281",
            "0.000281",
            "0.000281",
            "0.000281",
            "0.000281",
            "0.000284",
            "0.000288",
            "0.000278",
            "0.000276",
            "0.000278",
            "0.000275",
            "0.000275",
            "0.000275",
            "0.000275",
            "0.000281",
            "0.000284",
            "0.000277",
            "0.000277",
            "0.000278",
            "0.000282",
            "0.000281",
            "0.000284",
            "0.000282",
            "0.000279",
            "0.000280",
            "0.000281",
            "0.000281",
            "0.000286",
            "0.000278",
            "0.000278",
            "0.000281",
            "0.000276",
            "0.000284",
            "0.000281",
            "0.000276",
            "0.000276",
            "0.000279",
            "0.000283",
            "0.000282",
            "0.000278",
            "0.000281",
            "0.000284",
            "0.000279",
            "0.000276",
            "0.000278",
            "0.000283",
            "0.000282",
            "0.000276",
            "0.000281",
            "0.000279",
            "0.000276",
            "0.000277",
            "0.000283",
            "0.000279",
            "0.000281",
            "0.000283",
            "0.000279",
            "0.000282",
            "0.000283",
            "0.000278",
            "0.000281",
            "0.000282",
            "0.000278",
            "0.000276",
            "0.000281",
            "0.000278",
            "0.000276",
            "0.000282",
            "0.000281",
            "0.000282",
            "0.000280",
            "0.000281",
            "0.000282",
            "0.000280",
            "0.000282",
            "0.000280",
            "0.000280",
            "0.000282",
            "0.000278",
            "0.000284",
            "0.000290",
            "0.000282",
            "0.000281",
            "0.000281",
            "0.000281",
            "0.000278"
        ],
        "iqr": "0.000004",
        "iterations": 100,
        "max": "0.000290",
        "mean": "0.000280",
        "median": "0.000281",
        "min": "0.000275",
        "q1": "0.000278",
        "q3": "0.000282",
        "stdev": "0.000003",
        "time_unit": "s",
        "times": [],
        "unit": "s"
    },
    "tags": {
        "name": "nlike_utf8 scalar starts with",
        "suite": "nlike_utf8 scalar starts with"
    },
    "timestamp": "2022-02-09T02:33:26.792404+00:00"
}
```

## Debug with test benchmark

```
(conbench) $ cd ~/arrow-rs/conbench/
(conbench) $ conbench test --iterations=3

Benchmark result:
{
    "batch_id": "f4235d547e9d4f94925b54692e625d7d",
    "context": {
        "benchmark_language": "Python"
    },
    "github": {
        "commit": "35e16be01e680e9381b2d1393c2e3f8e7acb7b13",
        "repository": "https://github.com/dianaclarke/arrow-rs"
    },
    "info": {
        "benchmark_language_version": "Python 3.9.7"
    },
    "machine_info": {
        "architecture_name": "x86_64",
        "cpu_core_count": "8",
        "cpu_frequency_max_hz": "2400000000",
        "cpu_l1d_cache_bytes": "65536",
        "cpu_l1i_cache_bytes": "131072",
        "cpu_l2_cache_bytes": "4194304",
        "cpu_l3_cache_bytes": "0",
        "cpu_model_name": "Apple M1",
        "cpu_thread_count": "8",
        "gpu_count": "0",
        "gpu_product_names": [],
        "kernel_name": "20.6.0",
        "memory_bytes": "17179869184",
        "name": "diana",
        "os_name": "macOS",
        "os_version": "10.16"
    },
    "run_id": "b2ca0d581cf14f21936276ff7ca5a940",
    "stats": {
        "data": [
            "0.000002",
            "0.000001",
            "0.000001"
        ],
        "iqr": "0.000001",
        "iterations": 3,
        "max": "0.000002",
        "mean": "0.000001",
        "median": "0.000001",
        "min": "0.000001",
        "q1": "0.000001",
        "q3": "0.000002",
        "stdev": "0.000001",
        "time_unit": "s",
        "times": [],
        "unit": "s"
    },
    "tags": {
        "name": "test"
    },
    "timestamp": "2022-02-09T01:55:52.250727+00:00"
}
```
