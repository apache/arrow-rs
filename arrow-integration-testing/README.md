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

# Apache Arrow Rust Integration Testing

See [Integration Testing](https://arrow.apache.org/docs/format/Integration.html) for an overview of integration testing.

This crate contains the following binaries, which are invoked by Archery during integration testing with other Arrow implementations.

| Binary                         | Purpose                                   |
| ------------------------------ | ----------------------------------------- |
| arrow-file-to-stream           | Converts an Arrow file to an Arrow stream |
| arrow-stream-to-file           | Converts an Arrow stream to an Arrow file |
| arrow-json-integration-test    | Converts between Arrow and JSON formats   |
| flight-test-integration-server | Flight integration test: Server           |
| flight-test-integration-client | Flight integration test: Client           |

# Notes on how to run Rust Integration Test against C/C++

The code for running the integration tests is in the [arrow](https://github.com/apache/arrow) repository

### Check out code:

```shell
# check out arrow
git clone git@github.com:apache/arrow.git
# link rust source code into arrow
ln -s <path_to_arrow_rs> arrow/rust
```

### Install the tools:

```shell
cd arrow
pip install -e dev/archery[integration]
```

### Build the C++ binaries:

Follow the [C++ Direction](https://github.com/apache/arrow/tree/master/docs/source/developers/cpp) and build the integration test binaries with a command like this:

```
# build cpp binaries
cd arrow/cpp
mkdir build
cd  build
cmake  -DARROW_BUILD_INTEGRATION=ON -DARROW_FLIGHT=ON --preset ninja-debug-minimal ..
ninja
```

### Build the Rust binaries

Then

```
# build rust:
cd ../arrow-rs
cargo build --all
```

### Run archery

You can run the Archery tool using a command such as the following:

```shell
archery integration --with-cpp=true --with-rust=true
```

Above command will run producer and consumer for the matrix of enabled Arrow implementations (e.g. C++ and Rust),
and compare the results.

In C++ Arrow repo's CI, for integration test, it also runs consumer test with golden files specified by `--gold-dirs`:

```shell
archery integration --with-cpp=true --with-rust=true --gold-dirs=/path/to/arrow/testing/data/arrow-ipc-stream/integration/0.14.1 --gold-dirs=/path/to/arrow/testing/data/arrow-ipc-stream/integration/0.17.1
```

Actually C++ Arrow repo's CI runs with more implementations other than just C++ and Rust. This is the command to reproduce test result of CI:

```shell
archery integration --run-flight --with-cpp=1 --with-csharp=1 --with-java=1 --with-js=1 --with-go=1 --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/0.14.1 --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/0.17.1 --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/1.0.0-bigendian --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/1.0.0-littleendian --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/2.0.0-compression --gold-dirs=/arrow/testing/data/arrow-ipc-stream/integration/4.0.0-shareddict
```

To debug an individual test scenario, it is also possible to run the binaries directly:

```shell
# Run cpp server
$ arrow/cpp/build/debug/flight-test-integration-server -port 49153

# run rust client (you can see file names if you run archery --debug
$ arrow/rust/target/debug/flight-test-integration-client --host localhost --port=49153 --path /tmp/generated_dictionary_unsigned.json
```
